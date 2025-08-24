-- Goal: Generate time-based summaries to analyze sales patterns.
-- Why it matters: Helps identify peak periods and plan resources.
-- How to do it:
-- Aggregate daily, weekly, and monthly revenue from order_items
-- Break down by:
--  • Location
--  • Menu category (if available)
--  • Time of day (optional)

WITH order_revenue AS (
    SELECT 
        o.order_id,
        DATE(o.creation_time_utc) AS order_date,
        o.restaurant_id,
        i.item_category,
        COALESCE(i.item_total, 0) as order_total
    FROM fact_orders o
    JOIN fact_items i ON o.order_id = i.order_id
),
daily_revenue AS (
    SELECT
        order_date,
        restaurant_id,
        item_category,
        SUM(order_total) AS daily_total
    FROM order_revenue
    GROUP BY order_date, restaurant_id, item_category
),
weekly_revenue AS (
    SELECT
        DATE_TRUNC('week', order_date) AS week_start,
        restaurant_id,
        item_category,
        SUM(order_total) AS weekly_total
    FROM order_revenue
    GROUP BY DATE_TRUNC('week', order_date), restaurant_id, item_category
),
monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month_start,
        restaurant_id,
        item_category,
        SUM(order_total) AS monthly_total
    FROM order_revenue
    GROUP BY DATE_TRUNC('month', order_date), restaurant_id, item_category
)
SELECT 
    'Daily' AS period_type,
    order_date AS period_start,
    restaurant_id,
    item_category,
    daily_total AS revenue
FROM daily_revenue
UNION ALL
SELECT
    'Weekly' AS period_type,
    week_start AS period_start,
    restaurant_id,
    item_category,
    weekly_total AS revenue
FROM weekly_revenue
UNION ALL
SELECT
    'Monthly' AS period_type,
    month_start AS period_start,
    restaurant_id,
    item_category,
    monthly_total AS revenue
FROM monthly_revenue
ORDER BY period_start, restaurant_id, item_category;

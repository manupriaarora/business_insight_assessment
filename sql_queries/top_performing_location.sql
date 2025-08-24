-- Identify best and worst-performing store locations.
-- Why it matters: Informs decisions about promotions, staffing, or expansion.

-- How to do it:
-- Group order_items by location_id (or store_id if available)

-- Calculate:
--  • Total revenue
--  • Average order value
--  • Orders per day/week
-- Rank locations based on revenue

WITH order_totals AS (
    SELECT
        o.order_id,
        o.restaurant_id AS location_id,
        DATE(o.creation_time_utc) AS order_date,
        COALESCE(i.item_total, 0) + COALESCE(op.option_total, 0) AS order_total
    FROM fact_orders o
    LEFT JOIN (
        SELECT order_id, SUM(item_total) AS item_total
        FROM fact_items
        GROUP BY order_id
    ) i ON o.order_id = i.order_id
    LEFT JOIN (
        SELECT order_id, SUM(option_total) AS option_total
        FROM fact_items_options
        GROUP BY order_id
    ) op ON o.order_id = op.order_id
),
location_stats AS (
    SELECT
        location_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(order_total) AS total_revenue,
        AVG(order_total) AS avg_order_value,
        COUNT(DISTINCT order_date) AS active_days,
        COUNT(DISTINCT order_id) * 1.0 / COUNT(DISTINCT order_date) AS orders_per_day,
        COUNT(DISTINCT order_id) * 1.0 / 
   (DATE_DIFF('week', MIN(order_date), MAX(order_date)) + 1) AS orders_per_week
    FROM order_totals
    GROUP BY location_id
),
ranked_locations AS (
    SELECT
        location_id,
        total_revenue,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
        avg_order_value,
        orders_per_day,
        orders_per_week
    FROM location_stats
)
SELECT *
FROM ranked_locations
ORDER BY revenue_rank;

WITH customer_orders AS (
    SELECT
        o.user_id,
        o.order_id,
        DATE(o.creation_time_utc) AS order_date,
        COALESCE(i.item_total + op.option_total, 0) AS order_total
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
orders_with_previous AS (
    -- Add previous order date per user
    SELECT
        user_id,
        order_date,
        order_total,
        LAG(order_date) OVER (PARTITION BY user_id ORDER BY order_date) AS previous_order_date
    FROM customer_orders
),
avg_gap_per_user AS (
    -- Average gap between orders
    SELECT
        user_id,
        ROUND(AVG(DATE_DIFF('day', previous_order_date, order_date)), 2) AS avg_days_between_orders
    FROM orders_with_previous
    GROUP BY user_id
),
days_since_last_order AS (
    -- Days since last order
    SELECT
        user_id,
        MAX(order_date) AS last_order_date,
        DATE_DIFF('day', MAX(order_date), CURRENT_DATE) AS days_since_last_order
    FROM customer_orders
    GROUP BY user_id
),
monthly_orders AS (
    -- Spend per user per month
    SELECT
        user_id,
        DATE_TRUNC('month', order_date) AS month_start,
        SUM(order_total) AS monthly_total
    FROM customer_orders
    GROUP BY user_id, DATE_TRUNC('month', order_date)
),
-- Add previous month spend for percent change
monthly_orders_with_lag AS (
    SELECT
        user_id,
        month_start,
        monthly_total,
        LAG(monthly_total, 1) OVER (PARTITION BY user_id ORDER BY month_start) AS prev_month_total
    FROM monthly_orders
),
-- Calculate percent change safely
monthly_orders_pct_change AS (
    SELECT
        user_id,
        month_start,
        monthly_total,
        prev_month_total,
        CASE
            WHEN prev_month_total IS NULL OR prev_month_total < 1 THEN NULL
            ELSE ROUND((monthly_total - prev_month_total) / prev_month_total * 100, 2)
        END AS pct_change_last_month
    FROM monthly_orders_with_lag
),
-- Final customer activity profile
customer_activity_profile AS (
    SELECT
        d.user_id,
        d.last_order_date,
        d.days_since_last_order,
        g.avg_days_between_orders,
        p.pct_change_last_month
    FROM days_since_last_order d
    LEFT JOIN avg_gap_per_user g ON d.user_id = g.user_id
    LEFT JOIN (
        SELECT user_id, pct_change_last_month
        FROM monthly_orders_pct_change
        WHERE month_start = (SELECT MAX(month_start) FROM monthly_orders)
    ) p ON d.user_id = p.user_id
)
SELECT *,
    CASE
        WHEN days_since_last_order > 700 THEN 'At Risk'
        ELSE 'Active'
    END AS activity_status
FROM customer_activity_profile
ORDER BY days_since_last_order DESC;
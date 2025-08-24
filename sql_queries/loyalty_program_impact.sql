-- Loyalty Program Impact
-- Goal: Compare loyalty members vs non-members in terms of spend and engagement.
-- Why it matters: Evaluates ROI of the loyalty program.

-- How to do it:
-- Filter order_items by is_loyalty = true vs false
-- Compare per-customer:
--  • Average Spend
--  • Repeat Orders
--  • Lifetime Value

WITH order_totals AS (
    SELECT
        o.user_id,
        o.is_loyalty,  -- true = loyalty member, false = non-member
        o.order_id,
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
customer_stats AS (
    SELECT
        user_id,
        is_loyalty,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(order_total) AS total_spend,
        AVG(order_total) AS avg_order_value
    FROM order_totals
    GROUP BY user_id, is_loyalty
),
loyalty_vs_nonloyalty as (
SELECT
    is_loyalty,
    AVG(total_spend) AS avg_spend_per_customer,
    AVG(total_orders) AS avg_repeat_orders,
    AVG(avg_order_value) AS avg_order_value
FROM customer_stats
GROUP BY is_loyalty
)
SELECT
    CASE WHEN is_loyalty THEN 'Loyalty Customers' ELSE 'Non-Loyalty Customers' END AS customer_type,
    avg_spend_per_customer,
    avg_repeat_orders,
    avg_order_value
FROM loyalty_vs_nonloyalty;

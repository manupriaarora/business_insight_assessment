-- Measure how discounts affect revenue and profitability.
-- Why it matters: Helps optimize pricing strategies.
-- How to do it:
-- Use order_item_options.option_price to detect discounts (option_price < 0)
-- Compare:
--  • Revenue from discounted orders vs non-discounted
--  • Number of orders before/after applying discounts


WITH order_totals AS (
    SELECT
        o.order_id,
        o.user_id,
        o.restaurant_id,
        SUM(i.item_total) AS item_total,
        SUM(COALESCE(op.option_total, 0)) AS option_total,
        SUM(i.item_total + COALESCE(op.option_total, 0)) AS order_total,
        MAX(COALESCE(op.has_discount, 0)) AS has_discount
    FROM fact_orders o
    LEFT JOIN (
        SELECT order_id, SUM(item_total) AS item_total
        FROM fact_items
        GROUP BY order_id
    ) i ON o.order_id = i.order_id
    LEFT JOIN (
        SELECT order_id, SUM(option_total) AS option_total,
        MAX(CASE WHEN option_price < 0 THEN 1 ELSE 0 END) AS has_discount
        FROM fact_items_options
        GROUP BY order_id
    ) op ON o.order_id = op.order_id
    GROUP BY o.order_id, o.user_id, o.restaurant_id
)
SELECT
    CASE WHEN has_discount = 1 THEN 'Discounted Order'
    ELSE 'Non-Discounted Order'
    END as order_type,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(order_total) AS total_revenue,
    AVG(order_total) AS avg_order_value
FROM order_totals
GROUP BY has_discount
ORDER BY has_discount DESC;

-- Primary Metrics:
-- Customer Lifetime Value (CLV):
-- Goal: Estimate how much total revenue a customer will generate over their entire relationship with the business.
-- Why it matters: Helps prioritize high-value customers, plan marketing budgets wisely, and improve retention strategies.

-- How to do it:
-- Use order_items and order_item_options to compute revenue per order.
-- Aggregate total spend per customer_id.

-- Group CLV values (for tagging):
-- High CLV: Top 20% customers
-- Medium CLV: Mid 60%
-- Low CLV: Bottom 20%

WITH item_aggs AS (
  SELECT order_id, SUM(item_total) AS item_order_total
  FROM fact_items
  GROUP BY order_id
),
option_aggs AS (
  SELECT order_id, SUM(option_total) AS option_order_total
  FROM fact_items_options
  GROUP BY order_id
),
order_totals AS (
  SELECT
    o.user_id,
    o.order_id,
    COALESCE(i.item_order_total, 0)  AS item_order_total,
    COALESCE(op.option_order_total, 0) AS option_order_total
  FROM fact_orders o
  LEFT JOIN item_aggs  i  ON o.order_id = i.order_id
  LEFT JOIN option_aggs op ON o.order_id = op.order_id
),
per_user_revenue AS (
  SELECT
    user_id,
    SUM(item_order_total)                  AS total_item_cost,
    SUM(option_order_total)                AS total_option_cost,
    SUM(item_order_total + option_order_total) AS total_cost_per_user
  FROM order_totals
  GROUP BY user_id
),
user_clv AS (
  SELECT
    user_id,
    total_item_cost,
    total_option_cost,
    total_cost_per_user,
    ROUND(PERCENT_RANK() OVER (ORDER BY total_cost_per_user) * 100, 3) AS clv_percent
  FROM per_user_revenue
)
SELECT
  user_id,
  total_item_cost,
  total_option_cost,
  total_cost_per_user,
  clv_percent,
  CASE
    WHEN clv_percent >= 80 THEN 'High CLV'
    WHEN clv_percent >= 20 THEN 'Medium CLV'
    ELSE 'Low CLV'
  END AS clv_tag
FROM user_clv
ORDER BY total_cost_per_user DESC;



-- Goal: Group customers based on spending and activity to support campaign targeting.
-- Why it matters: Enables personalized offers and engagement.

-- How to do it:
-- Use RFM logic based on order_items:
-- Recency: Days since last purchase
-- Frequency: Number of purchases in last N months
-- Monetary: Total spend in last N months

-- Segment:
-- VIPs: High R, F, M
-- New Customers: Low F, high R
-- Churn Risk: Low R, low F

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
per_user_spending AS (
  SELECT
    user_id,
    SUM(item_order_total)                  AS total_item_cost,
    SUM(option_order_total)                AS total_option_cost,
    SUM(item_order_total + option_order_total) AS total_cost_per_user
  FROM order_totals
  GROUP BY user_id
),
per_user_recent_purchase as (
SELECT 
    user_id, 
    max(creation_time_utc) as last_purchase_date, 
    date_diff('day', max(creation_time_utc), CURRENT_DATE) AS days_passed
FROM fact_orders
GROUP BY user_id
),
per_user_frequent_purchase as (
SELECT 
    user_id,
    COALESCE(SUM(CASE 
        WHEN creation_time_utc >= date_add('month', -24, current_date) THEN 1
        ELSE 0
    END), 0) AS num_purchases_last_24_months
FROM fact_orders
GROUP BY user_id
),
rfm AS (
    SELECT 
        s.user_id,
        s.total_cost_per_user,
        r.days_passed,
        f.num_purchases_last_24_months
    FROM per_user_spending s
    JOIN per_user_recent_purchase r ON s.user_id = r.user_id
    JOIN per_user_frequent_purchase f ON s.user_id = f.user_id
),
customer_ranking as (
SELECT 
    user_id,
    total_cost_per_user,
    days_passed,
    num_purchases_last_24_months,
    NTILE(5) OVER (ORDER BY total_cost_per_user DESC) AS monetary_rank,
    NTILE(5) OVER (ORDER BY days_passed ASC) AS recency_rank,  -- smaller days_passed = more recent
    NTILE(5) OVER (ORDER BY num_purchases_last_24_months DESC) AS frequency_rank
FROM rfm
)
SELECT *,
    CASE
        WHEN recency_rank = 1 AND frequency_rank = 1 AND monetary_rank = 1 THEN 'VIP'
        WHEN recency_rank = 1 AND frequency_rank >= 4 THEN 'New Customer'
        WHEN recency_rank >= 4 AND frequency_rank >= 4 THEN 'Churn Risk'
        ELSE 'Other'
    END AS customer_segment
from customer_ranking

-- Segment:
-- VIPs: High R, F, M
-- New Customers: Low F, high R
-- Churn Risk: Low R, low F




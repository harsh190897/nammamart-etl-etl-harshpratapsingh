-- include/sql/marts/mart_top_customers.sql
-- Mart: customers ranked by spend (ELT, Part B). Answers business question 5 (top 10 by spend, with
-- loyalty tier). Personal data is masked: only name_masked is shown, never the raw name, email or phone.
-- One row per customer with at least one VALID, DELIVERED order. Rebuilt on every run.

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT row_number() OVER (ORDER BY sum(o.order_amount) DESC, o.customer_id) AS spend_rank,
       o.customer_id,
       c.name_masked,
       c.loyalty_tier,
       c.city,
       count(*)                     AS delivered_orders,
       round(sum(o.order_amount), 2) AS total_spend
FROM stg_orders AS o
JOIN stg_customers AS c USING (customer_id)
WHERE o.dq_status = 'VALID' AND o.order_status = getvariable('revenue_status')
GROUP BY o.customer_id, c.name_masked, c.loyalty_tier, c.city
ORDER BY spend_rank;

-- include/sql/marts/mart_category_margin.sql
-- Mart: revenue and gross margin per product category (ELT, Part B). Answers business question 2.
-- gross margin = revenue - quantity x cost_price, for VALID, DELIVERED orders. Rebuilt on every run.
-- The margin is rounded per order before it is summed, exactly like the ETL fact table.

CREATE OR REPLACE TABLE mart_category_margin AS
WITH delivered AS (
    SELECT p.category, o.quantity, o.order_amount, p.cost_price,
           round(o.order_amount - o.quantity * p.cost_price, 2) AS gross_margin
    FROM stg_orders AS o
    JOIN stg_products AS p USING (product_id)
    WHERE o.dq_status = 'VALID' AND o.order_status = getvariable('revenue_status')
)
SELECT category,
       count(*)                                                        AS delivered_orders,
       sum(quantity)                                                   AS units,
       round(sum(order_amount), 2)                                     AS revenue,
       round(sum(quantity * cost_price), 2)                            AS cost,
       round(sum(gross_margin), 2)                                     AS gross_margin,
       round(100 * sum(gross_margin) / nullif(sum(order_amount), 0), 2) AS margin_pct
FROM delivered
GROUP BY category
ORDER BY revenue DESC;

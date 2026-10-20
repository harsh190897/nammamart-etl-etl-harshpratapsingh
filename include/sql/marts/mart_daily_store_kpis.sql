-- include/sql/marts/mart_daily_store_kpis.sql
-- Mart: daily KPIs per store (ELT, Part B). Answers business question 1 (daily net revenue by zone and store).
-- Grain: one row per order_date and store. Built only from VALID, DELIVERED orders (revenue is after discount:
-- order_amount = quantity x unit_price x (1 - discount_pct/100), checked in staging).
-- Rebuilt in full on every run, so it always matches stg_orders.
--
-- Variables: sla_minutes (the "within 15 minutes" promise), revenue_status ('DELIVERED')

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
WITH delivered AS (
    SELECT o.order_date, s.zone, o.store_id, s.store_name, o.quantity, o.order_amount, o.delivery_minutes,
           round(o.order_amount - o.quantity * p.cost_price, 2) AS gross_margin
    FROM stg_orders AS o
    JOIN stg_stores   AS s USING (store_id)
    JOIN stg_products AS p USING (product_id)
    WHERE o.dq_status = 'VALID' AND o.order_status = getvariable('revenue_status')
)
SELECT order_date, zone, store_id, store_name,
       count(*)                                                                    AS delivered_orders,
       round(sum(order_amount), 2)                                                 AS net_revenue,
       round(sum(gross_margin), 2)                                                 AS gross_margin,
       round(avg(delivery_minutes), 1)                                             AS avg_delivery_minutes,
       count(*) FILTER (WHERE delivery_minutes <= getvariable('sla_minutes'))      AS orders_within_15_min,
       round(100.0 * count(*) FILTER (WHERE delivery_minutes <= getvariable('sla_minutes')) / count(*), 1)
                                                                                   AS pct_within_15_min
FROM delivered
GROUP BY ALL
ORDER BY order_date, zone, store_id;

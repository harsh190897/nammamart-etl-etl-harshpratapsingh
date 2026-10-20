-- include/sql/marts/mart_delivery_sla.sql
-- Mart: delivery performance per store (ELT, Part B). Answers business question 3
-- (average delivery time per store, and the % of delivered orders within 15 minutes).
-- Only VALID, DELIVERED orders: impossible times (> 120 minutes) were rejected in staging, so they
-- can no longer distort the "10-minute promise" KPI. Rebuilt on every run.

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT s.zone, o.store_id, s.store_name,
       count(*)                                                                    AS delivered_orders,
       round(avg(o.delivery_minutes), 1)                                           AS avg_delivery_minutes,
       min(o.delivery_minutes)                                                     AS fastest_minutes,
       max(o.delivery_minutes)                                                     AS slowest_minutes,
       count(*) FILTER (WHERE o.delivery_minutes <= getvariable('sla_minutes'))    AS orders_within_15_min,
       round(100.0 * count(*) FILTER (WHERE o.delivery_minutes <= getvariable('sla_minutes')) / count(*), 1)
                                                                                   AS pct_within_15_min
FROM stg_orders AS o
JOIN stg_stores AS s USING (store_id)
WHERE o.dq_status = 'VALID' AND o.order_status = getvariable('revenue_status')
GROUP BY ALL
ORDER BY s.zone, o.store_id;

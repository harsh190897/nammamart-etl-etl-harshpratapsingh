-- Q3 (ETL): average delivery time per store, and the % of delivered orders within 15 minutes.
SELECT s.zone, f.store_id, s.store_name,
       count(*)                                                              AS delivered_orders,
       round(avg(f.delivery_minutes), 1)                                     AS avg_delivery_minutes,
       round(100.0 * count(*) FILTER (WHERE f.within_15_min) / count(*), 1)  AS pct_within_15_min
FROM etl_fact_orders AS f
JOIN etl_dim_store AS s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY s.zone, f.store_id;

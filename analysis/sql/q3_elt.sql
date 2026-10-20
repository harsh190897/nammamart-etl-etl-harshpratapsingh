-- Q3 (ELT): average delivery time per store, and the % of delivered orders within 15 minutes.
SELECT zone, store_id, store_name, delivered_orders, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla
ORDER BY zone, store_id;

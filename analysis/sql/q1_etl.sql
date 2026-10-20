-- Q1 (ETL): daily net revenue by zone and by store (delivered orders only, after discount).
SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
FROM etl_daily_zone_revenue
ORDER BY order_date, zone, store_id;

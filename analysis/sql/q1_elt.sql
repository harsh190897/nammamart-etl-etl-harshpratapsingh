-- Q1 (ELT): daily net revenue by zone and by store (delivered orders only, after discount).
SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
FROM mart_daily_store_kpis
ORDER BY order_date, zone, store_id;

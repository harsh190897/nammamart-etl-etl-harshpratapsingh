-- Part B4: September 2026 net revenue by zone, ETL report table vs ELT mart.
WITH etl AS (
    SELECT zone, sum(delivered_orders) AS orders, sum(net_revenue) AS revenue
    FROM etl_daily_zone_revenue
    WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
    GROUP BY zone
),
elt AS (
    SELECT zone, sum(delivered_orders) AS orders, sum(net_revenue) AS revenue
    FROM mart_daily_store_kpis
    WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
    GROUP BY zone
)
SELECT coalesce(zone, 'TOTAL') AS zone,
       CAST(sum(etl.orders) AS BIGINT) AS etl_orders, CAST(sum(elt.orders) AS BIGINT) AS elt_orders,
       round(sum(etl.revenue), 2) AS etl_net_revenue, round(sum(elt.revenue), 2) AS elt_net_revenue,
       round(sum(elt.revenue) - sum(etl.revenue), 2) + 0.0 AS difference
FROM etl FULL OUTER JOIN elt USING (zone)
GROUP BY ROLLUP (zone)
ORDER BY zone IS NULL, etl_net_revenue DESC;

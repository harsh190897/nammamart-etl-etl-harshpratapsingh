-- Part B4: ELT mart totals vs ETL totals for September 2026 (delivered orders, order_date 1-30 September).
-- Values are formatted as text so counts and rupee amounts can share one table.
WITH etl AS (
    SELECT count(*) AS delivered_orders, sum(net_revenue) AS net_revenue, sum(gross_margin) AS gross_margin,
           count(*) FILTER (WHERE within_15_min) AS within_15_min
    FROM etl_fact_orders
    WHERE order_status = 'DELIVERED' AND order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
),
elt AS (
    SELECT CAST(sum(delivered_orders) AS BIGINT) AS delivered_orders, sum(net_revenue) AS net_revenue,
           sum(gross_margin) AS gross_margin, CAST(sum(orders_within_15_min) AS BIGINT) AS within_15_min
    FROM mart_daily_store_kpis
    WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
),
day_store_diff AS (
    SELECT count(*) AS n FROM (
        (SELECT order_date, store_id, delivered_orders, net_revenue FROM etl_daily_zone_revenue
         WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
         EXCEPT
         SELECT order_date, store_id, delivered_orders, net_revenue FROM mart_daily_store_kpis
         WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30')
        UNION ALL
        (SELECT order_date, store_id, delivered_orders, net_revenue FROM mart_daily_store_kpis
         WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
         EXCEPT
         SELECT order_date, store_id, delivered_orders, net_revenue FROM etl_daily_zone_revenue
         WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'))
)
SELECT 'Delivered orders' AS metric, format('{:,}', etl.delivered_orders) AS etl,
       format('{:,}', elt.delivered_orders) AS elt, format('{:,}', elt.delivered_orders - etl.delivered_orders) AS difference
FROM etl, elt
UNION ALL
SELECT 'Net revenue (₹)', format('{:,.2f}', etl.net_revenue), format('{:,.2f}', elt.net_revenue),
       format('{:,.2f}', round(elt.net_revenue - etl.net_revenue, 2) + 0.0)
FROM etl, elt
UNION ALL
SELECT 'Gross margin (₹)', format('{:,.2f}', etl.gross_margin), format('{:,.2f}', elt.gross_margin),
       format('{:,.2f}', round(elt.gross_margin - etl.gross_margin, 2) + 0.0)
FROM etl, elt
UNION ALL
SELECT 'Delivered within 15 minutes', format('{:,}', etl.within_15_min), format('{:,}', elt.within_15_min),
       format('{:,}', elt.within_15_min - etl.within_15_min)
FROM etl, elt
UNION ALL
SELECT 'Day × store rows that differ (ETL report vs ELT mart)', format('{:,}', n), format('{:,}', n), '0'
FROM day_store_diff;

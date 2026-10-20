-- Q4 (ETL): revenue by payment mode, and the amount that does not reconcile with the payment gateway.
-- A mismatch is assigned to the order's payment mode, or to the gateway's when there is no valid order.
WITH revenue AS (
    SELECT payment_mode, count(*) AS delivered_orders, sum(net_revenue) AS net_revenue
    FROM etl_fact_orders
    WHERE order_status = 'DELIVERED'
    GROUP BY payment_mode
),
not_reconciled AS (
    SELECT coalesce(order_payment_mode, gateway_payment_mode) AS payment_mode,
           count(*) AS mismatches, sum(amount_at_risk) AS amount_not_reconciled
    FROM etl_payment_reconciliation
    GROUP BY 1
),
per_mode AS (
    SELECT coalesce(r.payment_mode, n.payment_mode) AS payment_mode,
           coalesce(r.delivered_orders, 0) AS delivered_orders, coalesce(r.net_revenue, 0) AS net_revenue,
           coalesce(n.mismatches, 0) AS mismatches, coalesce(n.amount_not_reconciled, 0) AS amount_not_reconciled
    FROM revenue AS r FULL OUTER JOIN not_reconciled AS n USING (payment_mode)
)
SELECT coalesce(payment_mode, 'TOTAL') AS payment_mode, CAST(sum(delivered_orders) AS BIGINT) AS delivered_orders,
       round(sum(net_revenue), 2) AS net_revenue, CAST(sum(mismatches) AS BIGINT) AS mismatches,
       round(sum(amount_not_reconciled), 2) AS amount_not_reconciled
FROM per_mode
GROUP BY ROLLUP (payment_mode)
ORDER BY payment_mode IS NULL, net_revenue DESC;

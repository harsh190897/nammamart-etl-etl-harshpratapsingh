-- Q4 (ELT): revenue by payment mode, and the amount that does not reconcile with the payment gateway.
SELECT coalesce(payment_mode, 'TOTAL') AS payment_mode, CAST(sum(delivered_orders) AS BIGINT) AS delivered_orders,
       round(sum(net_revenue), 2) AS net_revenue, CAST(sum(mismatches) AS BIGINT) AS mismatches,
       round(sum(amount_not_reconciled), 2) AS amount_not_reconciled
FROM mart_payment_mode_revenue
GROUP BY ROLLUP (payment_mode)
ORDER BY payment_mode IS NULL, net_revenue DESC;

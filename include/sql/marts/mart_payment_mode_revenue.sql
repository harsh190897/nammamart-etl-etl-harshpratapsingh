-- include/sql/marts/mart_payment_mode_revenue.sql
-- Mart: revenue per payment mode, and the amount that does not reconcile with the payment gateway
-- (ELT, Part B). Answers business question 4. Built after mart_payment_reconciliation. Rebuilt on every run.
-- A mismatch is assigned to the order's payment mode, or to the gateway's when there is no valid order.

CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
WITH revenue AS (
    SELECT payment_mode,
           count(*)                    AS delivered_orders,
           round(sum(order_amount), 2) AS net_revenue
    FROM stg_orders
    WHERE dq_status = 'VALID' AND order_status = getvariable('revenue_status')
    GROUP BY payment_mode
),
not_reconciled AS (
    SELECT coalesce(order_payment_mode, gateway_payment_mode) AS payment_mode,
           count(*)                                           AS mismatches,
           round(sum(amount_at_risk), 2)                      AS amount_not_reconciled
    FROM mart_payment_reconciliation
    GROUP BY 1
)
SELECT coalesce(r.payment_mode, n.payment_mode)  AS payment_mode,
       coalesce(r.delivered_orders, 0)           AS delivered_orders,
       coalesce(r.net_revenue, 0)                AS net_revenue,
       coalesce(n.mismatches, 0)                 AS mismatches,
       coalesce(n.amount_not_reconciled, 0)      AS amount_not_reconciled
FROM revenue AS r
FULL OUTER JOIN not_reconciled AS n USING (payment_mode)
ORDER BY net_revenue DESC;

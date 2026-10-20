-- include/sql/marts/mart_payment_reconciliation.sql
-- Mart: every mismatch between orders and the payment gateway (ELT, Part B; acceptance criterion 7).
-- One row per mismatch, with a reason and the amount at risk. Same rules as the ETL table
-- etl_payment_reconciliation, applied to the VALID rows of stg_orders and stg_payments.
-- A full outer join finds problems on both sides (orders without a payment, payments without an order).
-- Rebuilt on every run.
--
-- Variables: amount_tolerance (rupees)

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH orders AS (
    SELECT * FROM stg_orders WHERE dq_status = 'VALID'
),
payments AS (
    SELECT * FROM stg_payments WHERE dq_status = 'VALID'
),
joined AS (
    SELECT coalesce(o.order_id, p.order_id)          AS order_id,
           p.payment_id,
           o.order_status,
           p.payment_status,
           o.order_amount,
           p.amount_paid,
           o.payment_mode                            AS order_payment_mode,
           p.payment_mode                            AS gateway_payment_mode,
           o.order_id IS NOT NULL                    AS has_order,
           p.payment_id IS NOT NULL                  AS has_payment,
           coalesce(p.order_in_source, false)        AS order_in_source
    FROM orders AS o
    FULL OUTER JOIN payments AS p ON o.order_id = p.order_id
),
mismatches AS (
    SELECT *, 'ORDER_WITHOUT_PAYMENT' AS reason, order_amount AS amount_at_risk
    FROM joined WHERE has_order AND NOT has_payment AND order_status IN ('DELIVERED', 'RETURNED')
    UNION ALL
    SELECT *, 'PAYMENT_FAILED', order_amount
    FROM joined WHERE order_status = 'DELIVERED' AND payment_status = 'FAILED'
    UNION ALL
    SELECT *, 'AMOUNT_MISMATCH', abs(order_amount - amount_paid)
    FROM joined WHERE order_status = 'DELIVERED' AND payment_status = 'SUCCESS'
                  AND abs(order_amount - amount_paid) > getvariable('amount_tolerance')
    UNION ALL
    SELECT *, 'REFUNDED_BUT_DELIVERED', amount_paid
    FROM joined WHERE order_status = 'DELIVERED' AND payment_status = 'REFUNDED'
    UNION ALL
    SELECT *, 'RETURNED_NOT_REFUNDED', amount_paid
    FROM joined WHERE order_status = 'RETURNED' AND payment_status = 'SUCCESS'
    UNION ALL
    SELECT *, 'PAID_BUT_CANCELLED', amount_paid
    FROM joined WHERE order_status = 'CANCELLED' AND payment_status = 'SUCCESS'
    UNION ALL
    -- the order_id is in no orders file
    SELECT *, 'PAYMENT_FOR_UNKNOWN_ORDER', amount_paid
    FROM joined WHERE has_payment AND NOT has_order AND NOT order_in_source
    UNION ALL
    -- the order exists in the source file but was rejected in staging, so it is not in revenue yet
    SELECT *, 'PAYMENT_FOR_QUARANTINED_ORDER', amount_paid
    FROM joined WHERE has_payment AND NOT has_order AND order_in_source
    UNION ALL
    SELECT *, 'PAYMENT_MODE_MISMATCH', 0
    FROM joined WHERE has_order AND has_payment AND order_payment_mode <> gateway_payment_mode
)
SELECT order_id, payment_id, reason, round(amount_at_risk, 2) AS amount_at_risk,
       order_status, payment_status, order_amount, amount_paid, order_payment_mode, gateway_payment_mode
FROM mismatches
ORDER BY reason, order_id;

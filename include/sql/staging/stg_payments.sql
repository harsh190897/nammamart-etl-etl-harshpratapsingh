-- include/sql/staging/stg_payments.sql
-- Staging: payments (ELT, Part B). Input: this run's batch of raw_payments. Output: stg_payments.
-- Same 4 steps as every staging file (see stg_stores.sql): chk -> reasons -> results -> stg table.
-- A payment for an order that is in no orders file is only a Warning: the payment row itself is fine,
-- and the mismatch is reported in the payment reconciliation mart (order_in_source = false).

-- 1. Typed columns and rule results ---------------------------------------------------------
CREATE OR REPLACE TEMP TABLE chk AS
WITH src AS (
    SELECT *,
           clean(payment_id)                       AS id,
           to_timestamp_value(payment_ts)          AS ts,
           upper(clean(payment_status))            AS status,
           standard_payment_mode(payment_mode)     AS mode,
           to_number(amount_paid)                  AS amount
    FROM raw_payments
    WHERE _run_id = getvariable('run_id')
),
order_ids AS (SELECT DISTINCT clean(order_id) AS id FROM raw_orders WHERE _run_id = getvariable('run_id'))
SELECT
    _source_file, _source_row, id AS record_id,
    -- typed and standardised
    id                                                                       AS payment_id,
    clean(order_id)                                                          AS order_id,
    ts                                                                       AS payment_ts,
    mode                                                                     AS payment_mode,
    amount                                                                   AS amount_paid,
    status                                                                   AS payment_status,
    value_or_null(gateway_ref)                                               AS gateway_ref,
    clean(order_id) IN (SELECT id FROM order_ids)                            AS order_in_source,
    -- rules (names as in elt_dq_rules)
    flag(is_missing(payment_id))                                             AS fail__payment_id_not_null,
    flag(NOT is_missing(payment_id)
         AND row_number() OVER (PARTITION BY id ORDER BY _source_row) > 1)   AS fail__payment_id_unique,
    flag(is_missing(order_id))                                               AS fail__order_id_not_null,
    flag(ts IS NULL)                                                         AS fail__payment_ts_valid,
    flag(NOT list_contains(getvariable('valid_payment_status'), status))     AS fail__payment_status_valid,
    flag(mode IS NULL)                                                       AS fail__payment_mode_valid,
    flag(mode IS NOT NULL AND payment_mode <> mode)                          AS fail__payment_mode_standardised,
    flag(amount IS NULL)                                                     AS fail__amount_paid_is_number,
    flag(amount < 0)                                                         AS fail__amount_paid_not_negative,
    flag(NOT is_missing(order_id)
         AND clean(order_id) NOT IN (SELECT id FROM order_ids))              AS fail__order_id_exists,
    flag(is_missing(gateway_ref) AND status = 'SUCCESS'
         AND mode IS NOT NULL AND mode <> 'Cash on Delivery')                AS fail__gateway_ref_for_online_success,
    flag(ts > getvariable('as_of'))                                          AS fail__payment_ts_not_future
FROM src;

-- 2. failure_reason: every Critical rule the row breaks --------------------------------------
CREATE OR REPLACE TEMP TABLE chk_long AS
SELECT u._source_file, u._source_row, r.rule, r.rule_order, r.dimension, r.severity, r.reason, u.failed
FROM (UNPIVOT chk ON COLUMNS('^fail__') INTO NAME fail_column VALUE failed) AS u
JOIN elt_dq_rules AS r ON r.entity = 'payments' AND 'fail__' || r.rule = u.fail_column;

CREATE OR REPLACE TEMP TABLE chk_reason AS
SELECT _source_file, _source_row, string_agg(reason, '; ' ORDER BY rule_order) AS failure_reason
FROM chk_long
WHERE failed AND severity = 'Critical'
GROUP BY _source_file, _source_row;

-- 3. dq_results (one row per rule) and stg_rejected_rows --------------------------------------
CREATE OR REPLACE TEMP TABLE chk_counts AS
SELECT regexp_replace(_source_file, '\.csv$', '') || '.' || rule AS check_name, dimension, severity,
       count(*) AS rows_checked, count(*) FILTER (WHERE failed) AS rows_failed
FROM chk_long
GROUP BY ALL;

DELETE FROM dq_results
WHERE run_id = getvariable('run_id') AND dag_id = getvariable('dag_id')
  AND check_name IN (SELECT check_name FROM chk_counts);

INSERT INTO dq_results BY NAME
SELECT getvariable('run_id') AS run_id, getvariable('dag_id') AS dag_id, check_name, dimension, severity,
       rows_checked, rows_failed,
       CASE WHEN rows_failed = 0 THEN 'PASS' WHEN severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END AS status,
       getvariable('checked_at') AS checked_at
FROM chk_counts;

DELETE FROM stg_rejected_rows
WHERE _run_id = getvariable('run_id') AND source_file IN (SELECT DISTINCT _source_file FROM chk);

INSERT INTO stg_rejected_rows BY NAME
SELECT getvariable('run_id') AS _run_id, c._source_file AS source_file, c._source_row AS source_row,
       c.record_id, r.failure_reason, getvariable('checked_at') AS rejected_at
FROM chk AS c JOIN chk_reason AS r USING (_source_file, _source_row);

-- 4. Staging table: one row per payment_id ------------------------------------------------------
CREATE OR REPLACE TABLE stg_payments AS
SELECT c.payment_id, c.order_id, c.payment_ts, c.payment_mode, c.amount_paid, c.payment_status, c.gateway_ref,
       c.order_in_source,
       CASE WHEN r.failure_reason IS NULL THEN 'VALID' ELSE 'REJECTED' END AS dq_status,
       r.failure_reason,
       c._source_file, c._source_row,
       getvariable('run_id') AS _run_id
FROM chk AS c LEFT JOIN chk_reason AS r USING (_source_file, _source_row)
WHERE NOT c.fail__payment_id_not_null AND NOT c.fail__payment_id_unique
ORDER BY c._source_row;

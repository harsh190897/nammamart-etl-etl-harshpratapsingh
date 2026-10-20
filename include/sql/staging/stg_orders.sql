-- include/sql/staging/stg_orders.sql
-- Staging: orders (ELT, Part B). Input: this run's batch of raw_orders (orders.csv + next-day files).
-- Output: stg_orders, a persistent table updated with MERGE (upsert) on order_id (Part B3).
-- Same steps as every staging file (see stg_stores.sql), plus:
--   * consistency: customer_id / product_id / store_id must exist in the master file AND point to a
--     VALID row of stg_customers / stg_products / stg_stores (built earlier in this run)
--   * file-level checks: freshness (per orders file) and volume (orders.csv vs the previous run)
--   * one version per order: the latest VALID version wins (a next-day correction such as DELIVERED ->
--     RETURNED replaces the older row); if no version is valid, the latest version is kept as REJECTED
--   * MERGE: new order -> insert; changed order -> update; identical order -> untouched (re-runs change nothing)

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id          VARCHAR PRIMARY KEY,
    order_ts          TIMESTAMP,
    order_date        DATE,
    customer_id       VARCHAR,
    store_id          VARCHAR,
    product_id        VARCHAR,
    quantity          INTEGER,
    unit_price        DOUBLE,
    discount_pct      DOUBLE,
    order_amount      DOUBLE,
    payment_mode      VARCHAR,
    order_status      VARCHAR,
    channel           VARCHAR,
    delivery_minutes  DOUBLE,
    dq_status         VARCHAR,
    failure_reason    VARCHAR,
    row_hash          VARCHAR,
    _source_file      VARCHAR,
    _source_row       BIGINT,
    _run_id           VARCHAR,
    _loaded_at        TIMESTAMP
);

-- 1. Typed columns and rule results ---------------------------------------------------------
CREATE OR REPLACE TEMP TABLE chk AS
WITH src AS (
    SELECT *,
           clean(order_id)                         AS id,
           to_timestamp_value(order_ts)            AS ts,
           upper(clean(order_status))              AS status,
           standard_payment_mode(payment_mode)     AS mode,
           to_number(quantity)                     AS qty,
           to_number(unit_price)                   AS price,
           to_number(discount_pct)                 AS disc,
           to_number(order_amount)                 AS amount,
           to_number(delivery_minutes)             AS minutes,
           clean(customer_id)                      AS cid,
           clean(product_id)                       AS pid,
           clean(store_id)                         AS sid,
           md5(concat_ws('|', order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price,
                         discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes)) AS raw_hash
    FROM raw_orders
    WHERE _run_id = getvariable('run_id')
),
customer_ids       AS (SELECT DISTINCT clean(customer_id) AS id FROM raw_customers WHERE _run_id = getvariable('run_id')),
product_ids        AS (SELECT DISTINCT clean(product_id)  AS id FROM raw_products  WHERE _run_id = getvariable('run_id')),
store_ids          AS (SELECT DISTINCT clean(store_id)    AS id FROM raw_stores    WHERE _run_id = getvariable('run_id')),
valid_customer_ids AS (SELECT customer_id AS id FROM stg_customers WHERE dq_status = 'VALID'),
valid_product_ids  AS (SELECT product_id  AS id FROM stg_products  WHERE dq_status = 'VALID'),
valid_store_ids    AS (SELECT store_id    AS id FROM stg_stores    WHERE dq_status = 'VALID')
SELECT
    _source_file, _source_row, id AS record_id, raw_hash,
    -- typed and standardised
    id                                                                        AS order_id,
    ts                                                                        AS order_ts,
    CAST(ts AS DATE)                                                          AS order_date,
    cid                                                                       AS customer_id,
    sid                                                                       AS store_id,
    pid                                                                       AS product_id,
    CASE WHEN qty = floor(qty) THEN CAST(qty AS INTEGER) END                  AS quantity,
    price                                                                     AS unit_price,
    disc                                                                      AS discount_pct,
    amount                                                                    AS order_amount,
    mode                                                                      AS payment_mode,
    status                                                                    AS order_status,
    clean(channel)                                                            AS channel,
    minutes                                                                   AS delivery_minutes,
    -- rules (names as in elt_dq_rules). Uniqueness is checked within each file.
    flag(is_missing(order_id))                                                AS fail__order_id_not_null,
    flag(NOT is_missing(order_id)
         AND row_number() OVER (PARTITION BY _source_file, id ORDER BY _source_row) > 1) AS fail__order_id_unique,
    flag(is_missing(customer_id))                                             AS fail__customer_id_not_null,
    flag(is_missing(order_ts))                                                AS fail__order_ts_not_null,
    flag(status = 'DELIVERED' AND is_missing(delivery_minutes))               AS fail__delivered_has_delivery_minutes,
    flag(NOT is_missing(order_ts) AND ts IS NULL)                             AS fail__order_ts_valid,
    flag(NOT list_contains(getvariable('valid_order_status'), status))        AS fail__order_status_valid,
    flag(list_contains(getvariable('valid_order_status'), status)
         AND order_status <> status)                                          AS fail__order_status_casing,
    flag(mode IS NULL)                                                        AS fail__payment_mode_valid,
    flag(mode IS NOT NULL AND payment_mode <> mode)                           AS fail__payment_mode_standardised,
    flag(is_missing(quantity))                                                AS fail__quantity_not_null,
    flag(is_missing(unit_price))                                              AS fail__unit_price_not_null,
    flag(is_missing(discount_pct))                                            AS fail__discount_pct_not_null,
    flag(is_missing(order_amount))                                            AS fail__order_amount_not_null,
    flag(not_a_number(quantity))                                              AS fail__quantity_is_number,
    flag(not_a_number(unit_price))                                            AS fail__unit_price_is_number,
    flag(not_a_number(discount_pct))                                          AS fail__discount_pct_is_number,
    flag(not_a_number(order_amount))                                          AS fail__order_amount_is_number,
    flag(not_a_number(delivery_minutes))                                      AS fail__delivery_minutes_is_number,
    flag(qty IS NOT NULL AND qty <> floor(qty))                               AS fail__quantity_whole_number,
    flag(disc < 0 OR disc > 100)                                              AS fail__discount_pct_0_to_100,
    flag(NOT list_contains(getvariable('valid_channels'), clean(channel)))    AS fail__channel_valid,
    flag(qty <= 0)                                                            AS fail__quantity_positive,
    flag(price <= 0)                                                          AS fail__unit_price_positive,
    flag(abs(round_even(qty * price * (1 - disc / 100), 2) - amount)
         > getvariable('amount_tolerance'))                                   AS fail__order_amount_matches_formula,
    flag(amount < 0)                                                          AS fail__order_amount_not_negative,
    flag(minutes > getvariable('max_delivery_minutes'))                       AS fail__delivery_minutes_max,
    flag((minutes < getvariable('normal_delivery_min') OR minutes > getvariable('normal_delivery_max'))
         AND NOT minutes > getvariable('max_delivery_minutes'))               AS fail__delivery_minutes_normal_range,
    flag(NOT is_missing(customer_id) AND cid NOT IN (SELECT id FROM customer_ids))         AS fail__customer_id_exists,
    flag(NOT is_missing(customer_id) AND cid IN (SELECT id FROM customer_ids)
         AND cid NOT IN (SELECT id FROM valid_customer_ids))                               AS fail__customer_id_master_valid,
    flag(NOT is_missing(product_id) AND pid NOT IN (SELECT id FROM product_ids))           AS fail__product_id_exists,
    flag(NOT is_missing(product_id) AND pid IN (SELECT id FROM product_ids)
         AND pid NOT IN (SELECT id FROM valid_product_ids))                                AS fail__product_id_master_valid,
    flag(NOT is_missing(store_id) AND sid NOT IN (SELECT id FROM store_ids))               AS fail__store_id_exists,
    flag(NOT is_missing(store_id) AND sid IN (SELECT id FROM store_ids)
         AND sid NOT IN (SELECT id FROM valid_store_ids))                                  AS fail__store_id_master_valid,
    flag(ts > getvariable('as_of'))                                           AS fail__order_ts_not_future
FROM src;

-- 2. failure_reason: every Critical rule the row breaks --------------------------------------
CREATE OR REPLACE TEMP TABLE chk_long AS
SELECT u._source_file, u._source_row, r.rule, r.rule_order, r.dimension, r.severity, r.reason, u.failed
FROM (UNPIVOT (SELECT * EXCLUDE (raw_hash) FROM chk) ON COLUMNS('^fail__') INTO NAME fail_column VALUE failed) AS u
JOIN elt_dq_rules AS r ON r.entity = 'orders' AND 'fail__' || r.rule = u.fail_column;

CREATE OR REPLACE TEMP TABLE chk_reason AS
SELECT _source_file, _source_row, string_agg(reason, '; ' ORDER BY rule_order) AS failure_reason
FROM chk_long
WHERE failed AND severity = 'Critical'
GROUP BY _source_file, _source_row;

-- 3. dq_results: row-level rules per file, plus the file-level freshness and volume checks -------
CREATE OR REPLACE TEMP TABLE chk_counts AS
SELECT regexp_replace(_source_file, '\.csv$', '') || '.' || rule AS check_name, dimension, severity,
       count(*) AS rows_checked, count(*) FILTER (WHERE failed) AS rows_failed
FROM chk_long
GROUP BY ALL

UNION ALL
-- Freshness (Timeliness, Warning): the newest order in each file must be at most N hours old.
SELECT regexp_replace(_source_file, '\.csv$', '') || '.freshness', 'Timeliness', 'Warning', 1,
       CASE WHEN max(order_ts) FILTER (WHERE order_ts <= getvariable('as_of')) IS NULL
              OR getvariable('as_of') - max(order_ts) FILTER (WHERE order_ts <= getvariable('as_of'))
                 > to_hours(getvariable('freshness_max_age_hours'))
            THEN 1 ELSE 0 END
FROM chk
GROUP BY _source_file

UNION ALL
-- Volume (Completeness, Warning): orders.csv must not shrink by more than X % compared with the previous run.
SELECT 'orders.volume_vs_previous_run', 'Completeness', 'Warning', 1,
       CASE WHEN previous.n > 0 AND current_run.n < previous.n * (1 - getvariable('volume_drop_warn_pct') / 100)
            THEN 1 ELSE 0 END
FROM (SELECT count(*) AS n FROM chk WHERE _source_file = 'orders.csv') AS current_run
LEFT JOIN (SELECT rows_checked AS n FROM dq_results
           WHERE dag_id = getvariable('dag_id') AND check_name = 'orders.order_id_unique'
             AND run_id <> getvariable('run_id')
           ORDER BY checked_at DESC LIMIT 1) AS previous ON true;

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

-- 4. One version per order_id: the latest VALID version wins ----------------------------------
--    order: VALID before REJECTED, then next-day files before orders.csv, then the newest next-day file first
CREATE OR REPLACE TEMP TABLE new_orders AS
SELECT c.order_id, c.order_ts, c.order_date, c.customer_id, c.store_id, c.product_id, c.quantity, c.unit_price,
       c.discount_pct, c.order_amount, c.payment_mode, c.order_status, c.channel, c.delivery_minutes,
       CASE WHEN r.failure_reason IS NULL THEN 'VALID' ELSE 'REJECTED' END AS dq_status,
       r.failure_reason,
       md5(c.raw_hash || '|' || coalesce(r.failure_reason, '')) AS row_hash,
       c._source_file, c._source_row,
       getvariable('run_id') AS _run_id,
       getvariable('loaded_at') AS _loaded_at
FROM chk AS c LEFT JOIN chk_reason AS r USING (_source_file, _source_row)
WHERE NOT c.fail__order_id_not_null AND NOT c.fail__order_id_unique
QUALIFY row_number() OVER (PARTITION BY c.order_id
                           ORDER BY r.failure_reason IS NULL DESC,
                                    c._source_file = 'orders.csv' ASC,
                                    c._source_file DESC) = 1;

-- 5. MERGE (upsert) into stg_orders ----------------------------------------------------------
--    The counts are kept in a temp table so the Airflow task can print them.
CREATE OR REPLACE TEMP TABLE merge_counts AS
SELECT count(*)                                                                AS rows_in_batch,
       count(*) FILTER (WHERE s.order_id IS NULL)                              AS inserted,
       count(*) FILTER (WHERE s.order_id IS NOT NULL AND s.row_hash <> n.row_hash) AS updated
FROM new_orders AS n LEFT JOIN stg_orders AS s USING (order_id);

MERGE INTO stg_orders AS s
USING new_orders AS n
ON s.order_id = n.order_id
WHEN MATCHED AND s.row_hash <> n.row_hash THEN UPDATE SET
    order_ts = n.order_ts, order_date = n.order_date, customer_id = n.customer_id, store_id = n.store_id,
    product_id = n.product_id, quantity = n.quantity, unit_price = n.unit_price, discount_pct = n.discount_pct,
    order_amount = n.order_amount, payment_mode = n.payment_mode, order_status = n.order_status,
    channel = n.channel, delivery_minutes = n.delivery_minutes, dq_status = n.dq_status,
    failure_reason = n.failure_reason, row_hash = n.row_hash, _source_file = n._source_file,
    _source_row = n._source_row, _run_id = n._run_id, _loaded_at = n._loaded_at
WHEN NOT MATCHED THEN INSERT BY NAME;

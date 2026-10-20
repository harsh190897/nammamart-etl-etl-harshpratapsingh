-- include/sql/staging/stg_stores.sql
-- Staging: stores (ELT, Part B). Input: this run's batch of raw_stores. Output: stg_stores.
-- Every staging file follows the same 4 steps:
--   1. chk        typed, standardised columns + one TRUE/FALSE column per rule (fail__<rule>)
--   2. reasons    every Critical rule a row breaks, joined into failure_reason (rule order from elt_dq_rules)
--   3. results    one dq_results row per rule; rejected rows listed in stg_rejected_rows
--   4. stg table  one row per key, with dq_status ('VALID' / 'REJECTED') and failure_reason
-- Re-running the same run replaces this run's dq_results and stg_rejected_rows rows (no duplicates).

-- 1. Typed columns and rule results ---------------------------------------------------------
CREATE OR REPLACE TEMP TABLE chk AS
WITH src AS (
    SELECT *, clean(store_id) AS id, to_number(capacity_orders_per_day) AS capacity
    FROM raw_stores
    WHERE _run_id = getvariable('run_id')
)
SELECT
    _source_file, _source_row, id AS record_id,
    -- typed and standardised
    id                                                                       AS store_id,
    value_or_null(store_name)                                                AS store_name,
    CASE WHEN list_contains(getvariable('valid_zones'), clean(zone)) THEN clean(zone)
         ELSE getvariable('unknown') END                                     AS zone,
    value_or_null(area)                                                      AS area,
    value_or_null(pincode)                                                   AS pincode,
    CAST(to_date_value(open_date) AS DATE)                                   AS open_date,
    value_or_null(manager_name)                                              AS manager_name,
    TRY_CAST(capacity AS INTEGER)                                            AS capacity_orders_per_day,
    -- rules (names as in elt_dq_rules)
    flag(is_missing(store_id))                                               AS fail__store_id_not_null,
    flag(NOT is_missing(store_id)
         AND row_number() OVER (PARTITION BY id ORDER BY _source_row) > 1)   AS fail__store_id_unique,
    flag(NOT list_contains(getvariable('valid_zones'), clean(zone)))         AS fail__zone_valid,
    flag(NOT coalesce(capacity > 0, false))                                  AS fail__capacity_positive,
    flag(NOT regexp_full_match(clean(pincode), '\d{6}'))                     AS fail__pincode_6_digits
FROM src;

-- 2. failure_reason: every Critical rule the row breaks --------------------------------------
CREATE OR REPLACE TEMP TABLE chk_long AS
SELECT u._source_file, u._source_row, r.rule, r.rule_order, r.dimension, r.severity, r.reason, u.failed
FROM (UNPIVOT chk ON COLUMNS('^fail__') INTO NAME fail_column VALUE failed) AS u
JOIN elt_dq_rules AS r ON r.entity = 'stores' AND 'fail__' || r.rule = u.fail_column;

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

-- 4. Staging table: one row per store_id (later copies of a duplicate are only in stg_rejected_rows) --
CREATE OR REPLACE TABLE stg_stores AS
SELECT c.store_id, c.store_name, c.zone, c.area, c.pincode, c.open_date, c.manager_name, c.capacity_orders_per_day,
       CASE WHEN r.failure_reason IS NULL THEN 'VALID' ELSE 'REJECTED' END AS dq_status,
       r.failure_reason,
       c._source_file, c._source_row,
       getvariable('run_id') AS _run_id
FROM chk AS c LEFT JOIN chk_reason AS r USING (_source_file, _source_row)
WHERE NOT c.fail__store_id_not_null AND NOT c.fail__store_id_unique
ORDER BY c._source_row;

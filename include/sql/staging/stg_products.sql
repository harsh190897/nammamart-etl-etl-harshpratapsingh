-- include/sql/staging/stg_products.sql
-- Staging: products (ELT, Part B). Input: this run's batch of raw_products. Output: stg_products.
-- Same 4 steps as every staging file (see stg_stores.sql): chk -> reasons -> results -> stg table.
-- The '(old)' duplicate rows of P0030 and P0140 come later in the file, so the current row is kept.

-- 1. Typed columns and rule results ---------------------------------------------------------
CREATE OR REPLACE TEMP TABLE chk AS
WITH src AS (
    SELECT *, clean(product_id) AS id, to_number(mrp) AS mrp_value, to_number(cost_price) AS cost_value
    FROM raw_products
    WHERE _run_id = getvariable('run_id')
)
SELECT
    _source_file, _source_row, id AS record_id,
    -- typed and standardised
    id                                                                       AS product_id,
    value_or_null(product_name)                                              AS product_name,
    value_or_null(category)                                                  AS category,
    value_or_null(sub_category)                                              AS sub_category,
    value_or_null(brand)                                                     AS brand,
    mrp_value                                                                AS mrp,
    cost_value                                                               AS cost_price,
    value_or_null(unit)                                                      AS unit,
    upper(clean(is_perishable)) = 'TRUE'                                     AS is_perishable,
    CAST(to_date_value(launch_date) AS DATE)                                 AS launch_date,
    -- rules (names as in elt_dq_rules)
    flag(is_missing(product_id))                                             AS fail__product_id_not_null,
    flag(NOT is_missing(product_id)
         AND row_number() OVER (PARTITION BY id ORDER BY _source_row) > 1)   AS fail__product_id_unique,
    flag(is_missing(category))                                               AS fail__category_not_null,
    flag(mrp_value IS NULL)                                                  AS fail__mrp_is_number,
    flag(cost_value IS NULL)                                                 AS fail__cost_price_is_number,
    flag(mrp_value <= 0)                                                     AS fail__mrp_positive,
    flag(cost_value > mrp_value)                                             AS fail__cost_price_not_above_mrp,
    flag(NOT list_contains(['TRUE', 'FALSE'], upper(clean(is_perishable)))) AS fail__is_perishable_valid
FROM src;

-- 2. failure_reason: every Critical rule the row breaks --------------------------------------
CREATE OR REPLACE TEMP TABLE chk_long AS
SELECT u._source_file, u._source_row, r.rule, r.rule_order, r.dimension, r.severity, r.reason, u.failed
FROM (UNPIVOT chk ON COLUMNS('^fail__') INTO NAME fail_column VALUE failed) AS u
JOIN elt_dq_rules AS r ON r.entity = 'products' AND 'fail__' || r.rule = u.fail_column;

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

-- 4. Staging table: one row per product_id -----------------------------------------------------
CREATE OR REPLACE TABLE stg_products AS
SELECT c.product_id, c.product_name, c.category, c.sub_category, c.brand, c.mrp, c.cost_price, c.unit,
       c.is_perishable, c.launch_date,
       CASE WHEN r.failure_reason IS NULL THEN 'VALID' ELSE 'REJECTED' END AS dq_status,
       r.failure_reason,
       c._source_file, c._source_row,
       getvariable('run_id') AS _run_id
FROM chk AS c LEFT JOIN chk_reason AS r USING (_source_file, _source_row)
WHERE NOT c.fail__product_id_not_null AND NOT c.fail__product_id_unique
ORDER BY c._source_row;

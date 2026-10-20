-- include/sql/staging/stg_customers.sql
-- Staging: customers (ELT, Part B). Input: this run's batch of raw_customers. Output: stg_customers.
-- Same 4 steps as every staging file (see stg_stores.sql): chk -> reasons -> results -> stg table.
-- Personal data is masked here: stg_customers has name_masked, email_masked and phone_masked only.
-- The unmasked values stay in raw_customers, which only authorised people should read (see Part E).

-- 1. Typed, masked columns and rule results -----------------------------------------------------
CREATE OR REPLACE TEMP TABLE chk AS
WITH src AS (
    SELECT *,
           clean(customer_id)               AS id,
           to_date_value(signup_date)       AS signup_value,
           title_word(clean(loyalty_tier))  AS tier
    FROM raw_customers
    WHERE _run_id = getvariable('run_id')
)
SELECT
    _source_file, _source_row, id AS record_id,
    -- typed, standardised and masked
    id                                                                       AS customer_id,
    mask_name(full_name)                                                     AS name_masked,
    mask_email(email)                                                        AS email_masked,
    mask_phone(phone)                                                        AS phone_masked,
    coalesce(value_or_null(city), getvariable('unknown'))                    AS city,
    value_or_null(area)                                                      AS area,
    value_or_null(pincode)                                                   AS pincode,
    CAST(signup_value AS DATE)                                               AS signup_date,
    tier                                                                     AS loyalty_tier,
    upper(clean(is_active)) = 'Y'                                            AS is_active,
    -- rules (names as in elt_dq_rules)
    flag(is_missing(customer_id))                                            AS fail__customer_id_not_null,
    flag(NOT is_missing(customer_id)
         AND row_number() OVER (PARTITION BY id ORDER BY _source_row) > 1)   AS fail__customer_id_unique,
    flag(signup_value IS NULL)                                               AS fail__signup_date_valid,
    flag(signup_value > getvariable('as_of'))                                AS fail__signup_date_not_future,
    flag(NOT list_contains(getvariable('loyalty_tiers'), tier))              AS fail__loyalty_tier_valid,
    flag(list_contains(getvariable('loyalty_tiers'), tier)
         AND loyalty_tier <> tier)                                           AS fail__loyalty_tier_casing,
    flag(is_missing(email))                                                  AS fail__email_not_null,
    flag(NOT is_missing(email)
         AND NOT regexp_full_match(clean(email), '[\w.+-]+@[\w-]+\.[\w.]+')) AS fail__email_format,
    flag(NOT regexp_full_match(clean(phone), '\d{10}'))                      AS fail__phone_10_digits,
    flag(NOT regexp_full_match(clean(pincode), '\d{6}'))                     AS fail__pincode_6_digits,
    flag(is_missing(city))                                                   AS fail__city_not_null,
    flag(NOT list_contains(['Y', 'N'], upper(clean(is_active))))            AS fail__is_active_valid
FROM src;

-- 2. failure_reason: every Critical rule the row breaks --------------------------------------
CREATE OR REPLACE TEMP TABLE chk_long AS
SELECT u._source_file, u._source_row, r.rule, r.rule_order, r.dimension, r.severity, r.reason, u.failed
FROM (UNPIVOT chk ON COLUMNS('^fail__') INTO NAME fail_column VALUE failed) AS u
JOIN elt_dq_rules AS r ON r.entity = 'customers' AND 'fail__' || r.rule = u.fail_column;

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

-- 4. Staging table: one row per customer_id, personal data masked -----------------------------
CREATE OR REPLACE TABLE stg_customers AS
SELECT c.customer_id, c.name_masked, c.email_masked, c.phone_masked, c.city, c.area, c.pincode,
       c.signup_date, c.loyalty_tier, c.is_active,
       CASE WHEN r.failure_reason IS NULL THEN 'VALID' ELSE 'REJECTED' END AS dq_status,
       r.failure_reason,
       c._source_file, c._source_row,
       getvariable('run_id') AS _run_id
FROM chk AS c LEFT JOIN chk_reason AS r USING (_source_file, _source_row)
WHERE NOT c.fail__customer_id_not_null AND NOT c.fail__customer_id_unique
ORDER BY c._source_row;

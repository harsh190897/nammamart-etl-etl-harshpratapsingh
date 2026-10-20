-- include/sql/common/macros.sql
-- Small reusable SQL functions (DuckDB macros) used by every staging file (ELT, Part B).
-- They are TEMP macros: they exist only for the task's connection and are recreated by every task.
-- Each one mirrors a rule of the ETL pipeline, so both pipelines judge every value the same way.
--
-- Variables they read (set by the Airflow task from include/nammamart_common.py):
--   missing_values, timestamp_format, date_format, payment_mode_keys, payment_mode_values

-- Text cleaning ----------------------------------------------------------------------
-- Trim leading/trailing whitespace; NULL becomes ''.
CREATE OR REPLACE TEMP MACRO clean(x) AS regexp_replace(coalesce(x, ''), '^\s+|\s+$', '', 'g');

-- TRUE for blank values and placeholders such as 'N/A' or 'NULL' (same list as the ETL pipeline).
CREATE OR REPLACE TEMP MACRO is_missing(x) AS list_contains(getvariable('missing_values'), upper(clean(x)));

-- The cleaned value, or NULL when it is missing.
CREATE OR REPLACE TEMP MACRO value_or_null(x) AS CASE WHEN is_missing(x) THEN NULL ELSE clean(x) END;

-- 'bRONZE' -> 'Bronze' (first letter upper case, the rest lower case).
CREATE OR REPLACE TEMP MACRO title_word(x) AS upper(left(x, 1)) || lower(substr(x, 2));

-- A rule result is always TRUE or FALSE: a comparison with NULL (e.g. NULL > 120) counts as "not failed".
CREATE OR REPLACE TEMP MACRO flag(x) AS coalesce(x, false);

-- Type conversion (NULL when missing or not convertible) -------------------------------
CREATE OR REPLACE TEMP MACRO to_number(x) AS
    CASE WHEN is_missing(x) THEN NULL ELSE TRY_CAST(clean(x) AS DOUBLE) END;

CREATE OR REPLACE TEMP MACRO not_a_number(x) AS NOT is_missing(x) AND to_number(x) IS NULL;

CREATE OR REPLACE TEMP MACRO to_timestamp_value(x) AS
    CASE WHEN is_missing(x) THEN NULL ELSE try_strptime(clean(x), getvariable('timestamp_format')) END;

CREATE OR REPLACE TEMP MACRO to_date_value(x) AS
    CASE WHEN is_missing(x) THEN NULL ELSE try_strptime(clean(x), getvariable('date_format')) END;

-- 'upi', 'UPI ', 'cod' -> 'UPI', 'UPI', 'Cash on Delivery'. Unknown values (e.g. 'Cashh') -> NULL.
CREATE OR REPLACE TEMP MACRO standard_payment_mode(x) AS
    getvariable('payment_mode_values')[list_position(getvariable('payment_mode_keys'), upper(clean(x)))];

-- PII masking (same output as mask_email / mask_phone / mask_name in nammamart_common.py) --------
-- 'pratap.singh@example.com' -> 'pr***@example.com'; no '@' -> '***'; missing -> NULL
CREATE OR REPLACE TEMP MACRO mask_email(e) AS CASE
    WHEN is_missing(e) THEN NULL
    WHEN strpos(clean(e), '@') <= 1 OR strpos(clean(e), '@') = length(clean(e)) THEN '***'
    ELSE left(clean(e), CASE WHEN strpos(clean(e), '@') - 1 > 2 THEN 2 ELSE 1 END)
         || '***@' || substr(clean(e), strpos(clean(e), '@') + 1)
END;

-- '9876543221' -> '98******21' (first 2 and last 2 digits kept); missing -> NULL
CREATE OR REPLACE TEMP MACRO digits_only(p) AS regexp_replace(coalesce(p, ''), '[^0-9]', '', 'g');
CREATE OR REPLACE TEMP MACRO mask_phone(p) AS CASE
    WHEN is_missing(p) THEN NULL
    WHEN length(digits_only(p)) < 6 THEN repeat('*', greatest(length(digits_only(p)), 3))
    ELSE left(digits_only(p), 2) || repeat('*', length(digits_only(p)) - 4) || right(digits_only(p), 2)
END;

-- 'Deepa Desai' -> 'D*** D***'; missing -> NULL
CREATE OR REPLACE TEMP MACRO mask_name(n) AS CASE
    WHEN is_missing(n) THEN NULL
    ELSE array_to_string(list_transform(string_split_regex(clean(n), '\s+'), lambda w: left(w, 1) || '***'), ' ')
END;

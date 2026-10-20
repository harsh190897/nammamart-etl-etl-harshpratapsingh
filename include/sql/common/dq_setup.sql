-- include/sql/common/dq_setup.sql
-- Data quality set-up for the ELT staging layer (Part B / Part D).
--
-- elt_dq_rules: the rule catalog. Every check is written down ONCE here: which file it applies to,
-- its quality dimension, its severity and the reason text that goes into failure_reason.
-- Each staging file computes one TRUE/FALSE column per rule, named fail__<rule>; a generic block then
-- joins those columns to this catalog to build failure_reason, dq_status and the dq_results rows.
-- The rules, names, order and reasons are the same as the ETL pipeline's (include/nammamart_etl_checks.py),
-- so both pipelines reject exactly the same rows. Thresholds come from include/nammamart_common.py.
--
--   Critical = the row is REJECTED (kept in staging with dq_status = 'REJECTED' and listed in stg_rejected_rows)
--   Warning  = the row stays VALID; the problem is only counted in dq_results
--   reason is NULL for file-level checks (freshness, volume): they are not about a single row

CREATE OR REPLACE TABLE elt_dq_rules AS
SELECT * FROM (VALUES
    -- customers.csv
    ('customers',  1, 'customer_id_not_null',            'Completeness', 'Critical', 'customer_id is missing'),
    ('customers',  2, 'customer_id_unique',              'Uniqueness',   'Critical', 'duplicate customer_id (copy of an earlier row)'),
    ('customers',  3, 'signup_date_valid',               'Validity',     'Critical', 'signup_date missing or not YYYY-MM-DD'),
    ('customers',  4, 'signup_date_not_future',          'Timeliness',   'Critical', 'signup_date is in the future'),
    ('customers',  5, 'loyalty_tier_valid',              'Validity',     'Critical', 'unknown loyalty_tier'),
    ('customers',  6, 'loyalty_tier_casing',             'Consistency',  'Warning',  'loyalty_tier casing differs'),
    ('customers',  7, 'email_not_null',                  'Completeness', 'Warning',  'email is missing'),
    ('customers',  8, 'email_format',                    'Validity',     'Warning',  'email format is invalid'),
    ('customers',  9, 'phone_10_digits',                 'Validity',     'Warning',  'phone is not 10 digits'),
    ('customers', 10, 'pincode_6_digits',                'Validity',     'Warning',  'pincode is not 6 digits'),
    ('customers', 11, 'city_not_null',                   'Completeness', 'Warning',  'city is missing'),
    ('customers', 12, 'is_active_valid',                 'Validity',     'Warning',  'is_active is not Y/N'),
    -- products.csv
    ('products',   1, 'product_id_not_null',             'Completeness', 'Critical', 'product_id is missing'),
    ('products',   2, 'product_id_unique',               'Uniqueness',   'Critical', 'duplicate product_id (copy of an earlier row)'),
    ('products',   3, 'category_not_null',               'Completeness', 'Critical', 'category is missing'),
    ('products',   4, 'mrp_is_number',                   'Validity',     'Critical', 'mrp missing or not a number'),
    ('products',   5, 'cost_price_is_number',            'Validity',     'Critical', 'cost_price missing or not a number'),
    ('products',   6, 'mrp_positive',                    'Accuracy',     'Critical', 'mrp is zero or negative'),
    ('products',   7, 'cost_price_not_above_mrp',        'Accuracy',     'Critical', 'cost_price is greater than mrp'),
    ('products',   8, 'is_perishable_valid',             'Validity',     'Warning',  'is_perishable is not True/False'),
    -- stores.csv
    ('stores',     1, 'store_id_not_null',               'Completeness', 'Critical', 'store_id is missing'),
    ('stores',     2, 'store_id_unique',                 'Uniqueness',   'Critical', 'duplicate store_id (copy of an earlier row)'),
    ('stores',     3, 'zone_valid',                      'Completeness', 'Warning',  'zone is missing or not ' || array_to_string(getvariable('valid_zones'), '/')),
    ('stores',     4, 'capacity_positive',               'Accuracy',     'Warning',  'capacity_orders_per_day is not > 0'),
    ('stores',     5, 'pincode_6_digits',                'Validity',     'Warning',  'pincode is not 6 digits'),
    -- orders.csv and every next-day orders file
    ('orders',     1, 'order_id_not_null',               'Completeness', 'Critical', 'order_id is missing'),
    ('orders',     2, 'order_id_unique',                 'Uniqueness',   'Critical', 'duplicate order_id (copy of an earlier row)'),
    ('orders',     3, 'customer_id_not_null',            'Completeness', 'Critical', 'customer_id is missing'),
    ('orders',     4, 'order_ts_not_null',               'Completeness', 'Critical', 'order_ts is missing'),
    ('orders',     5, 'delivered_has_delivery_minutes',  'Completeness', 'Critical', 'DELIVERED order has no delivery_minutes'),
    ('orders',     6, 'order_ts_valid',                  'Validity',     'Critical', 'order_ts is not a valid YYYY-MM-DD HH:MM:SS timestamp'),
    ('orders',     7, 'order_status_valid',              'Validity',     'Critical', 'order_status is not ' || array_to_string(getvariable('valid_order_status'), '/')),
    ('orders',     8, 'order_status_casing',             'Validity',     'Warning',  'order_status needed upper-casing'),
    ('orders',     9, 'payment_mode_valid',              'Validity',     'Critical', 'payment_mode is not a known mode'),
    ('orders',    10, 'payment_mode_standardised',       'Validity',     'Warning',  'payment_mode needed standardising'),
    ('orders',    11, 'quantity_not_null',               'Completeness', 'Critical', 'quantity is missing'),
    ('orders',    12, 'unit_price_not_null',             'Completeness', 'Critical', 'unit_price is missing'),
    ('orders',    13, 'discount_pct_not_null',           'Completeness', 'Critical', 'discount_pct is missing'),
    ('orders',    14, 'order_amount_not_null',           'Completeness', 'Critical', 'order_amount is missing'),
    ('orders',    15, 'quantity_is_number',              'Validity',     'Critical', 'quantity is not a number'),
    ('orders',    16, 'unit_price_is_number',            'Validity',     'Critical', 'unit_price is not a number'),
    ('orders',    17, 'discount_pct_is_number',          'Validity',     'Critical', 'discount_pct is not a number'),
    ('orders',    18, 'order_amount_is_number',          'Validity',     'Critical', 'order_amount is not a number'),
    ('orders',    19, 'delivery_minutes_is_number',      'Validity',     'Critical', 'delivery_minutes is not a number'),
    ('orders',    20, 'quantity_whole_number',           'Validity',     'Critical', 'quantity is not a whole number'),
    ('orders',    21, 'discount_pct_0_to_100',           'Validity',     'Critical', 'discount_pct outside 0-100'),
    ('orders',    22, 'channel_valid',                   'Validity',     'Warning',  'channel is not ' || array_to_string(getvariable('valid_channels'), '/')),
    ('orders',    23, 'quantity_positive',               'Accuracy',     'Critical', 'quantity is zero or negative'),
    ('orders',    24, 'unit_price_positive',             'Accuracy',     'Critical', 'unit_price is zero or negative'),
    ('orders',    25, 'order_amount_matches_formula',    'Accuracy',     'Critical', 'order_amount != quantity x unit_price x (1 - discount_pct/100)'),
    ('orders',    26, 'order_amount_not_negative',       'Accuracy',     'Critical', 'order_amount is negative'),
    ('orders',    27, 'delivery_minutes_max',            'Accuracy',     'Critical', 'delivery_minutes above ' || getvariable('max_delivery_minutes')),
    ('orders',    28, 'delivery_minutes_normal_range',   'Accuracy',     'Warning',  'delivery_minutes outside the normal '
                                                                                     || getvariable('normal_delivery_min') || '-'
                                                                                     || getvariable('normal_delivery_max') || ' range'),
    ('orders',    29, 'customer_id_exists',              'Consistency',  'Critical', 'customer_id not found in customers'),
    ('orders',    30, 'customer_id_master_valid',        'Consistency',  'Critical', 'customer_id points to a quarantined customers row'),
    ('orders',    31, 'product_id_exists',               'Consistency',  'Critical', 'product_id not found in products'),
    ('orders',    32, 'product_id_master_valid',         'Consistency',  'Critical', 'product_id points to a quarantined products row'),
    ('orders',    33, 'store_id_exists',                 'Consistency',  'Critical', 'store_id not found in stores'),
    ('orders',    34, 'store_id_master_valid',           'Consistency',  'Critical', 'store_id points to a quarantined stores row'),
    ('orders',    35, 'order_ts_not_future',             'Timeliness',   'Critical', 'order_ts is in the future'),
    ('orders',    36, 'freshness',                       'Timeliness',   'Warning',  NULL),
    ('orders',    37, 'volume_vs_previous_run',          'Completeness', 'Warning',  NULL),
    -- payments.csv
    ('payments',   1, 'payment_id_not_null',             'Completeness', 'Critical', 'payment_id is missing'),
    ('payments',   2, 'payment_id_unique',               'Uniqueness',   'Critical', 'duplicate payment_id (copy of an earlier row)'),
    ('payments',   3, 'order_id_not_null',               'Completeness', 'Critical', 'order_id is missing'),
    ('payments',   4, 'payment_ts_valid',                'Validity',     'Critical', 'payment_ts missing or not a valid timestamp'),
    ('payments',   5, 'payment_status_valid',            'Validity',     'Critical', 'payment_status is not ' || array_to_string(getvariable('valid_payment_status'), '/')),
    ('payments',   6, 'payment_mode_valid',              'Validity',     'Critical', 'payment_mode is not a known mode'),
    ('payments',   7, 'payment_mode_standardised',       'Validity',     'Warning',  'payment_mode needed standardising'),
    ('payments',   8, 'amount_paid_is_number',           'Validity',     'Critical', 'amount_paid missing or not a number'),
    ('payments',   9, 'amount_paid_not_negative',        'Accuracy',     'Critical', 'amount_paid is negative'),
    ('payments',  10, 'order_id_exists',                 'Consistency',  'Warning',  'payment for an order_id not in the orders files'),
    ('payments',  11, 'gateway_ref_for_online_success',  'Completeness', 'Warning',  'online SUCCESS payment has no gateway_ref'),
    ('payments',  12, 'payment_ts_not_future',           'Timeliness',   'Critical', 'payment_ts is in the future')
) AS rules(entity, rule_order, rule, dimension, severity, reason);

-- ELT "quarantine": every row rejected by staging, per run (duplicate copies included), with the reason.
-- The row itself is never copied: it stays in the raw table and is found with
-- (_run_id, source_file, source_row) = (_run_id, _source_file, _source_row).
CREATE TABLE IF NOT EXISTS stg_rejected_rows (
    _run_id         VARCHAR,
    source_file     VARCHAR,
    source_row      BIGINT,
    record_id       VARCHAR,
    failure_reason  VARCHAR,
    rejected_at     TIMESTAMP
);

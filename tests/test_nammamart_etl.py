"""Unit tests for the ETL data quality rules (no Airflow needed).

Run locally from the project root:   pytest tests/test_nammamart_etl.py -v
Run inside Astro (Docker):           astro dev pytest tests/test_nammamart_etl.py

The expected numbers come from the Part 0 profiling (SUBMISSION.md, Data Issues Log).
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))
import nammamart_common as nc  # noqa: E402
import nammamart_etl_checks as ck  # noqa: E402

AS_OF = pd.Timestamp("2026-10-20 09:00")


def failed(result, rule):
    """rows_failed for one check, e.g. failed(orders, 'customer_id_not_null')."""
    prefix = Path(result.file).stem
    return next(c["rows_failed"] for c in result.checks if c["check_name"] == f"{prefix}.{rule}")


@pytest.fixture(scope="module")
def raw():
    data = {entity: nc.read_text_csv(nc.DATA_DIR / name) for entity, name in nc.SOURCE_FILES.items()}
    data["increment"] = nc.read_text_csv(nc.DATA_DIR / nc.INCREMENT_FILE)
    return data


@pytest.fixture(scope="module")
def validated(raw):
    customers = ck.validate_customers(raw["customers"], AS_OF)
    products = ck.validate_products(raw["products"])
    stores = ck.validate_stores(raw["stores"])

    def ids(df, key):
        return set(df[key].str.strip())

    refs = {"customer_id": (ids(raw["customers"], "customer_id"), ids(customers.passed, "customer_id")),
            "product_id": (ids(raw["products"], "product_id"), ids(products.passed, "product_id")),
            "store_id": (ids(raw["stores"], "store_id"), ids(stores.passed, "store_id"))}
    orders = ck.validate_orders(raw["orders"], "orders.csv", refs, AS_OF)
    increment = ck.validate_orders(raw["increment"], nc.INCREMENT_FILE, refs, AS_OF)
    payments = ck.validate_payments(raw["payments"], ids(raw["orders"], "order_id") | ids(raw["increment"], "order_id"),
                                    AS_OF)
    return dict(customers=customers, products=products, stores=stores, orders=orders, increment=increment,
                payments=payments)


# ── Validation: counts match the Part 0 Data Issues Log ───────────────────────────────
def test_orders_checks_match_profiling(validated):
    o = validated["orders"]
    assert failed(o, "order_id_unique") == 12
    assert failed(o, "customer_id_not_null") == 18
    assert failed(o, "order_ts_not_null") == 2
    assert failed(o, "order_ts_valid") == 4
    assert failed(o, "order_status_valid") == 3
    assert failed(o, "order_status_casing") == 2
    assert failed(o, "payment_mode_valid") == 2
    assert failed(o, "payment_mode_standardised") == 8
    assert failed(o, "discount_pct_0_to_100") == 3
    assert failed(o, "quantity_positive") == 10
    assert failed(o, "order_amount_matches_formula") == 18
    assert failed(o, "delivery_minutes_max") == 6
    assert failed(o, "delivered_has_delivery_minutes") == 8
    assert failed(o, "product_id_exists") == 9
    assert failed(o, "store_id_exists") == 5
    assert failed(o, "order_ts_not_future") == 7


def test_master_and_payment_checks_match_profiling(validated):
    c, p, s, pay = validated["customers"], validated["products"], validated["stores"], validated["payments"]
    assert failed(c, "customer_id_unique") == 5
    assert failed(c, "loyalty_tier_casing") == 8
    assert failed(c, "email_not_null") == 4
    assert failed(c, "email_format") == 8
    assert failed(p, "product_id_unique") == 2
    assert failed(p, "category_not_null") == 3
    assert failed(p, "cost_price_not_above_mrp") == 5
    assert failed(s, "zone_valid") == 1
    assert failed(pay, "payment_id_unique") == 4
    assert failed(pay, "amount_paid_not_negative") == 6
    assert failed(pay, "order_id_exists") == 8


def test_every_dimension_is_covered(validated):
    dimensions = {c["dimension"] for r in validated.values() for c in r.checks}
    assert dimensions == set(nc.DQ_DIMENSIONS)


def test_passed_plus_failed_equals_input(raw, validated):
    for name, result in validated.items():
        assert len(result.passed) + len(result.failed) == len(raw[name])
        assert result.failed["failure_reason"].str.len().gt(0).all()


def test_missing_zone_is_only_a_warning(validated):
    assert len(validated["stores"].passed) == 15     # S07 is kept; transform shows its zone as Unknown


def test_volume_check():
    assert ck.volume_check("x", 50, 2412)["rows_failed"] == 1
    assert ck.volume_check("x", 2412, 2412)["rows_failed"] == 0
    assert ck.volume_check("x", 2412, None)["rows_failed"] == 0

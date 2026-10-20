"""Unit tests for the ETL helper modules: quality rules, transform and load (no Airflow needed).

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
import nammamart_etl_load as ld  # noqa: E402
import nammamart_etl_transform as tf  # noqa: E402

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


# ── Transform ─────────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def tables(validated):
    dim_product = tf.build_dim_product(validated["products"].passed)
    return {
        "etl_dim_customer": tf.build_dim_customer(validated["customers"].passed),
        "etl_dim_product": dim_product,
        "etl_dim_store": tf.build_dim_store(validated["stores"].passed),
        "etl_fact_orders": tf.build_fact_orders([validated["orders"].passed, validated["increment"].passed],
                                                dim_product),
        "etl_fact_payments": tf.build_payments(validated["payments"].passed),
    }


def test_customer_pii_is_masked(tables):
    dim = tables["etl_dim_customer"]
    assert not {"email", "phone", "full_name"} & set(dim.columns)
    assert dim["phone_masked"].str.fullmatch(r"\d{2}\*+\d{2}").all()
    # valid emails keep 1-2 characters + domain; invalid ones (no '@') are fully hidden as '***'
    assert dim["email_masked"].dropna().str.fullmatch(r".{1,2}\*\*\*@.+|\*\*\*").all()
    assert dim["loyalty_tier"].isin(nc.LOYALTY_TIERS).all()


def test_fact_orders(tables):
    fact = tables["etl_fact_orders"]
    assert fact["order_id"].is_unique
    assert set(fact["payment_mode"]) <= set(nc.VALID_PAYMENT_MODES)
    assert (fact.loc[fact["order_status"] != "DELIVERED", "net_revenue"] == 0).all()
    corrected = fact.set_index("order_id").loc["ORD000017"]       # DELIVERED in orders.csv, RETURNED next day
    assert corrected["order_status"] == "RETURNED" and corrected["net_revenue"] == 0
    assert corrected["source_file"] == nc.INCREMENT_FILE


def test_unknown_zone(tables):
    assert tables["etl_dim_store"].set_index("store_id").loc["S07", "zone"] == nc.UNKNOWN


# ── Load ───────────────────────────────────────────────────────────
def test_load_is_idempotent(tables, tmp_path):
    paths = {name: nc.write_parquet(df, tmp_path / f"{name}.parquet") for name, df in tables.items()}
    with nc.get_connection(tmp_path / "wh.duckdb") as con:
        first = ld.load_star_schema(con, paths, "run_1")
        second = ld.load_star_schema(con, paths, "run_2")
        masked = con.execute("SELECT count(*) FROM etl_dim_customer WHERE email_masked LIKE '%@%' "
                             "AND email_masked NOT LIKE '%***@%'").fetchone()[0]

    assert first["fact_inserted"] == len(tables["etl_fact_orders"])
    assert (second["fact_inserted"], second["fact_updated"]) == (0, 0)
    assert second["etl_fact_orders"] == first["etl_fact_orders"]
    assert masked == 0


def test_increment_updates_without_duplicates(validated, tables, tmp_path):
    dim_product = tables["etl_dim_product"]
    day1 = dict(tables, etl_fact_orders=tf.build_fact_orders([validated["orders"].passed], dim_product))
    day1_paths = {n: nc.write_parquet(df, tmp_path / f"d1_{n}.parquet") for n, df in day1.items()}
    day2_paths = {n: nc.write_parquet(df, tmp_path / f"d2_{n}.parquet") for n, df in tables.items()}
    with nc.get_connection(tmp_path / "wh.duckdb") as con:
        ld.load_star_schema(con, day1_paths, "day_1")
        day2 = ld.load_star_schema(con, day2_paths, "day_2")
    new_orders = len(tables["etl_fact_orders"]) - len(day1["etl_fact_orders"])
    assert day2["fact_inserted"] == new_orders
    assert day2["fact_updated"] == 9            # 10 corrections, one is quarantined (quarantined product)
    assert day2["etl_fact_orders"] == len(tables["etl_fact_orders"])


# ── Reconcile + report (Part A.4) ─────────────────────────────────────────────────────
def test_reconciliation_and_reports(tables, tmp_path):
    paths = {name: nc.write_parquet(df, tmp_path / f"{name}.parquet") for name, df in tables.items()}
    with nc.get_connection(tmp_path / "wh.duckdb") as con:
        ld.load_star_schema(con, paths, "run_1")
        recon = ld.reconcile(con)
        recon_again = ld.reconcile(con)                            # rebuilt, never appended
        report = ld.build_reports(con)
        zone_total = con.execute("SELECT sum(net_revenue) FROM etl_daily_zone_revenue").fetchone()[0]
        fact_total = con.execute("SELECT sum(net_revenue) FROM etl_fact_orders "
                                 "WHERE order_status = 'DELIVERED'").fetchone()[0]
        category_total = con.execute("SELECT sum(revenue) FROM etl_category_margin").fetchone()[0]

    assert recon == recon_again
    assert recon["by_reason"]["PAYMENT_FOR_UNKNOWN_ORDER"] == 8      # matches the Part 0 issues log
    assert recon["by_reason"]["RETURNED_NOT_REFUNDED"] == 9          # the 9 applied DELIVERED -> RETURNED corrections
    assert "AMOUNT_MISMATCH" in recon["by_reason"]
    assert report["category_rows"] == 6
    assert round(zone_total, 2) == round(fact_total, 2) == round(category_total, 2)

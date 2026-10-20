"""Unit tests for the ELT pipeline's SQL (no Airflow needed): the SQL files run on a temporary DuckDB.

Run locally from the project root:   pytest tests/test_nammamart_elt.py -v
Run inside Astro (Docker):           astro dev pytest tests/test_nammamart_elt.py
"""
import re
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))
import nammamart_common as nc  # noqa: E402
import nammamart_elt as elt  # noqa: E402

RAW_FILES = {"raw_customers": "customers.csv", "raw_products": "products.csv", "raw_stores": "stores.csv",
             "raw_orders": "orders.csv", "raw_payments": "payments.csv"}
RAW_TABLES = list(RAW_FILES)


def load_raw(con, run_id, data_dir=nc.DATA_DIR, increments=(nc.INCREMENT_FILE,)):
    """What the DAG's load_raw + load_raw_increments tasks do, without Airflow."""
    variables = {"run_id": run_id, "loaded_at": datetime(2026, 10, 20, 3, 30)}
    for entity, file_name in nc.SOURCE_FILES.items():
        variables[f"path_{entity}"] = str(Path(data_dir) / file_name)
    elt.run_sql_files(con, ["raw/raw_tables.sql", "raw/load_raw.sql"], variables)
    for file_name in increments:
        elt.run_sql_files(con, ["raw/load_raw_increments.sql"],
                          {"run_id": run_id, "loaded_at": datetime(2026, 10, 20, 3, 30),
                           "path_increment": str(Path(data_dir) / file_name)})


@pytest.fixture()
def con(tmp_path):
    connection = duckdb.connect(str(tmp_path / "elt.duckdb"))
    yield connection
    connection.close()


# ── Extract helpers ───────────────────────────────────────────────────────────────────
def test_header_and_row_count_without_pandas():
    header, rows = elt.read_header_and_count(nc.DATA_DIR / "orders.csv")
    assert header == nc.EXPECTED_COLUMNS["orders"]
    assert rows == 2412


def test_check_header_catches_a_renamed_column():
    header = [c if c != "order_amount" else "total" for c in nc.EXPECTED_COLUMNS["orders"]]
    with pytest.raises(ValueError, match=r"missing columns \['order_amount'\], unexpected columns \['total'\]"):
        elt.check_header(header, nc.EXPECTED_COLUMNS["orders"], "orders.csv")


def test_snapshot_marks_the_increment_file(tmp_path, monkeypatch):
    monkeypatch.setattr(nc, "TMP_DIR", tmp_path / "tmp")
    result = elt.snapshot_source_files("orders", "manual__test")
    assert [(f["file"], f["kind"], f["rows"]) for f in result["files"]] == [
        ("orders.csv", "daily", 2412), (nc.INCREMENT_FILE, "increment", 160)]
    assert all(Path(f["path"]).exists() for f in result["files"])


# ── Raw layer ─────────────────────────────────────────────────────────────────────────
def test_raw_tables_are_an_exact_copy_of_the_files(con):
    load_raw(con, "run_1")
    for table, file_name in RAW_FILES.items():
        raw = con.execute(f"""SELECT * EXCLUDE (_source_file, _source_row, _run_id, _loaded_at) FROM {table}
                              WHERE _source_file = ? ORDER BY _source_row""", [file_name]).df()
        source = pd.read_csv(nc.DATA_DIR / file_name, dtype=str, keep_default_na=False)
        assert raw.equals(source), f"{table} differs from {file_name}"
        rows = con.execute(f"SELECT list(_source_row ORDER BY _source_row) FROM {table} WHERE _source_file = ?",
                           [file_name]).fetchone()[0]
        assert rows == list(range(2, len(source) + 2))           # CSV line numbers, header is line 1


def test_bad_values_are_kept_as_received(con):
    load_raw(con, "run_1")
    assert con.execute("SELECT count(*) FROM raw_orders WHERE payment_mode = 'UPI '").fetchone()[0] == 2
    assert con.execute("SELECT count(*) FROM raw_orders WHERE order_ts = 'N/A'").fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM raw_orders WHERE customer_id = ''").fetchone()[0] == 18
    assert con.execute("SELECT count(*) FROM raw_orders WHERE customer_id IS NULL").fetchone()[0] == 0


def test_increment_is_appended_to_raw_orders(con):
    load_raw(con, "run_1")
    by_file = dict(con.execute("SELECT _source_file, count(*) FROM raw_orders GROUP BY 1").fetchall())
    assert by_file == {"orders.csv": 2412, nc.INCREMENT_FILE: 160}


def test_each_file_is_loaded_once_per_run_and_raw_grows_per_run(con):
    load_raw(con, "run_1")
    load_raw(con, "run_1")                                        # retry of the same run: nothing added
    first = elt.table_counts(con, RAW_TABLES)
    assert first == {"raw_customers": 405, "raw_products": 152, "raw_stores": 15,
                     "raw_orders": 2572, "raw_payments": 2228}
    load_raw(con, "run_2")                                        # next run: a new batch is appended
    second = elt.table_counts(con, RAW_TABLES)
    assert second == {t: 2 * n for t, n in first.items()}
    assert elt.table_counts(con, RAW_TABLES, "run_2") == first


def test_schema_change_fails_loudly_and_loads_nothing(con, tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    for file_name in nc.SOURCE_FILES.values():
        text = (nc.DATA_DIR / file_name).read_text()
        if file_name == "orders.csv":
            text = text.replace("order_amount", "total", 1)       # experiment 6: renamed column
        (data / file_name).write_text(text)
    elt.run_sql_files(con, ["raw/raw_tables.sql"])                # tables exist from earlier runs
    with pytest.raises(duckdb.BinderException, match='does not have a column with name "total"'):
        load_raw(con, "run_1", data_dir=data, increments=())
    assert elt.table_counts(con, RAW_TABLES) == {t: 0 for t in RAW_TABLES}   # whole load rolled back


def test_sql_variable_names_are_validated(con):
    with pytest.raises(ValueError, match="Invalid SQL variable name"):
        elt.run_sql_files(con, [], {"run_id; DROP TABLE x": "1"})


# ── Staging layer and data quality in SQL (Part B.2) ──────────────────────────────────
STAGING_ORDER = ["stores", "products", "customers", "orders", "payments"]
AS_OF = pd.Timestamp("2026-10-20 09:00")


def run_staging(con, run_id, as_of=AS_OF, dag_id="elt_test"):
    """What the DAG's five staging tasks do, without Airflow."""
    nc.init_audit_tables(con)
    for entity in STAGING_ORDER:
        elt.run_sql_files(con, ["common/macros.sql", "common/dq_setup.sql", f"staging/stg_{entity}.sql"],
                          elt.staging_variables(run_id, dag_id, as_of))


@pytest.fixture(scope="module")
def staged(tmp_path_factory):
    connection = duckdb.connect(str(tmp_path_factory.mktemp("elt") / "elt.duckdb"))
    load_raw(connection, "run_1")
    run_staging(connection, "run_1")
    yield connection
    connection.close()


@pytest.fixture(scope="module")
def etl():
    """The ETL pipeline's validation and transform on the same files, for comparison."""
    import nammamart_etl_checks as ck
    import nammamart_etl_transform as tf

    raw = {entity: nc.read_text_csv(nc.DATA_DIR / name) for entity, name in nc.SOURCE_FILES.items()}
    increment = nc.read_text_csv(nc.DATA_DIR / nc.INCREMENT_FILE)
    customers = ck.validate_customers(raw["customers"], AS_OF)
    products = ck.validate_products(raw["products"])
    stores = ck.validate_stores(raw["stores"])

    def ids(df, key):
        return set(df[key].str.strip())

    refs = {"customer_id": (ids(raw["customers"], "customer_id"), ids(customers.passed, "customer_id")),
            "product_id": (ids(raw["products"], "product_id"), ids(products.passed, "product_id")),
            "store_id": (ids(raw["stores"], "store_id"), ids(stores.passed, "store_id"))}
    orders = ck.validate_orders(raw["orders"], "orders.csv", refs, AS_OF)
    increment_v = ck.validate_orders(increment, nc.INCREMENT_FILE, refs, AS_OF)
    payments = ck.validate_payments(raw["payments"], ids(raw["orders"], "order_id") | ids(increment, "order_id"), AS_OF)
    results = [customers, products, stores, orders, increment_v, payments]
    checks = [c for r in results for c in r.checks] + [ck.volume_check("orders.volume_vs_previous_run", 2412, None)]
    return {
        "checks": pd.DataFrame(checks),
        "rejected": pd.concat([r.failed[["source_file", "source_row", "failure_reason"]] for r in results]),
        "fact": tf.build_fact_orders([orders.passed, increment_v.passed], tf.build_dim_product(products.passed)),
        "dim_customer": tf.build_dim_customer(customers.passed),
    }


def test_every_rule_column_has_a_catalog_entry(staged):
    for entity in STAGING_ORDER:
        sql = (nc.SQL_DIR / "staging" / f"stg_{entity}.sql").read_text()
        columns = set(re.findall(r"AS (fail__\w+)", sql))
        rules = {f"fail__{r}" for (r,) in staged.execute(
            "SELECT rule FROM elt_dq_rules WHERE entity = ? AND reason IS NOT NULL", [entity]).fetchall()}
        assert columns == rules, f"stg_{entity}.sql and elt_dq_rules disagree: {columns ^ rules}"


def test_dq_results_match_the_etl_checks_exactly(staged, etl):
    elt_checks = staged.execute("""SELECT check_name, dimension, severity, rows_checked, rows_failed
                                   FROM dq_results WHERE run_id = 'run_1'""").df()
    merged = etl["checks"].merge(elt_checks, on="check_name", how="outer", suffixes=("_etl", "_elt"), indicator=True)
    assert (merged["_merge"] == "both").all(), merged.loc[merged["_merge"] != "both", "check_name"].tolist()
    for column in ("dimension", "severity", "rows_checked", "rows_failed"):
        different = merged[merged[f"{column}_etl"] != merged[f"{column}_elt"]]
        assert different.empty, different[["check_name", f"{column}_etl", f"{column}_elt"]]
    assert len(elt_checks) == 110 and set(elt_checks["dimension"]) == set(nc.DQ_DIMENSIONS)


def test_rejected_rows_and_reasons_match_the_etl_quarantine(staged, etl):
    elt_rejected = staged.execute("SELECT source_file, source_row, failure_reason FROM stg_rejected_rows "
                                  "WHERE _run_id = 'run_1'").df()
    etl_rejected = etl["rejected"].astype({"source_row": "int64"})
    merged = etl_rejected.merge(elt_rejected, on=["source_file", "source_row"], how="outer",
                                suffixes=("_etl", "_elt"), indicator=True)
    assert (merged["_merge"] == "both").all()
    assert (merged["failure_reason_etl"] == merged["failure_reason_elt"]).all()
    assert elt_rejected.groupby("source_file").size().to_dict() == {
        "customers.csv": 8, "products.csv": 10, "orders.csv": 235, nc.INCREMENT_FILE: 15, "payments.csv": 19}


def test_valid_orders_match_the_etl_fact_table(staged, etl):
    elt_valid = staged.execute("""SELECT order_id, order_status, order_amount, payment_mode, _source_file
                                  FROM stg_orders WHERE dq_status = 'VALID'""").df()
    fact = etl["fact"]
    assert set(elt_valid["order_id"]) == set(fact["order_id"]) and len(elt_valid) == 2313
    joined = fact.merge(elt_valid, on="order_id", suffixes=("_etl", "_elt"))
    assert (joined["order_status_etl"] == joined["order_status_elt"]).all()
    assert (joined["payment_mode_etl"] == joined["payment_mode_elt"]).all()
    assert (joined["source_file"] == joined["_source_file"]).all()
    assert ((joined["order_amount_etl"] - joined["order_amount_elt"]).abs() < 1e-9).all()


def test_staging_masks_personal_data_like_the_etl(staged, etl):
    columns = [row[0] for row in staged.execute("DESCRIBE stg_customers").fetchall()]
    assert not {"full_name", "email", "phone"} & set(columns)
    elt_dim = staged.execute("""SELECT customer_id, name_masked, email_masked, phone_masked, city, loyalty_tier
                                FROM stg_customers WHERE dq_status = 'VALID'""").df().set_index("customer_id")
    etl_dim = etl["dim_customer"].set_index("customer_id")
    assert set(elt_dim.index) == set(etl_dim.index)
    for column in ("name_masked", "email_masked", "phone_masked", "city", "loyalty_tier"):
        assert (elt_dim.loc[etl_dim.index, column].fillna("<NULL>") == etl_dim[column].fillna("<NULL>")).all(), column


def test_staging_keeps_one_row_per_key_with_status(staged):
    counts = {t: dict(staged.execute(f"SELECT dq_status, count(*) FROM {t} GROUP BY 1").fetchall())
              for t in ("stg_customers", "stg_products", "stg_stores", "stg_orders", "stg_payments")}
    assert counts == {"stg_customers": {"VALID": 397, "REJECTED": 3}, "stg_products": {"VALID": 142, "REJECTED": 8},
                      "stg_stores": {"VALID": 15}, "stg_orders": {"VALID": 2313, "REJECTED": 237},
                      "stg_payments": {"VALID": 2209, "REJECTED": 15}}
    zone = staged.execute("SELECT zone, dq_status FROM stg_stores WHERE store_id = 'S07'").fetchone()
    assert zone == ("Unknown", "VALID")


def test_rerun_of_the_same_run_creates_no_duplicates(tmp_path):
    con = duckdb.connect(str(tmp_path / "elt.duckdb"))
    load_raw(con, "run_1")
    run_staging(con, "run_1")
    run_staging(con, "run_1")                                      # Airflow retry / Clear
    assert con.execute("SELECT count(*) FROM dq_results").fetchone()[0] == 110
    assert con.execute("SELECT count(*) FROM stg_rejected_rows").fetchone()[0] == 287
    assert con.execute("SELECT * FROM merge_counts").fetchone() == (2550, 0, 0)
    con.close()


def test_next_run_merges_nothing_and_raw_grows(tmp_path):
    con = duckdb.connect(str(tmp_path / "elt.duckdb"))
    load_raw(con, "run_1")
    run_staging(con, "run_1")
    load_raw(con, "run_2")
    run_staging(con, "run_2")
    assert con.execute("SELECT * FROM merge_counts").fetchone() == (2550, 0, 0)    # 0 inserted, 0 updated
    assert con.execute("SELECT count(*) FROM stg_orders").fetchone()[0] == 2550
    assert con.execute("SELECT count(*) FROM raw_orders").fetchone()[0] == 2 * 2572
    con.close()


def test_increment_merge_updates_10_corrections_and_adds_150_orders(tmp_path):
    con = duckdb.connect(str(tmp_path / "elt.duckdb"))
    load_raw(con, "day_1", increments=())                          # experiment 3, day 1: no increment file
    run_staging(con, "day_1")
    assert con.execute("SELECT * FROM merge_counts").fetchone() == (2400, 2400, 0)
    load_raw(con, "day_2")                                         # day 2: increment file added
    run_staging(con, "day_2")
    assert con.execute("SELECT * FROM merge_counts").fetchone() == (2550, 150, 10)
    assert con.execute("SELECT count(*), count(DISTINCT order_id) FROM stg_orders").fetchone() == (2550, 2550)
    corrected = con.execute("""SELECT order_status, _source_file FROM stg_orders
                               WHERE order_id = 'ORD000017'""").fetchone()
    assert corrected == ("RETURNED", nc.INCREMENT_FILE)
    con.close()


def test_volume_check_warns_when_orders_drop_by_more_than_90_percent(tmp_path):
    con = duckdb.connect(str(tmp_path / "elt.duckdb"))
    load_raw(con, "run_1")
    run_staging(con, "run_1")
    small = tmp_path / "small"
    small.mkdir()
    for file_name in nc.SOURCE_FILES.values():
        lines = (nc.DATA_DIR / file_name).read_text().splitlines(keepends=True)
        (small / file_name).write_text("".join(lines[:51]) if file_name == "orders.csv" else "".join(lines))
    load_raw(con, "run_2", data_dir=small, increments=())          # experiment 7: only 50 orders
    run_staging(con, "run_2")
    status = con.execute("""SELECT rows_failed, status FROM dq_results
                            WHERE run_id = 'run_2' AND check_name = 'orders.volume_vs_previous_run'""").fetchone()
    assert status == (1, "WARN")
    con.close()


# ── Mart layer (Part B.3) ─────────────────────────────────────────────────────────────
MARTS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla", "mart_payment_reconciliation",
         "mart_payment_mode_revenue", "mart_top_customers"]


def run_marts(con, run_id="run_1"):
    for mart in MARTS:
        elt.run_sql_files(con, [f"marts/{mart}.sql"], elt.staging_variables(run_id, "elt_test", AS_OF))


@pytest.fixture(scope="module")
def marts_and_etl(tmp_path_factory):
    """ELT marts and the ETL report tables built from the same files, in one database."""
    import nammamart_etl_checks as ck
    import nammamart_etl_load as ld
    import nammamart_etl_transform as tf

    folder = tmp_path_factory.mktemp("marts")
    con = duckdb.connect(str(folder / "both.duckdb"))
    load_raw(con, "run_1")
    run_staging(con, "run_1")
    run_marts(con)

    raw = {entity: nc.read_text_csv(nc.DATA_DIR / name) for entity, name in nc.SOURCE_FILES.items()}
    increment = nc.read_text_csv(nc.DATA_DIR / nc.INCREMENT_FILE)
    customers = ck.validate_customers(raw["customers"], AS_OF)
    products = ck.validate_products(raw["products"])
    stores = ck.validate_stores(raw["stores"])

    def ids(df, key):
        return set(df[key].str.strip())

    refs = {"customer_id": (ids(raw["customers"], "customer_id"), ids(customers.passed, "customer_id")),
            "product_id": (ids(raw["products"], "product_id"), ids(products.passed, "product_id")),
            "store_id": (ids(raw["stores"], "store_id"), ids(stores.passed, "store_id"))}
    orders = ck.validate_orders(raw["orders"], "orders.csv", refs, AS_OF)
    increment_v = ck.validate_orders(increment, nc.INCREMENT_FILE, refs, AS_OF)
    payments = ck.validate_payments(raw["payments"], ids(raw["orders"], "order_id") | ids(increment, "order_id"), AS_OF)
    dim_product = tf.build_dim_product(products.passed)
    tables = {"etl_dim_customer": tf.build_dim_customer(customers.passed), "etl_dim_product": dim_product,
              "etl_dim_store": tf.build_dim_store(stores.passed),
              "etl_fact_orders": tf.build_fact_orders([orders.passed, increment_v.passed], dim_product),
              "etl_fact_payments": tf.build_payments(payments.passed)}
    paths = {name: nc.write_parquet(df, folder / f"{name}.parquet") for name, df in tables.items()}
    ld.load_star_schema(con, paths, "etl_run_1")
    ld.reconcile(con)
    ld.build_reports(con)
    yield con
    con.close()


def rows_only_in_one(con, left: str, right: str) -> int:
    return con.execute(f"SELECT count(*) FROM (({left} EXCEPT {right}) UNION ALL ({right} EXCEPT {left}))").fetchone()[0]


def test_daily_revenue_by_zone_and_store_matches_etl(marts_and_etl):
    con = marts_and_etl
    columns = "order_date, zone, store_id, store_name, delivered_orders, net_revenue"
    assert rows_only_in_one(con, f"SELECT {columns} FROM mart_daily_store_kpis",
                            f"SELECT {columns} FROM etl_daily_zone_revenue") == 0
    assert con.execute("SELECT count(*) FROM mart_daily_store_kpis").fetchone()[0] == 459


def test_category_margin_matches_etl(marts_and_etl):
    assert rows_only_in_one(marts_and_etl, "SELECT * FROM mart_category_margin",
                            "SELECT * FROM etl_category_margin") == 0


def test_payment_reconciliation_matches_etl(marts_and_etl):
    con = marts_and_etl
    assert rows_only_in_one(con, "SELECT * FROM mart_payment_reconciliation",
                            "SELECT * FROM etl_payment_reconciliation") == 0
    assert con.execute("SELECT count(*), round(sum(amount_at_risk), 2) FROM mart_payment_reconciliation"
                       ).fetchone() == (433, 276928.5)


def test_mart_totals_agree_with_each_other_and_with_etl(marts_and_etl):
    con = marts_and_etl

    def one(sql):
        return con.execute(sql).fetchone()

    etl_revenue, etl_margin = one("""SELECT round(sum(net_revenue), 2), round(sum(gross_margin), 2)
                                     FROM etl_fact_orders WHERE order_status = 'DELIVERED'""")
    assert one("SELECT round(sum(net_revenue), 2), round(sum(gross_margin), 2) FROM mart_daily_store_kpis") == (
        etl_revenue, etl_margin) == (1128410.45, 277758.4)
    assert one("SELECT round(sum(revenue), 2) FROM mart_category_margin")[0] == etl_revenue
    assert one("SELECT round(sum(net_revenue), 2) FROM mart_payment_mode_revenue")[0] == etl_revenue
    assert one("SELECT round(sum(total_spend), 2) FROM mart_top_customers")[0] == etl_revenue
    assert one("SELECT round(sum(amount_not_reconciled), 2) FROM mart_payment_mode_revenue")[0] == 276928.5
    sla = one("SELECT sum(delivered_orders), sum(orders_within_15_min) FROM mart_delivery_sla")
    etl_sla = one("""SELECT count(*), count(*) FILTER (WHERE within_15_min) FROM etl_fact_orders
                     WHERE order_status = 'DELIVERED'""")
    assert sla == etl_sla


def test_top_customers_show_no_personal_data(marts_and_etl):
    columns = {row[0] for row in marts_and_etl.execute("DESCRIBE mart_top_customers").fetchall()}
    assert not {"full_name", "email", "phone", "email_masked", "phone_masked"} & columns
    top = marts_and_etl.execute("SELECT spend_rank, name_masked FROM mart_top_customers WHERE spend_rank <= 10").fetchall()
    assert [r[0] for r in top] == list(range(1, 11))
    assert all(r[1].endswith("***") for r in top)


def test_marts_are_rebuilt_not_appended(marts_and_etl):
    con = marts_and_etl
    before = {m: con.execute(f"SELECT count(*) FROM {m}").fetchone()[0] for m in MARTS}
    run_marts(con)
    assert {m: con.execute(f"SELECT count(*) FROM {m}").fetchone()[0] for m in MARTS} == before


# ── Business questions (Part B.4) ─────────────────────────────────────────────────────
@pytest.mark.parametrize("question", ["q1", "q2", "q3", "q4", "q5"])
def test_business_question_sql_gives_the_same_answer_in_both_pipelines(marts_and_etl, question):
    """analysis/sql/<q>_etl.sql and <q>_elt.sql must return identical rows (Q6 also needs the quarantine
    files and pipeline_runs of a real run, so it is checked by analysis/business_questions.py)."""
    folder = Path(__file__).resolve().parent.parent / "analysis" / "sql"
    if not folder.exists():   # the running Airflow containers only mount dags/, include/, plugins/ and tests/
        pytest.skip("analysis/sql is not available here (run locally or with `astro dev pytest`)")
    etl = marts_and_etl.execute((folder / f"{question}_etl.sql").read_text()).df()
    elt = marts_and_etl.execute((folder / f"{question}_elt.sql").read_text()).df()
    assert len(etl) > 0
    pd.testing.assert_frame_equal(etl, elt, check_dtype=False, check_exact=False, atol=0.005)

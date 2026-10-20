"""Unit tests for the ELT pipeline's SQL (no Airflow needed): the SQL files run on a temporary DuckDB.

Run locally from the project root:   pytest tests/test_nammamart_elt.py -v
Run inside Astro (Docker):           astro dev pytest tests/test_nammamart_elt.py
"""
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

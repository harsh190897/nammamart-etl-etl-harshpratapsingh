"""
dags/nammamart_elt_harshpratapsingh.py
--------------------------------------
NammaMart ELT pipeline (Part B): Extract -> Load raw -> Transform in the warehouse with SQL.
Every source row is loaded into the raw tables exactly as received; cleaning happens later, in SQL.

Graph so far (B.1):
  extract_customers --+
  extract_products  --+
  extract_stores    --+--> load_raw --> load_raw_increments
  extract_orders    --+
  extract_payments  --+

Helper code:
  include/nammamart_common.py     paths, constants, DuckDB connection, audit tables (shared with the ETL DAG)
  include/nammamart_elt.py        snapshot + header check, run .sql files with run parameters
  include/sql/raw/*.sql           raw layer SQL
"""
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, get_current_context, task

# Make include/ importable (same idea as meridian_pipeline.py, but works from any project path)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))

DAG_ID = "nammamart_elt_harshpratapsingh"

RAW_TABLES = ["raw_customers", "raw_products", "raw_stores", "raw_orders", "raw_payments"]


def _assets(*tables: str) -> list:
    return [Asset(name=t, uri=f"duckdb://warehouse_harshpratapsingh/{t}", group="nammamart_elt") for t in tables]


default_args = {
    "owner": "harshpratapsingh",
    "retries": 2,                                 # try a failed task 2 more times...
    "retry_delay": timedelta(minutes=1),          # ...waiting 1 minute between attempts
    "execution_timeout": timedelta(minutes=10),   # kill a task that hangs longer than this
}


def _run_id() -> str:
    return get_current_context()["run_id"]


def _extract(entity: str) -> dict:
    from nammamart_elt import snapshot_source_files
    return snapshot_source_files(entity, _run_id())


@dag(
    dag_id=DAG_ID,
    description="NammaMart ELT: raw -> staging -> marts in DuckDB SQL, ready by 9:00 AM IST",
    schedule="0 9 * * 1-6",                                       # 09:00, Monday to Saturday
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),  # timezone-aware: the cron runs in IST
    catchup=False,                                                 # do not back-fill missed past days
    max_active_runs=1,                                             # never two runs writing at the same time
    dagrun_timeout=timedelta(minutes=25),                          # must finish well before the 9:30 review
    default_args=default_args,
    tags=["nammamart", "elt", "harshpratapsingh"],
    doc_md=__doc__,
)
def nammamart_elt_harshpratapsingh():

    # ── 1. EXTRACT: copy today's files into the run folder (no parsing, no cleaning) ─────────
    @task
    def extract_customers():
        return _extract("customers")

    @task
    def extract_products():
        return _extract("products")

    @task
    def extract_stores():
        return _extract("stores")

    @task
    def extract_orders():
        return _extract("orders")

    @task
    def extract_payments():
        return _extract("payments")

    # ── 2. LOAD RAW: all five files, exactly as received, in one transaction ──────────────────
    @task(outlets=_assets(*RAW_TABLES))
    def load_raw(customers: dict, products: dict, stores: dict, orders: dict, payments: dict):
        import nammamart_common as nc
        from nammamart_elt import run_sql_files, table_counts

        run_id = _run_id()
        variables = {"run_id": run_id, "loaded_at": nc.utc_now()}
        for extracted in (customers, products, stores, orders, payments):
            daily = next(f for f in extracted["files"] if f["kind"] == "daily")
            variables[f"path_{extracted['entity']}"] = daily["path"]

        with nc.get_connection() as con:
            run_sql_files(con, ["raw/raw_tables.sql", "raw/load_raw.sql"], variables)
            this_run = table_counts(con, RAW_TABLES, run_id)
            total = table_counts(con, RAW_TABLES)
        for table in RAW_TABLES:
            print(f"[Load raw] {table}: {this_run[table]} rows loaded by this run, {total[table]} rows in total")
        return {"this_run": this_run, "total": total}

    # ── 3. LOAD RAW INCREMENTS: append next-day order files to raw_orders (Part B3) ─────────────
    @task(outlets=_assets("raw_orders"))
    def load_raw_increments(orders: dict, raw_result: dict):
        # raw_result is not used: it makes this task wait for load_raw (one writer at a time).
        import nammamart_common as nc
        from nammamart_elt import run_sql_files

        run_id = _run_id()
        increments = [f for f in orders["files"] if f["kind"] == "increment"]
        if not increments:
            print("[Load raw increments] No increment files today. Nothing to append.")
            return {"files": [], "rows": 0}

        loaded = []
        with nc.get_connection() as con:
            for f in increments:
                run_sql_files(con, ["raw/load_raw_increments.sql"],
                              {"run_id": run_id, "loaded_at": nc.utc_now(), "path_increment": f["path"]})
                rows = con.execute("SELECT count(*) FROM raw_orders WHERE _run_id = ? AND _source_file = ?",
                                   [run_id, f["file"]]).fetchone()[0]
                print(f"[Load raw increments] {f['file']}: {rows} rows appended to raw_orders")
                loaded.append({"file": f["file"], "rows": rows})
            total = con.execute("SELECT count(*) FROM raw_orders").fetchone()[0]
        print(f"[Load raw increments] raw_orders now has {total} rows in total")
        return {"files": loaded, "rows": sum(f["rows"] for f in loaded)}

    # ── Wiring: calling a task with another task's output creates the dependency ──────────
    customers_x = extract_customers()
    products_x = extract_products()
    stores_x = extract_stores()
    orders_x = extract_orders()
    payments_x = extract_payments()

    raw = load_raw(customers_x, products_x, stores_x, orders_x, payments_x)
    raw_increments = load_raw_increments(orders_x, raw)


nammamart_elt_harshpratapsingh()

"""
dags/nammamart_elt_harshpratapsingh.py
--------------------------------------
NammaMart ELT pipeline (Part B): Extract -> Load raw -> Transform in the warehouse with SQL.
Every source row is loaded into the raw tables exactly as received; cleaning happens later, in SQL.

Graph:
  extract_customers --+
  extract_products  --+
  extract_stores    --+--> load_raw --> load_raw_increments --> [staging] --> [marts] --> audit_run
  extract_orders    --+
  extract_payments  --+
  [staging] = stg_stores -> stg_products -> stg_customers -> stg_orders -> stg_payments
              (one after another: DuckDB allows one writer, and orders are checked against the staged masters)
  [marts]   = mart_daily_store_kpis -> mart_category_margin -> mart_delivery_sla
              -> mart_payment_reconciliation -> mart_payment_mode_revenue -> mart_top_customers
  audit_run runs even if something failed and records the run in pipeline_runs.

Helper code:
  include/nammamart_common.py     paths, constants, DuckDB connection, audit tables (shared with the ETL DAG)
  include/nammamart_elt.py        snapshot + header check, run .sql files with run parameters
  include/sql/raw/*.sql           raw layer SQL
  include/sql/common/*.sql        SQL macros (cleaning, masking) and the data quality rule catalog
  include/sql/staging/*.sql       staging layer SQL: types, standardisation, dedupe, masking, quality rules
  include/sql/marts/*.sql         mart layer SQL: business-ready tables for the six business questions
"""
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, get_current_context, task, task_group

# Make include/ importable (same idea as meridian_pipeline.py, but works from any project path)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))

DAG_ID = "nammamart_elt_harshpratapsingh"

RAW_TABLES = ["raw_customers", "raw_products", "raw_stores", "raw_orders", "raw_payments"]
MART_TABLES = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
               "mart_payment_reconciliation", "mart_payment_mode_revenue", "mart_top_customers"]


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


def _run_staging(entity: str) -> dict:
    """Run include/sql/staging/stg_<entity>.sql on this run's raw batch and log what happened."""
    import nammamart_common as nc
    from nammamart_elt import run_sql_files, staging_variables

    run_id = _run_id()
    table = f"stg_{entity}"
    with nc.get_connection() as con:
        nc.init_audit_tables(con)                      # dq_results is shared with the ETL DAG
        run_sql_files(con, ["common/macros.sql", "common/dq_setup.sql", f"staging/{table}.sql"],
                      staging_variables(run_id, DAG_ID))
        status = dict(con.execute(f"SELECT dq_status, count(*) FROM {table} GROUP BY 1").fetchall())
        rejected = dict(con.execute("""
            SELECT source_file, count(*) FROM stg_rejected_rows
            WHERE _run_id = ? AND source_file IN (SELECT DISTINCT _source_file FROM chk)
            GROUP BY 1 ORDER BY 1""", [run_id]).fetchall())
        merge = None
        if entity == "orders":
            row = con.execute("SELECT rows_in_batch, inserted, updated FROM merge_counts").fetchone()
            merge = {"rows_in_batch": row[0], "inserted": row[1], "updated": row[2],
                     "unchanged": row[0] - row[1] - row[2]}

    print(f"[Staging] {table}: {status.get('VALID', 0)} VALID, {status.get('REJECTED', 0)} REJECTED")
    for file_name, rows in rejected.items():
        print(f"[Staging] {file_name}: {rows} rows rejected this run (see stg_rejected_rows)")
    if merge:
        print(f"[Staging] MERGE into stg_orders: {merge['inserted']} inserted, {merge['updated']} updated, "
              f"{merge['unchanged']} unchanged")
    return {"table": table, "valid": status.get("VALID", 0), "rejected": status.get("REJECTED", 0),
            "rejected_rows": rejected, "merge": merge}


def _run_mart(table: str) -> dict:
    """Rebuild one mart from the VALID staging rows (include/sql/marts/<table>.sql) and print it."""
    import nammamart_common as nc
    from nammamart_elt import run_sql_files, staging_variables

    with nc.get_connection() as con:
        run_sql_files(con, [f"marts/{table}.sql"], staging_variables(_run_id(), DAG_ID))
        rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        preview = con.execute(f"SELECT * FROM {table} LIMIT 10").df()
    print(f"[Mart] {table}: {rows} rows. First rows:\n{preview.to_string(index=False)}")
    return {"table": table, "rows": int(rows)}


def _count(con, sql: str, params: list) -> int:
    """Row count for the audit; 0 if the table does not exist yet (e.g. the very first run failed early)."""
    try:
        return int(con.execute(sql, params).fetchone()[0])
    except Exception:
        return 0


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

    # ── 4. STAGING: SQL only, one task per table, one after another ──────────────────────────
    @task(outlets=_assets("stg_stores", "stg_rejected_rows"))
    def stg_stores():
        return _run_staging("stores")

    @task(outlets=_assets("stg_products", "stg_rejected_rows"))
    def stg_products():
        return _run_staging("products")

    @task(outlets=_assets("stg_customers", "stg_rejected_rows"))
    def stg_customers():
        return _run_staging("customers")

    @task(outlets=_assets("stg_orders", "stg_rejected_rows"))
    def stg_orders():
        return _run_staging("orders")

    @task(outlets=_assets("stg_payments", "stg_rejected_rows"))
    def stg_payments():
        return _run_staging("payments")

    @task_group(group_id="staging")
    def staging():
        # The group returns nothing, so calling staging() gives back the whole group:
        # "upstream >> group" then makes the FIRST task (stg_stores) wait, not only the last one.
        stores, products, customers = stg_stores(), stg_products(), stg_customers()
        orders, payments = stg_orders(), stg_payments()
        stores >> products >> customers >> orders >> payments

    # ── 5. MARTS: SQL only, business-ready tables, rebuilt on every run ──────────────────────
    @task(outlets=_assets("mart_daily_store_kpis"))
    def mart_daily_store_kpis():
        return _run_mart("mart_daily_store_kpis")

    @task(outlets=_assets("mart_category_margin"))
    def mart_category_margin():
        return _run_mart("mart_category_margin")

    @task(outlets=_assets("mart_delivery_sla"))
    def mart_delivery_sla():
        return _run_mart("mart_delivery_sla")

    @task(outlets=_assets("mart_payment_reconciliation"))
    def mart_payment_reconciliation():
        return _run_mart("mart_payment_reconciliation")

    @task(outlets=_assets("mart_payment_mode_revenue"))
    def mart_payment_mode_revenue():
        return _run_mart("mart_payment_mode_revenue")

    @task(outlets=_assets("mart_top_customers"))
    def mart_top_customers():
        return _run_mart("mart_top_customers")

    @task_group(group_id="marts")
    def marts(after):
        # `after` is the staging group: every mart waits for the whole staging layer.
        tasks = [mart_daily_store_kpis(), mart_category_margin(), mart_delivery_sla(),
                 mart_payment_reconciliation(), mart_payment_mode_revenue(), mart_top_customers()]
        after >> tasks[0]
        for before, nxt in zip(tasks, tasks[1:]):
            before >> nxt
        return tasks[-1]                               # the audit uses the last mart's result

    # ── 6. RUN AUDIT: always runs, records the outcome in pipeline_runs ───────────────────
    @task(trigger_rule="all_done", retries=0)
    def audit_run(extracted: list, last_mart):
        # With trigger_rule="all_done" this task runs even when an upstream task failed.
        # A failed upstream task has no return value, so its argument arrives as None.
        import nammamart_common as nc

        ctx = get_current_context()
        run_id = ctx["run_id"]
        rows_extracted = sum(f["rows"] for e in extracted if e for f in e["files"])
        status = "SUCCESS" if last_mart is not None else "FAILED"
        with nc.get_connection() as con:
            # ELT loads every row it receives into raw; rejected rows are flagged in staging, not removed
            rows_loaded = sum(_count(con, f"SELECT count(*) FROM {t} WHERE _run_id = ?", [run_id])
                              for t in RAW_TABLES)
            rows_quarantined = _count(con, "SELECT count(*) FROM stg_rejected_rows WHERE _run_id = ?", [run_id])
            nc.write_pipeline_run(con, run_id, DAG_ID, ctx["dag_run"].start_date, nc.utc_now(),
                                  rows_extracted, rows_loaded, rows_quarantined, status)
        removed = nc.cleanup_old_run_dirs()
        print(f"[Audit] {status}: extracted={rows_extracted} loaded into raw={rows_loaded} "
              f"rejected in staging={rows_quarantined} (old run folders removed: {removed})")
        if status == "FAILED":
            # Fail this task too, otherwise the DAG run would show green even though a step failed.
            raise RuntimeError("Pipeline failed - see the red task(s) and the pipeline_runs table")
        return status

    # ── Wiring: calling a task with another task's output creates the dependency ──────────
    customers_x = extract_customers()
    products_x = extract_products()
    stores_x = extract_stores()
    orders_x = extract_orders()
    payments_x = extract_payments()

    raw = load_raw(customers_x, products_x, stores_x, orders_x, payments_x)
    raw_increments = load_raw_increments(orders_x, raw)
    staged = staging()
    raw_increments >> staged
    last_mart = marts(staged)
    audit_run([customers_x, products_x, stores_x, orders_x, payments_x], last_mart)


nammamart_elt_harshpratapsingh()

"""
dags/nammamart_etl_harshpratapsingh.py
--------------------------------------
NammaMart ETL pipeline (Part A): Extract -> Validate -> Transform -> Load.
Only rows that pass every Critical check reach the finance tables; failed rows are quarantined with a reason.

Graph:
  extract_customers -> validate_customers --+
  extract_products  -> validate_products  --+--> validate_orders --+--> quarantine_and_log_dq --+
  extract_stores    -> validate_stores    --+          ^           |                            +--> load
  extract_orders    -----------------------------------+           +--> transform --------------+
  extract_payments  -> validate_payments (also uses extract_orders) -+
  load -> reconcile -> report -> audit_run (runs even if something failed)

`load`, `reconcile` and `report` publish their warehouse tables as Airflow Assets (see the Assets page).

Helper code lives in include/:
  nammamart_common.py           paths, constants, DuckDB connection, masking, audit tables
  nammamart_etl_checks.py       data quality rules (pandas)
  nammamart_etl_transform.py    standardise, mask, join, derive (pandas)
  nammamart_etl_load.py         star schema load, reconciliation, reports (DuckDB SQL)
"""
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, get_current_context, task

# Make include/ importable (same idea as meridian_pipeline.py, but works from any project path)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))

DAG_ID = "nammamart_etl_harshpratapsingh"

# Warehouse tables written by the `load` task. Declaring them as Assets makes Airflow record an
# asset event every time `load` succeeds, and shows the tables and their producer on the Assets page.
STAR_SCHEMA_TABLES = ["etl_dim_customer", "etl_dim_product", "etl_dim_store", "etl_fact_orders", "etl_fact_payments"]


def _assets(*tables: str) -> list:
    return [Asset(name=t, uri=f"duckdb://warehouse_harshpratapsingh/{t}", group="nammamart_etl") for t in tables]


STAR_SCHEMA_ASSETS = _assets(*STAR_SCHEMA_TABLES)
RECONCILIATION_ASSETS = _assets("etl_payment_reconciliation")
REPORT_ASSETS = _assets("etl_daily_zone_revenue", "etl_category_margin")

default_args = {
    "owner": "harshpratapsingh",
    "retries": 2,                                 # try a failed task 2 more times...
    "retry_delay": timedelta(minutes=1),          # ...waiting 1 minute between attempts
    "execution_timeout": timedelta(minutes=10),   # kill a task that hangs longer than this
}


# ─────────────────────────── helpers used by the tasks ───────────────────────────
def _run_id() -> str:
    return get_current_context()["run_id"]


def _extract(entity: str) -> dict:
    """Copy today's file(s) for one entity into this run's folder and check the header."""
    import shutil

    import nammamart_common as nc

    file_names = [nc.SOURCE_FILES[entity]]
    if entity == "orders":   # next-day files are picked up automatically, oldest first
        file_names += sorted(p.name for p in nc.DATA_DIR.glob(nc.INCREMENT_GLOB))

    files = []
    for name in file_names:
        snapshot = nc.run_dir(_run_id(), "extract") / name
        shutil.copyfile(nc.DATA_DIR / name, snapshot)   # FileNotFoundError -> task fails -> Airflow retries
        df = nc.read_text_csv(snapshot)
        nc.check_columns(df, nc.EXPECTED_COLUMNS[entity], name)
        print(f"[Extract] {name}: {len(df)} rows -> {snapshot}")
        files.append({"file": name, "path": str(snapshot), "rows": len(df)})
    return {"entity": entity, "files": files}


def _save_validation(result) -> dict:
    """Write passed / failed rows to this run's folder and return a small summary for XCom."""
    import nammamart_common as nc

    folder = nc.run_dir(_run_id(), "validate")
    stem = Path(result.file).stem
    passed_path, failed_path = folder / f"{stem}_passed.csv", folder / f"{stem}_failed.csv"
    result.passed.to_csv(passed_path, index=False)
    result.failed.to_csv(failed_path, index=False)
    return {"file": result.file, "passed_path": str(passed_path), "failed_path": str(failed_path),
            "passed": len(result.passed), "failed": len(result.failed), "checks": result.checks}


def _validate_master(extracted: dict, key: str, rules) -> dict:
    import nammamart_common as nc

    source = extracted["files"][0]
    df = nc.read_text_csv(source["path"])
    saved = _save_validation(rules(df, source["file"]))
    valid = nc.read_text_csv(saved["passed_path"])
    return {"entity": extracted["entity"], "files": [saved],
            "all_ids": sorted(set(df[key].str.strip())), "valid_ids": sorted(set(valid[key].str.strip()))}


def _passed_rows(validated: dict):
    import nammamart_common as nc
    return [nc.read_text_csv(f["passed_path"]) for f in validated["files"]]


@dag(
    dag_id=DAG_ID,
    description="NammaMart ETL: validated, reconciled finance tables by 9:00 AM IST",
    schedule="0 9 * * 1-6",                                       # 09:00, Monday to Saturday
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),  # timezone-aware: the cron runs in IST
    catchup=False,                                                 # do not back-fill missed past days
    max_active_runs=1,                                             # never two runs writing at the same time
    dagrun_timeout=timedelta(minutes=25),                          # must finish well before the 9:30 review
    default_args=default_args,
    tags=["nammamart", "etl", "harshpratapsingh"],
    doc_md=__doc__,
)
def nammamart_etl_harshpratapsingh():

    # ── 1. EXTRACT: five independent tasks, so Airflow runs them in parallel ──────────────
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

    # ── 2. VALIDATE: split every file into passed and failed rows ─────────────────────────
    @task
    def validate_customers(extracted: dict):
        import nammamart_common as nc
        from nammamart_etl_checks import validate_customers as rules
        return _validate_master(extracted, "customer_id", lambda df, f: rules(df, nc.local_now(), f))

    @task
    def validate_products(extracted: dict):
        from nammamart_etl_checks import validate_products as rules
        return _validate_master(extracted, "product_id", rules)

    @task
    def validate_stores(extracted: dict):
        from nammamart_etl_checks import validate_stores as rules
        return _validate_master(extracted, "store_id", rules)

    @task
    def validate_orders(extracted: dict, customers: dict, products: dict, stores: dict):
        import nammamart_common as nc
        from nammamart_etl_checks import validate_orders as rules

        refs = {"customer_id": (customers["all_ids"], customers["valid_ids"]),
                "product_id": (products["all_ids"], products["valid_ids"]),
                "store_id": (stores["all_ids"], stores["valid_ids"])}
        as_of = nc.local_now()
        files = [_save_validation(rules(nc.read_text_csv(f["path"]), f["file"], refs, as_of))
                 for f in extracted["files"]]
        return {"entity": "orders", "files": files}

    @task
    def validate_payments(extracted: dict, orders_extracted: dict):
        import nammamart_common as nc
        from nammamart_etl_checks import validate_payments as rules

        known_orders = set()
        for f in orders_extracted["files"]:
            known_orders |= set(nc.read_text_csv(f["path"])["order_id"].str.strip())
        source = extracted["files"][0]
        result = rules(nc.read_text_csv(source["path"]), known_orders, nc.local_now(), source["file"])
        return {"entity": "payments", "files": [_save_validation(result)]}

    # ── 3. QUARANTINE + DQ RESULTS: failed rows to CSV, every check to dq_results ──────────
    @task
    def quarantine_and_log_dq(customers: dict, products: dict, stores: dict, orders: dict, payments: dict):
        import shutil

        import nammamart_common as nc
        from nammamart_etl_checks import volume_check

        run_id = _run_id()
        checks, quarantined, files_written = [], {}, []
        for validated in (customers, products, stores, orders, payments):
            for f in validated["files"]:
                checks += f["checks"]
                quarantined[f["file"]] = f["failed"]
                if f["failed"]:
                    target = nc.quarantine_path(f["file"], run_id)
                    shutil.copyfile(f["failed_path"], target)
                    files_written.append(str(target))
                    print(f"[Quarantine] {f['failed']} rows from {f['file']} -> {target}")

        with nc.get_connection() as con:
            nc.init_audit_tables(con)
            previous = con.execute("""
                SELECT rows_checked FROM dq_results
                WHERE dag_id = ? AND check_name = 'orders.order_id_unique' AND run_id <> ?
                ORDER BY checked_at DESC LIMIT 1""", [DAG_ID, run_id]).fetchone()
            current = next(f for f in orders["files"] if f["file"] == nc.SOURCE_FILES["orders"])
            checks.append(volume_check("orders.volume_vs_previous_run", current["passed"] + current["failed"],
                                       previous[0] if previous else None))
            written = nc.write_dq_results(con, run_id, DAG_ID, checks)

        failed_checks = sum(1 for c in checks if c["rows_failed"])
        print(f"[DQ] {written} checks written to dq_results ({failed_checks} with failures)")
        return {"quarantined": sum(quarantined.values()), "by_file": quarantined, "files": files_written}

    # ── 4. TRANSFORM (pandas): standardise, dedupe, mask PII, join, derive ─────────────────
    @task
    def transform(customers: dict, products: dict, stores: dict, orders: dict, payments: dict):
        import nammamart_common as nc
        import nammamart_etl_transform as tf

        dim_product = tf.build_dim_product(_passed_rows(products)[0])
        tables = {
            "etl_dim_customer": tf.build_dim_customer(_passed_rows(customers)[0]),
            "etl_dim_product": dim_product,
            "etl_dim_store": tf.build_dim_store(_passed_rows(stores)[0]),
            "etl_fact_orders": tf.build_fact_orders(_passed_rows(orders), dim_product),
            "etl_fact_payments": tf.build_payments(_passed_rows(payments)[0]),
        }
        folder = nc.run_dir(_run_id(), "transform")
        paths = {}
        for name, df in tables.items():
            paths[name] = str(nc.write_parquet(df, folder / f"{name}.parquet"))
            print(f"[Transform] {name}: {len(df)} rows")
        return paths

    # ── 5. LOAD into DuckDB (idempotent) ──────────────────────────────────────────────────
    @task(outlets=STAR_SCHEMA_ASSETS)
    def load(parquet_paths: dict, quarantine_result: dict):
        # quarantine_result is not used: it makes load wait until dq_results is written,
        # so only one task writes to the warehouse file at a time.
        import nammamart_common as nc
        from nammamart_etl_load import load_star_schema

        with nc.get_connection() as con:
            counts = load_star_schema(con, parquet_paths, _run_id())
        print(f"[Load] fact orders: {counts['fact_inserted']} inserted, {counts['fact_updated']} updated, "
              f"{counts['fact_unchanged']} unchanged -> {counts['etl_fact_orders']} rows in etl_fact_orders")
        print(f"[Load] table row counts: {counts}")
        return {k: int(v) for k, v in counts.items()}

    # ── 6. RECONCILE orders with payments ─────────────────────────────────────────────────
    @task(outlets=RECONCILIATION_ASSETS)
    def reconcile(load_result: dict):
        import nammamart_common as nc
        from nammamart_etl_load import reconcile as run_reconciliation

        with nc.get_connection() as con:
            return run_reconciliation(con)

    # ── 7. REPORT tables, printed in the task log ─────────────────────────────────────────
    @task(outlets=REPORT_ASSETS)
    def report(reconcile_result: dict):
        import nammamart_common as nc
        from nammamart_etl_load import build_reports

        with nc.get_connection() as con:
            return build_reports(con)

    # ── 8. RUN AUDIT: always runs, records the outcome in pipeline_runs ───────────────────
    @task(trigger_rule="all_done", retries=0)
    def audit_run(extracted: list, quarantine_result, load_result, report_result):
        # With trigger_rule="all_done" this task runs even when an upstream task failed.
        # A failed upstream task has no return value, so its argument arrives as None.
        import nammamart_common as nc

        ctx = get_current_context()
        rows_extracted = sum(f["rows"] for e in extracted if e for f in e["files"])
        rows_quarantined = quarantine_result["quarantined"] if quarantine_result else 0
        rows_loaded = 0
        if load_result:
            rows_loaded = sum(load_result[t] for t in ("etl_dim_customer", "etl_dim_product", "etl_dim_store",
                                                       "etl_fact_payments", "fact_rows_in_batch"))
        status = "SUCCESS" if report_result is not None else "FAILED"

        with nc.get_connection() as con:
            nc.write_pipeline_run(con, ctx["run_id"], DAG_ID, ctx["dag_run"].start_date, nc.utc_now(),
                                  rows_extracted, rows_loaded, rows_quarantined, status)
        removed = nc.cleanup_old_run_dirs()
        print(f"[Audit] {status}: extracted={rows_extracted} loaded={rows_loaded} "
              f"quarantined={rows_quarantined} (old run folders removed: {removed})")
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

    customers_v = validate_customers(customers_x)
    products_v = validate_products(products_x)
    stores_v = validate_stores(stores_x)
    orders_v = validate_orders(orders_x, customers_v, products_v, stores_v)
    payments_v = validate_payments(payments_x, orders_x)

    quarantined = quarantine_and_log_dq(customers_v, products_v, stores_v, orders_v, payments_v)
    transformed = transform(customers_v, products_v, stores_v, orders_v, payments_v)
    loaded = load(transformed, quarantined)
    reported = report(reconcile(loaded))
    audit_run([customers_x, products_x, stores_x, orders_x, payments_x], quarantined, loaded, reported)


nammamart_etl_harshpratapsingh()

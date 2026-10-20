"""
dags/nammamart_etl_harshpratapsingh.py
--------------------------------------
NammaMart ETL pipeline (Part A): Extract -> Validate -> Transform -> Load.
Only rows that pass every Critical check reach the finance tables; failed rows are quarantined with a reason.

Graph so far (A.1 + A.2):
  extract_customers -> validate_customers --+
  extract_products  -> validate_products  --+--> validate_orders --+
  extract_stores    -> validate_stores    --+          ^           +--> quarantine_and_log_dq
  extract_orders    -----------------------------------+           |
  extract_payments  -> validate_payments (also uses extract_orders) -+

Helper code lives in include/:
  nammamart_common.py           paths, constants, DuckDB connection, masking, audit tables
  nammamart_etl_checks.py       data quality rules (pandas)
"""
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task

# Make include/ importable (same idea as meridian_pipeline.py, but works from any project path)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))

DAG_ID = "nammamart_etl_harshpratapsingh"

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


nammamart_etl_harshpratapsingh()

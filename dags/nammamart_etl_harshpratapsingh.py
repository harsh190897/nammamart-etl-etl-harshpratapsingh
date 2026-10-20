"""
dags/nammamart_etl_harshpratapsingh.py
--------------------------------------
NammaMart ETL pipeline (Part A): Extract -> Validate -> Transform -> Load.
Only rows that pass every Critical check reach the finance tables; failed rows are quarantined with a reason.

Step A.1: DAG skeleton + the five Extract tasks.
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

    # ── Wiring: calling a task with another task's output creates the dependency ──────────
    customers_x = extract_customers()
    products_x = extract_products()
    stores_x = extract_stores()
    orders_x = extract_orders()
    payments_x = extract_payments()


nammamart_etl_harshpratapsingh()

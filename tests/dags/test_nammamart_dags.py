"""Structure tests for the NammaMart DAGs: task order, schedule and settings.
These need Airflow, so they run inside the Astro container:   astro dev pytest tests/dags
"""
import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(include_examples=False)


def upstream(dag, task_id):
    return set(dag.get_task(task_id).upstream_task_ids)


@pytest.mark.parametrize("dag_id", ["nammamart_etl_harshpratapsingh", "nammamart_elt_harshpratapsingh"])
def test_schedule_is_9am_ist_monday_to_saturday(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, dagbag.import_errors
    assert dag.timetable.expression == "0 9 * * 1-6"
    assert str(dag.timetable.timezone.name) == "Asia/Kolkata"                # the cron is read in IST
    assert dag.catchup is False and dag.max_active_runs == 1


def test_elt_task_order(dagbag):
    dag = dagbag.get_dag("nammamart_elt_harshpratapsingh")
    extracts = {f"extract_{e}" for e in ("customers", "products", "stores", "orders", "payments")}
    assert all(not upstream(dag, t) for t in extracts)                       # extracts run in parallel
    assert upstream(dag, "load_raw") == extracts
    assert upstream(dag, "load_raw_increments") == {"extract_orders", "load_raw"}
    chain = ["load_raw_increments", "staging.stg_stores", "staging.stg_products", "staging.stg_customers",
             "staging.stg_orders", "staging.stg_payments"]
    for before, after in zip(chain, chain[1:]):                              # one writer at a time, in order
        assert upstream(dag, after) == {before}, f"{after} must wait for {before}"


def test_etl_task_order(dagbag):
    dag = dagbag.get_dag("nammamart_etl_harshpratapsingh")
    assert upstream(dag, "validate_orders") == {"extract_orders", "validate_customers", "validate_products",
                                                "validate_stores"}
    assert upstream(dag, "load") == {"transform", "quarantine_and_log_dq"}
    assert upstream(dag, "audit_run") >= {"load", "report", "quarantine_and_log_dq"}
    assert dag.get_task("audit_run").trigger_rule == "all_done"


def test_elt_marts_and_audit_order(dagbag):
    dag = dagbag.get_dag("nammamart_elt_harshpratapsingh")
    chain = ["staging.stg_payments", "marts.mart_daily_store_kpis", "marts.mart_category_margin",
             "marts.mart_delivery_sla", "marts.mart_payment_reconciliation", "marts.mart_payment_mode_revenue",
             "marts.mart_top_customers"]
    for before, after in zip(chain, chain[1:]):
        assert upstream(dag, after) == {before}, f"{after} must wait for {before}"
    audit = dag.get_task("audit_run")
    assert audit.trigger_rule == "all_done"
    assert "marts.mart_top_customers" in audit.upstream_task_ids
    assert {f"extract_{e}" for e in ("customers", "products", "stores", "orders", "payments")} <= audit.upstream_task_ids

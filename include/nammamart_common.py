"""
include/nammamart_common.py
---------------------------
Shared settings and helpers for the NammaMart ETL and ELT DAGs.

Used by:
  dags/nammamart_etl_harshpratapsingh.py
  dags/nammamart_elt_harshpratapsingh.py

Contents:
  1. Names and paths
  2. Business constants (allowed values, limits)
  3. Small utilities (UTC time, safe run_id, quarantine path)
  4. DuckDB connection (closes itself, waits if the file is locked)
  5. PII masking (email, phone, name)
  6. Audit tables: dq_results and pipeline_runs
"""
import re
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

# ─────────────────────────── 1. Names and paths ───────────────────────────
NAME = "harshpratapsingh"
ETL_DAG_ID = f"nammamart_etl_{NAME}"
ELT_DAG_ID = f"nammamart_elt_{NAME}"

# This file lives in include/, so every path is built from its own location.
# In Docker that is /usr/local/airflow/include; on your Mac it is <project>/include.
INCLUDE_DIR = Path(__file__).resolve().parent
DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
SQL_DIR = INCLUDE_DIR / "sql"
WAREHOUSE_DB = INCLUDE_DIR / f"warehouse_{NAME}.duckdb"
QUARANTINE_DIR = INCLUDE_DIR / "quarantine" / NAME
TMP_DIR = INCLUDE_DIR / "tmp" / NAME

SOURCE_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}
INCREMENT_FILE = "orders_2026-10-01_increment.csv"

# ─────────────────────────── 2. Business constants ───────────────────────────
# One place for every rule value, so a rule change (e.g. the 120-minute limit) is a one-line edit.
VALID_ORDER_STATUS = ("DELIVERED", "CANCELLED", "RETURNED")
REVENUE_STATUS = "DELIVERED"
VALID_PAYMENT_MODES = ("UPI", "Card", "Cash on Delivery", "Wallet")
# Keys are UPPER-cased and trimmed before lookup, so 'upi', 'UPI ' and 'Upi' all map to 'UPI'.
PAYMENT_MODE_MAP = {
    "UPI": "UPI",
    "CARD": "Card",
    "CASH ON DELIVERY": "Cash on Delivery",
    "COD": "Cash on Delivery",
    "WALLET": "Wallet",
}
LOYALTY_TIERS = ("Bronze", "Silver", "Gold", "Platinum")
MAX_DELIVERY_MINUTES = 120          # above this = impossible (Critical)
NORMAL_DELIVERY_RANGE = (8, 35)     # outside this = unusual (Warning)
SLA_MINUTES = 15                    # "within_15_min" KPI
AMOUNT_TOLERANCE = 0.01             # rupees allowed between recomputed and reported amount

DQ_DIMENSIONS = ("Completeness", "Uniqueness", "Validity", "Accuracy", "Consistency", "Timeliness")
DQ_SEVERITIES = ("Critical", "Warning")

# ─────────────────────────── 3. Small utilities ───────────────────────────
def utc_now() -> datetime:
    """Current time in UTC, without tzinfo. All audit timestamps are stored in UTC, like Airflow."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_utc_naive(value):
    """Convert a timezone-aware datetime (e.g. Airflow's dag_run.start_date) to naive UTC for DuckDB."""
    if value is None:
        return None
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def safe_run_id(run_id: str) -> str:
    """Airflow run_ids look like 'manual__2026-10-20T07:35:00.123+00:00'.
    ':' and '+' are awkward in file names, so replace anything unusual with '_'."""
    return re.sub(r"[^A-Za-z0-9_-]+", "_", run_id).strip("_")


def quarantine_path(file_name: str, run_id: str) -> Path:
    """include/quarantine/<name>/<file>_failed_<run_id>.csv  (creates the folder if needed)."""
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    return QUARANTINE_DIR / f"{Path(file_name).stem}_failed_{safe_run_id(run_id)}.csv"


# ─────────────────────────── 4. DuckDB connection ───────────────────────────
@contextmanager
def get_connection(db_path=WAREHOUSE_DB, retries: int = 10, wait_seconds: float = 3):
    """Open the warehouse, hand it to the caller, and ALWAYS close it afterwards.

    DuckDB allows only one process to write to a file at a time. If another task
    (or the other DAG) holds the lock, wait and retry instead of failing at once.

    Usage:
        with get_connection() as con:
            con.execute("SELECT 1")
    """
    con = None
    for attempt in range(1, retries + 1):
        try:
            con = duckdb.connect(str(db_path))
            break
        except duckdb.IOException as exc:
            if "lock" not in str(exc).lower() or attempt == retries:
                raise
            print(f"[DuckDB] Warehouse locked by another process; retry {attempt}/{retries} in {wait_seconds}s")
            time.sleep(wait_seconds)
    try:
        yield con
    finally:
        con.close()


# ─────────────────────────── 5. PII masking ───────────────────────────
def _is_blank(value) -> bool:
    return value is None or (not isinstance(value, str) and pd.isna(value)) or str(value).strip() == ""


def mask_email(email):
    """'pratap.singh@example.com' -> 'pr***@example.com'. Blank -> None. No '@' -> '***'."""
    if _is_blank(email):
        return None
    local, at, domain = str(email).strip().partition("@")
    if not at or not local or not domain:
        return "***"
    keep = 2 if len(local) > 2 else 1
    return f"{local[:keep]}***@{domain}"


def mask_phone(phone):
    """'9876543221' -> '98******21' (first 2 and last 2 digits kept). Blank -> None."""
    if _is_blank(phone):
        return None
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) < 6:
        return "*" * max(len(digits), 3)
    return digits[:2] + "*" * (len(digits) - 4) + digits[-2:]


def mask_name(name):
    """'Deepa Desai' -> 'D*** D***'. Blank -> None."""
    if _is_blank(name):
        return None
    return " ".join(part[0] + "***" for part in str(name).split())


def mask_customer_pii(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of a customers DataFrame with masked columns and the raw PII columns removed."""
    out = df.copy()
    if "email" in out:
        out["email_masked"] = out["email"].map(mask_email)
    if "phone" in out:
        out["phone_masked"] = out["phone"].map(mask_phone)
    if "full_name" in out:
        out["name_masked"] = out["full_name"].map(mask_name)
    return out.drop(columns=[c for c in ("email", "phone", "full_name") if c in out])


# ─────────────────────────── 6. Audit tables ───────────────────────────
DQ_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id       VARCHAR,
    dag_id       VARCHAR,
    check_name   VARCHAR,
    dimension    VARCHAR,
    severity     VARCHAR,
    rows_checked BIGINT,
    rows_failed  BIGINT,
    status       VARCHAR,     -- PASS / WARN / FAIL
    checked_at   TIMESTAMP    -- UTC
)"""

PIPELINE_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           VARCHAR,
    dag_id           VARCHAR,
    start_time       TIMESTAMP,   -- UTC
    end_time         TIMESTAMP,   -- UTC
    rows_extracted   BIGINT,
    rows_loaded      BIGINT,
    rows_quarantined BIGINT,
    status           VARCHAR      -- SUCCESS / FAILED
)"""


def init_audit_tables(con) -> None:
    """Create dq_results and pipeline_runs if they don't exist yet (safe to call every run)."""
    con.execute(DQ_RESULTS_DDL)
    con.execute(PIPELINE_RUNS_DDL)


def dq_status(severity: str, rows_failed: int) -> str:
    """PASS if nothing failed; otherwise FAIL for Critical checks and WARN for Warning checks."""
    if rows_failed == 0:
        return "PASS"
    return "FAIL" if severity == "Critical" else "WARN"


def write_dq_results(con, run_id: str, dag_id: str, results: list) -> int:
    """Save one row per quality check.

    `results` is a list of dicts with keys:
        check_name, dimension, severity, rows_checked, rows_failed

    Idempotent: rows for the same (run_id, dag_id, check_name) are replaced, so a task retry
    never creates duplicates.
    """
    init_audit_tables(con)
    checked_at = utc_now()
    rows = []
    for r in results:
        if r["dimension"] not in DQ_DIMENSIONS:
            raise ValueError(f"Unknown dimension '{r['dimension']}' in check '{r['check_name']}'")
        if r["severity"] not in DQ_SEVERITIES:
            raise ValueError(f"Unknown severity '{r['severity']}' in check '{r['check_name']}'")
        failed = int(r["rows_failed"])
        rows.append((run_id, dag_id, r["check_name"], r["dimension"], r["severity"],
                     int(r["rows_checked"]), failed, dq_status(r["severity"], failed), checked_at))
    if not rows:
        return 0

    con.execute("BEGIN TRANSACTION")
    try:
        con.executemany("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ? AND check_name = ?",
                        [(row[0], row[1], row[2]) for row in rows])
        con.executemany("INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return len(rows)


def write_pipeline_run(con, run_id: str, dag_id: str, start_time, end_time,
                       rows_extracted: int, rows_loaded: int, rows_quarantined: int, status: str) -> None:
    """Save one row per DAG run. Idempotent: re-running the audit task replaces the row."""
    init_audit_tables(con)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
        con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [run_id, dag_id, to_utc_naive(start_time), to_utc_naive(end_time),
                     int(rows_extracted), int(rows_loaded), int(rows_quarantined), status])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
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
import shutil
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
# Next-day order files follow this pattern; the ETL picks up every file that matches.
INCREMENT_GLOB = "orders_*_increment.csv"

# Header each source file must have. Extract fails loudly if a column is missing or renamed.
EXPECTED_COLUMNS = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price",
               "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date",
                  "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit",
                 "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
               "capacity_orders_per_day"],
}

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
VALID_PAYMENT_STATUS = ("SUCCESS", "FAILED", "REFUNDED")
VALID_ZONES = ("North", "South", "East", "West")
VALID_CHANNELS = ("App", "Web")
UNKNOWN = "Unknown"                 # shown when a Warning-level value is missing (e.g. a store's zone)
MAX_DELIVERY_MINUTES = 120          # above this = impossible (Critical)
NORMAL_DELIVERY_RANGE = (8, 35)     # outside this = unusual (Warning)
SLA_MINUTES = 15                    # "within_15_min" KPI
AMOUNT_TOLERANCE = 0.01             # rupees allowed between recomputed and reported amount
# Text that means "no value" (same list as the profiling scripts). Compared after TRIM + UPPER.
MISSING_VALUES = ("", "NULL", "NONE", "NAN", "N/A", "NA", "NIL", "-")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
LOCAL_TZ = "Asia/Kolkata"           # source timestamps are Bengaluru local time
FRESHNESS_MAX_AGE_HOURS = 24        # Warning if the newest order is older than this
VOLUME_DROP_WARN_PCT = 90           # Warning if orders volume drops by more than this vs the previous run
TMP_KEEP_DAYS = 3                   # run working folders (include/tmp/...) older than this are deleted

DQ_DIMENSIONS = ("Completeness", "Uniqueness", "Validity", "Accuracy", "Consistency", "Timeliness")
DQ_SEVERITIES = ("Critical", "Warning")

# ─────────────────────────── 3. Small utilities ───────────────────────────
def utc_now() -> datetime:
    """Current time in UTC, without tzinfo. All audit timestamps are stored in UTC, like Airflow."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def local_now() -> pd.Timestamp:
    """Current Bengaluru time, without tzinfo, to compare with source timestamps (which are local time)."""
    return pd.Timestamp.now(tz=LOCAL_TZ).tz_localize(None)


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


def run_dir(run_id: str, *parts: str) -> Path:
    """Working folder for one DAG run: include/tmp/<name>/<run_id>/<parts...> (created if needed).
    Tasks pass file paths to each other through XCom instead of passing whole DataFrames."""
    folder = TMP_DIR.joinpath(safe_run_id(run_id), *parts)
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def cleanup_old_run_dirs(keep_days: int = TMP_KEEP_DAYS) -> int:
    """Delete run folders older than keep_days. Recent ones are kept so a failed task can be
    cleared and re-run. They contain raw customer data, so they must not be kept for long."""
    if not TMP_DIR.exists():
        return 0
    cutoff = time.time() - keep_days * 86400
    old = [d for d in TMP_DIR.iterdir() if d.is_dir() and d.stat().st_mtime < cutoff]
    for folder in old:
        shutil.rmtree(folder)
    return len(old)


def read_text_csv(path) -> pd.DataFrame:
    """Read a CSV with every column as text, exactly as received (same rule as the profiling scripts)."""
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def is_missing(series: pd.Series) -> pd.Series:
    """True where the value is blank or a placeholder such as 'N/A' or 'NULL'."""
    return series.fillna("").astype(str).str.strip().str.upper().isin(MISSING_VALUES)


def check_columns(df: pd.DataFrame, expected: list, file_name: str) -> None:
    """Raise a clear error if the file's header is not what we expect (schema change)."""
    missing = [c for c in expected if c not in df.columns]
    extra = [c for c in df.columns if c not in expected]
    if missing or extra:
        raise ValueError(f"Schema change in {file_name}: missing columns {missing}, unexpected columns {extra}")


def _sql_literal(path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def write_parquet(df: pd.DataFrame, path) -> Path:
    """Save a typed DataFrame as Parquet using DuckDB (the Airflow image has no pyarrow)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.register("df_to_write", df)
        con.execute(f"COPY df_to_write TO {_sql_literal(path)} (FORMAT parquet)")
    finally:
        con.close()
    return path


def read_parquet(path) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        return con.execute(f"SELECT * FROM read_parquet({_sql_literal(path)})").df()
    finally:
        con.close()


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
"""
include/nammamart_elt.py
------------------------
Helpers for the NammaMart ELT DAG (Part B).

In ELT all cleaning happens in SQL inside the warehouse, so this module does not parse or clean data:
  snapshot_source_files  copy today's source file(s) into the run folder and check the header line
  run_sql_files          run .sql files from include/sql/ in one transaction, with run parameters
"""
import csv
import re
import shutil

import nammamart_common as nc

_VARIABLE_NAME = re.compile(r"^[a-z_][a-z0-9_]*$")


def read_header_and_count(path) -> tuple:
    """Return (header columns, number of data rows) using the csv module (no pandas, no cleaning)."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        rows = sum(1 for _ in reader)
    return header, rows


def check_header(header: list, expected: list, file_name: str) -> None:
    """Raise a clear error if the header is not what we expect (schema change)."""
    missing = [c for c in expected if c not in header]
    extra = [c for c in header if c not in expected]
    if missing or extra:
        raise ValueError(f"Schema change in {file_name}: missing columns {missing}, unexpected columns {extra}")


def snapshot_source_files(entity: str, run_id: str) -> dict:
    """Copy today's file(s) for one entity into include/tmp/<name>/<run_id>/elt/extract/ and check the header.

    For orders, next-day files matching orders_*_increment.csv are copied too (oldest first) and
    marked kind="increment"; they are loaded by a separate step (load_raw_increments).
    """
    file_names = [nc.SOURCE_FILES[entity]]
    if entity == "orders":
        file_names += sorted(p.name for p in nc.DATA_DIR.glob(nc.INCREMENT_GLOB))

    folder = nc.run_dir(run_id, "elt", "extract")
    files = []
    for name in file_names:
        snapshot = folder / name
        shutil.copyfile(nc.DATA_DIR / name, snapshot)   # FileNotFoundError -> task fails -> Airflow retries
        header, rows = read_header_and_count(snapshot)
        check_header(header, nc.EXPECTED_COLUMNS[entity], name)
        kind = "daily" if name == nc.SOURCE_FILES[entity] else "increment"
        print(f"[Extract] {name}: {rows} rows ({kind}) -> {snapshot}")
        files.append({"file": name, "path": str(snapshot), "rows": rows, "kind": kind})
    return {"entity": entity, "files": files}


def run_sql_files(con, files: list, variables: dict = None) -> None:
    """Run one or more .sql files (paths relative to include/sql/) in ONE transaction.

    `variables` are set as DuckDB variables first, and the SQL reads them with getvariable('name'),
    so values are never pasted into the SQL text. If any statement fails, everything is rolled back.
    """
    for name, value in (variables or {}).items():
        if not _VARIABLE_NAME.match(name):
            raise ValueError(f"Invalid SQL variable name: {name}")
        con.execute(f"SET VARIABLE {name} = ?", [value])

    con.execute("BEGIN TRANSACTION")
    try:
        for relative_path in files:
            sql = (nc.SQL_DIR / relative_path).read_text()
            con.execute(sql)
            print(f"[SQL] ran include/sql/{relative_path}")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def table_counts(con, tables: list, run_id: str = None) -> dict:
    """Row count per table; with run_id, only the rows loaded by that run (raw tables have _run_id)."""
    counts = {}
    for table in tables:
        if run_id is None:
            counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        else:
            counts[table] = con.execute(f"SELECT count(*) FROM {table} WHERE _run_id = ?", [run_id]).fetchone()[0]
    return counts


def staging_variables(run_id: str, dag_id: str, as_of=None) -> dict:
    """Run parameters and business rules for the staging and mart SQL, all taken from nammamart_common.py,
    so the ELT SQL and the ETL Python use exactly the same values (one place to change a rule).

    as_of is the Bengaluru time the checks compare with ("is this date in the future?").
    """
    now = nc.utc_now()
    as_of = nc.local_now() if as_of is None else as_of
    if hasattr(as_of, "to_pydatetime"):
        as_of = as_of.to_pydatetime()
    low, high = nc.NORMAL_DELIVERY_RANGE
    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "as_of": as_of,
        "checked_at": now,
        "loaded_at": now,
        "missing_values": list(nc.MISSING_VALUES),
        "timestamp_format": nc.TIMESTAMP_FORMAT,
        "date_format": nc.DATE_FORMAT,
        "valid_order_status": list(nc.VALID_ORDER_STATUS),
        "valid_payment_status": list(nc.VALID_PAYMENT_STATUS),
        "payment_mode_keys": list(nc.PAYMENT_MODE_MAP),
        "payment_mode_values": list(nc.PAYMENT_MODE_MAP.values()),
        "loyalty_tiers": list(nc.LOYALTY_TIERS),
        "valid_zones": list(nc.VALID_ZONES),
        "valid_channels": list(nc.VALID_CHANNELS),
        "unknown": nc.UNKNOWN,
        "max_delivery_minutes": nc.MAX_DELIVERY_MINUTES,
        "normal_delivery_min": low,
        "normal_delivery_max": high,
        "amount_tolerance": nc.AMOUNT_TOLERANCE,
        "freshness_max_age_hours": nc.FRESHNESS_MAX_AGE_HOURS,
        "volume_drop_warn_pct": nc.VOLUME_DROP_WARN_PCT,
        "revenue_status": nc.REVENUE_STATUS,
        "sla_minutes": nc.SLA_MINUTES,
    }

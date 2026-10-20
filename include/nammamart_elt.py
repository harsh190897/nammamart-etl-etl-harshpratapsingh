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

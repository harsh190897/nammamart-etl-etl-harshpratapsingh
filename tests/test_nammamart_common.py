"""Unit tests for include/nammamart_common.py.

Run locally from the project root:   pytest tests/test_nammamart_common.py -v
Run inside Astro (Docker):           astro dev pytest tests/test_nammamart_common.py
"""
import subprocess
import sys
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))
import nammamart_common as nc  # noqa: E402


# ── Paths ─────────────────────────────────────────────────────────────
def test_paths_point_inside_include():
    assert nc.INCLUDE_DIR.name == "include"
    assert nc.WAREHOUSE_DB.name == f"warehouse_{nc.NAME}.duckdb"
    assert nc.QUARANTINE_DIR == nc.INCLUDE_DIR / "quarantine" / nc.NAME
    for file_name in list(nc.SOURCE_FILES.values()) + [nc.INCREMENT_FILE]:
        assert (nc.DATA_DIR / file_name).exists(), f"missing {file_name}"


def test_safe_run_id_and_quarantine_path(tmp_path, monkeypatch):
    run_id = "manual__2026-10-20T07:35:00.123456+00:00"
    assert nc.safe_run_id(run_id) == "manual__2026-10-20T07_35_00_123456_00_00"
    monkeypatch.setattr(nc, "QUARANTINE_DIR", tmp_path / "q")
    path = nc.quarantine_path("orders.csv", run_id)
    assert path.parent.exists()
    assert path.name == "orders_failed_manual__2026-10-20T07_35_00_123456_00_00.csv"


# ── Masking ───────────────────────────────────────────────────────────
@pytest.mark.parametrize("raw, expected", [
    ("pratap@example.com", "pr***@example.com"),
    ("  deepa.desai36@example.com ", "de***@example.com"),
    ("ab@x.com", "a***@x.com"),
    ("not-an-email", "***"),
    ("", None),
    (None, None),
    (float("nan"), None),
])
def test_mask_email(raw, expected):
    assert nc.mask_email(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("9876543221", "98******21"),
    ("98765 43221", "98******21"),
    (9876543221, "98******21"),
    ("12345", "*****"),
    ("", None),
    (None, None),
])
def test_mask_phone(raw, expected):
    assert nc.mask_phone(raw) == expected


def test_mask_name():
    assert nc.mask_name("Deepa Desai") == "D*** D***"
    assert nc.mask_name(" ") is None


def test_mask_customer_pii_removes_raw_columns():
    df = pd.DataFrame({"customer_id": ["C1"], "full_name": ["Deepa Desai"],
                       "email": ["deepa@example.com"], "phone": ["9876543221"]})
    out = nc.mask_customer_pii(df)
    assert set(out.columns) == {"customer_id", "email_masked", "phone_masked", "name_masked"}
    assert out.iloc[0].to_dict() == {"customer_id": "C1", "email_masked": "de***@example.com",
                                     "phone_masked": "98******21", "name_masked": "D*** D***"}
    assert "email" in df.columns  # original DataFrame is not modified


# ── Audit tables ──────────────────────────────────────────────────────
CHECKS = [
    dict(check_name="orders_customer_id_not_null", dimension="Completeness", severity="Critical", rows_checked=2412, rows_failed=18),
    dict(check_name="orders_payment_mode_casing", dimension="Validity", severity="Warning", rows_checked=2412, rows_failed=8),
    dict(check_name="stores_id_unique", dimension="Uniqueness", severity="Critical", rows_checked=15, rows_failed=0),
]


def test_dq_results_written_with_status_and_idempotent(tmp_path):
    db = tmp_path / "test.duckdb"
    with nc.get_connection(db) as con:
        assert nc.write_dq_results(con, "run_1", "dag_x", CHECKS) == 3
        nc.write_dq_results(con, "run_1", "dag_x", CHECKS)          # retry of the same run
        nc.write_dq_results(con, "run_2", "dag_x", CHECKS[:1])      # a different run
        rows = con.execute("SELECT run_id, check_name, status FROM dq_results ORDER BY run_id, check_name").fetchall()
    assert rows == [
        ("run_1", "orders_customer_id_not_null", "FAIL"),
        ("run_1", "orders_payment_mode_casing", "WARN"),
        ("run_1", "stores_id_unique", "PASS"),
        ("run_2", "orders_customer_id_not_null", "FAIL"),
    ]


def test_dq_results_rejects_unknown_dimension(tmp_path):
    bad = [dict(CHECKS[0], dimension="Completness")]  # typo on purpose
    with nc.get_connection(tmp_path / "test.duckdb") as con:
        with pytest.raises(ValueError, match="Unknown dimension"):
            nc.write_dq_results(con, "run_1", "dag_x", bad)


def test_pipeline_runs_idempotent_and_utc(tmp_path):
    ist = timezone(timedelta(hours=5, minutes=30))
    start = datetime(2026, 10, 20, 9, 0, tzinfo=ist)            # 09:00 IST
    end = start + timedelta(minutes=2)
    with nc.get_connection(tmp_path / "test.duckdb") as con:
        nc.write_pipeline_run(con, "run_1", "dag_x", start, end, 5372, 2300, 120, "FAILED")
        nc.write_pipeline_run(con, "run_1", "dag_x", start, end, 5372, 2300, 120, "SUCCESS")  # re-run
        rows = con.execute("SELECT start_time, status FROM pipeline_runs").fetchall()
    assert rows == [(datetime(2026, 10, 20, 3, 30), "SUCCESS")]   # stored as 03:30 UTC, one row


# ── Connection ────────────────────────────────────────────────────────
def test_connection_is_closed_after_with_block(tmp_path):
    with nc.get_connection(tmp_path / "test.duckdb") as con:
        con.execute("SELECT 1")
    with pytest.raises(Exception):
        con.execute("SELECT 1")


def test_connection_waits_for_lock(tmp_path):
    db = tmp_path / "locked.duckdb"
    holder = textwrap.dedent(f"""
        import duckdb, time
        con = duckdb.connect(r"{db}")
        print("locked", flush=True)
        time.sleep(3)
        con.close()
    """)
    proc = subprocess.Popen([sys.executable, "-c", holder], stdout=subprocess.PIPE, text=True)
    assert proc.stdout.readline().strip() == "locked"
    with nc.get_connection(db, retries=10, wait_seconds=1) as con:
        assert con.execute("SELECT 42").fetchone() == (42,)
    proc.wait()


# ── Extract helpers (Part A.1) ────────────────────────────────────────
def test_every_source_file_has_the_expected_header():
    for entity, file_name in nc.SOURCE_FILES.items():
        df = nc.read_text_csv(nc.DATA_DIR / file_name)
        nc.check_columns(df, nc.EXPECTED_COLUMNS[entity], file_name)      # raises if the header changed
    increments = sorted(nc.DATA_DIR.glob(nc.INCREMENT_GLOB))
    assert [p.name for p in increments] == [nc.INCREMENT_FILE]


def test_check_columns_catches_a_renamed_column():
    df = pd.DataFrame(columns=[c if c != "order_amount" else "total" for c in nc.EXPECTED_COLUMNS["orders"]])
    with pytest.raises(ValueError, match=r"missing columns \['order_amount'\], unexpected columns \['total'\]"):
        nc.check_columns(df, nc.EXPECTED_COLUMNS["orders"], "orders.csv")


def test_read_text_csv_keeps_values_as_text(tmp_path):
    path = tmp_path / "x.csv"
    path.write_text("id,pincode,note\n007,560001,N/A\n")
    df = nc.read_text_csv(path)
    assert df.iloc[0].to_dict() == {"id": "007", "pincode": "560001", "note": "N/A"}


def test_run_dir_is_created_per_run(tmp_path, monkeypatch):
    monkeypatch.setattr(nc, "TMP_DIR", tmp_path / "tmp")
    folder = nc.run_dir("manual__2026-10-20T07:35:00+00:00", "extract")
    assert folder.is_dir()
    assert folder == tmp_path / "tmp" / "manual__2026-10-20T07_35_00_00_00" / "extract"



# ── Missing values (Part A.2) ─────────────────────────────────────────
def test_is_missing_treats_placeholders_as_blank():
    values = pd.Series(["", "  ", "N/A", "null", "-", None, "C00001", "0"])
    assert nc.is_missing(values).tolist() == [True, True, True, True, True, True, False, False]


# ── Parquet helpers (Part A.3) ────────────────────────────────────────
def test_parquet_round_trip_keeps_types(tmp_path):
    df = pd.DataFrame({"id": ["A", "B"], "amount": [1.5, None],
                       "flag": pd.array([True, None], dtype="boolean"),
                       "ts": pd.to_datetime(["2026-09-01 10:00:00", None])})
    back = nc.read_parquet(nc.write_parquet(df, tmp_path / "x" / "t.parquet"))
    assert back["id"].tolist() == ["A", "B"]
    assert back["amount"].iloc[0] == 1.5 and pd.isna(back["amount"].iloc[1])
    assert back["flag"].iloc[0] == True and pd.isna(back["flag"].iloc[1])  # noqa: E712
    assert back["ts"].iloc[0] == pd.Timestamp("2026-09-01 10:00:00") and pd.isna(back["ts"].iloc[1])



# ── Run folder clean-up (Part A.4) ────────────────────────────────────
def test_cleanup_removes_only_old_run_folders(tmp_path, monkeypatch):
    import os
    import time
    monkeypatch.setattr(nc, "TMP_DIR", tmp_path / "tmp")
    old, new = nc.run_dir("old_run"), nc.run_dir("new_run")
    five_days_ago = time.time() - 5 * 86400
    os.utime(old, (five_days_ago, five_days_ago))
    assert nc.cleanup_old_run_dirs(keep_days=3) == 1
    assert not old.exists() and new.exists()

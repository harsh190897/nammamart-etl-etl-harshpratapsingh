"""
include/nammamart_etl_checks.py
-------------------------------
Data quality rules for the ETL pipeline, written in pandas (Part D).

Every validate_* function takes a raw, all-text DataFrame (exactly as read from the CSV)
and returns a ValidationResult:
    passed  rows that passed every Critical check -> continue to Transform
    failed  rows that failed at least one Critical check, with a failure_reason -> quarantine
    checks  one dict per check -> dq_results table

Critical = the row is quarantined.  Warning = the row continues, the problem is only counted.
Both kinds are written to dq_results on every run.
"""
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

import nammamart_common as nc

EMAIL_PATTERN = r"[\w.+-]+@[\w-]+\.[\w.]+"


@dataclass
class ValidationResult:
    file: str
    passed: pd.DataFrame
    failed: pd.DataFrame
    checks: list


# ─────────────────────────── helpers ───────────────────────────
def _blank(s: pd.Series) -> pd.Series:
    return nc.is_missing(s)


def _clean(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def _number(s: pd.Series) -> pd.Series:
    return pd.to_numeric(_clean(s).where(~_blank(s)), errors="coerce")


def _not_a_number(s: pd.Series) -> pd.Series:
    return ~_blank(s) & _number(s).isna()


def _timestamp(s: pd.Series, fmt: str) -> pd.Series:
    return pd.to_datetime(_clean(s).where(~_blank(s)), format=fmt, errors="coerce")


def standard_payment_mode(s: pd.Series) -> pd.Series:
    """'upi', 'UPI ', 'cod' -> 'UPI', 'UPI', 'Cash on Delivery'. Unknown values -> NaN."""
    return _clean(s).str.upper().map(nc.PAYMENT_MODE_MAP)


class _Checker:
    """Collects the result of every check for one file."""

    def __init__(self, df: pd.DataFrame, file_name: str):
        self.file = file_name
        self.prefix = Path(file_name).stem
        self.df = df.reset_index(drop=True).copy()
        self.df["source_file"] = file_name
        self.df["source_row"] = self.df.index + 2          # CSV line number (line 1 is the header)
        self.critical = pd.Series(False, index=self.df.index)
        self.reason = pd.Series("", index=self.df.index)
        self.checks = []

    def check(self, rule: str, dimension: str, severity: str, failed, reason: str) -> pd.Series:
        """Row-level check. `failed` is True for every bad row."""
        failed = pd.Series(failed, index=self.df.index).fillna(False).astype(bool)
        self.checks.append(dict(check_name=f"{self.prefix}.{rule}", dimension=dimension, severity=severity,
                                rows_checked=len(self.df), rows_failed=int(failed.sum())))
        if severity == "Critical":
            self.critical |= failed
            self.reason[failed] = self.reason[failed] + reason + "; "
        if failed.any():
            print(f"[DQ] {self.prefix}.{rule} ({severity}): {int(failed.sum())} rows - {reason}")
        return failed

    def file_check(self, rule: str, dimension: str, severity: str, failed: bool, detail: str) -> None:
        """File-level check (e.g. freshness): 1 row checked, 1 row failed if the file is bad."""
        self.checks.append(dict(check_name=f"{self.prefix}.{rule}", dimension=dimension, severity=severity,
                                rows_checked=1, rows_failed=int(failed)))
        print(f"[DQ] {self.prefix}.{rule} ({severity}): {'FAILED' if failed else 'ok'} - {detail}")

    def unique_key(self, key: str) -> None:
        self.check(f"{key}_not_null", "Completeness", "Critical", _blank(self.df[key]), f"{key} is missing")
        dup = _clean(self.df[key]).duplicated(keep="first") & ~_blank(self.df[key])
        self.check(f"{key}_unique", "Uniqueness", "Critical", dup, f"duplicate {key} (copy of an earlier row)")

    def result(self) -> ValidationResult:
        failed = self.df[self.critical].copy()
        failed["failure_reason"] = self.reason[self.critical].str.rstrip("; ")
        passed = self.df[~self.critical].copy()
        print(f"[DQ] {self.file}: {len(self.df)} rows -> {len(passed)} passed, {len(failed)} quarantined")
        return ValidationResult(self.file, passed, failed, self.checks)


# ─────────────────────────── master files ───────────────────────────
def validate_customers(df: pd.DataFrame, as_of: pd.Timestamp, file_name: str = "customers.csv") -> ValidationResult:
    c = _Checker(df, file_name)
    d = c.df
    c.unique_key("customer_id")

    signup = _timestamp(d["signup_date"], nc.DATE_FORMAT)
    c.check("signup_date_valid", "Validity", "Critical", signup.isna(), "signup_date missing or not YYYY-MM-DD")
    c.check("signup_date_not_future", "Timeliness", "Critical", signup > as_of, "signup_date is in the future")

    tier = _clean(d["loyalty_tier"]).str.title()
    c.check("loyalty_tier_valid", "Validity", "Critical", ~tier.isin(nc.LOYALTY_TIERS), "unknown loyalty_tier")
    c.check("loyalty_tier_casing", "Consistency", "Warning",
            tier.isin(nc.LOYALTY_TIERS) & (d["loyalty_tier"] != tier), "loyalty_tier casing differs")

    email = _clean(d["email"])
    c.check("email_not_null", "Completeness", "Warning", _blank(d["email"]), "email is missing")
    c.check("email_format", "Validity", "Warning",
            ~_blank(d["email"]) & ~email.str.fullmatch(EMAIL_PATTERN), "email format is invalid")
    c.check("phone_10_digits", "Validity", "Warning", ~_clean(d["phone"]).str.fullmatch(r"\d{10}"),
            "phone is not 10 digits")
    c.check("pincode_6_digits", "Validity", "Warning", ~_clean(d["pincode"]).str.fullmatch(r"\d{6}"),
            "pincode is not 6 digits")
    c.check("city_not_null", "Completeness", "Warning", _blank(d["city"]), "city is missing")
    c.check("is_active_valid", "Validity", "Warning", ~_clean(d["is_active"]).str.upper().isin(["Y", "N"]),
            "is_active is not Y/N")
    return c.result()


def validate_products(df: pd.DataFrame, file_name: str = "products.csv") -> ValidationResult:
    c = _Checker(df, file_name)
    d = c.df
    c.unique_key("product_id")
    c.check("category_not_null", "Completeness", "Critical", _blank(d["category"]), "category is missing")

    mrp, cost = _number(d["mrp"]), _number(d["cost_price"])
    c.check("mrp_is_number", "Validity", "Critical", mrp.isna(), "mrp missing or not a number")
    c.check("cost_price_is_number", "Validity", "Critical", cost.isna(), "cost_price missing or not a number")
    c.check("mrp_positive", "Accuracy", "Critical", mrp <= 0, "mrp is zero or negative")
    c.check("cost_price_not_above_mrp", "Accuracy", "Critical", cost > mrp, "cost_price is greater than mrp")
    c.check("is_perishable_valid", "Validity", "Warning",
            ~_clean(d["is_perishable"]).str.upper().isin(["TRUE", "FALSE"]), "is_perishable is not True/False")
    return c.result()


def validate_stores(df: pd.DataFrame, file_name: str = "stores.csv") -> ValidationResult:
    c = _Checker(df, file_name)
    d = c.df
    c.unique_key("store_id")
    # A missing zone is only a Warning: the store is real and its orders are valid revenue.
    # Quarantining it would hide all its orders, so Transform shows the zone as 'Unknown'.
    c.check("zone_valid", "Completeness", "Warning", ~_clean(d["zone"]).isin(nc.VALID_ZONES),
            "zone is missing or not North/South/East/West")
    capacity = _number(d["capacity_orders_per_day"])
    c.check("capacity_positive", "Accuracy", "Warning", ~(capacity > 0), "capacity_orders_per_day is not > 0")
    c.check("pincode_6_digits", "Validity", "Warning", ~_clean(d["pincode"]).str.fullmatch(r"\d{6}"),
            "pincode is not 6 digits")
    return c.result()


# ─────────────────────────── orders ───────────────────────────
def validate_orders(df: pd.DataFrame, file_name: str, refs: dict, as_of: pd.Timestamp) -> ValidationResult:
    """refs = {"customer_id": (all_ids, valid_ids), "product_id": (...), "store_id": (...)}
    all_ids   = every ID in the master file;  valid_ids = IDs that passed the master's own checks."""
    c = _Checker(df, file_name)
    d = c.df

    # Completeness + Uniqueness
    c.unique_key("order_id")
    c.check("customer_id_not_null", "Completeness", "Critical", _blank(d["customer_id"]), "customer_id is missing")
    c.check("order_ts_not_null", "Completeness", "Critical", _blank(d["order_ts"]), "order_ts is missing")
    status = _clean(d["order_status"]).str.upper()
    c.check("delivered_has_delivery_minutes", "Completeness", "Critical",
            (status == "DELIVERED") & _blank(d["delivery_minutes"]), "DELIVERED order has no delivery_minutes")

    # Validity
    ts = _timestamp(d["order_ts"], nc.TIMESTAMP_FORMAT)
    c.check("order_ts_valid", "Validity", "Critical", ~_blank(d["order_ts"]) & ts.isna(),
            "order_ts is not a valid YYYY-MM-DD HH:MM:SS timestamp")
    c.check("order_status_valid", "Validity", "Critical", ~status.isin(nc.VALID_ORDER_STATUS),
            "order_status is not DELIVERED/CANCELLED/RETURNED")
    c.check("order_status_casing", "Validity", "Warning",
            status.isin(nc.VALID_ORDER_STATUS) & (d["order_status"] != status), "order_status needed upper-casing")
    mode = standard_payment_mode(d["payment_mode"])
    c.check("payment_mode_valid", "Validity", "Critical", mode.isna(), "payment_mode is not a known mode")
    c.check("payment_mode_standardised", "Validity", "Warning", mode.notna() & (d["payment_mode"] != mode),
            "payment_mode needed standardising")
    for col in ("quantity", "unit_price", "discount_pct", "order_amount"):
        c.check(f"{col}_not_null", "Completeness", "Critical", _blank(d[col]), f"{col} is missing")
    for col in ("quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"):
        c.check(f"{col}_is_number", "Validity", "Critical", _not_a_number(d[col]), f"{col} is not a number")
    qty, price, disc = _number(d["quantity"]), _number(d["unit_price"]), _number(d["discount_pct"])
    amount, minutes = _number(d["order_amount"]), _number(d["delivery_minutes"])
    c.check("quantity_whole_number", "Validity", "Critical", qty.notna() & ((qty % 1) != 0),
            "quantity is not a whole number")
    c.check("discount_pct_0_to_100", "Validity", "Critical", (disc < 0) | (disc > 100), "discount_pct outside 0-100")
    c.check("channel_valid", "Validity", "Warning", ~_clean(d["channel"]).isin(nc.VALID_CHANNELS),
            "channel is not App/Web")

    # Accuracy
    c.check("quantity_positive", "Accuracy", "Critical", qty <= 0, "quantity is zero or negative")
    c.check("unit_price_positive", "Accuracy", "Critical", price <= 0, "unit_price is zero or negative")
    expected = (qty * price * (1 - disc / 100)).round(2)
    c.check("order_amount_matches_formula", "Accuracy", "Critical", (expected - amount).abs() > nc.AMOUNT_TOLERANCE,
            "order_amount != quantity x unit_price x (1 - discount_pct/100)")
    c.check("order_amount_not_negative", "Accuracy", "Critical", amount < 0, "order_amount is negative")
    c.check("delivery_minutes_max", "Accuracy", "Critical", minutes > nc.MAX_DELIVERY_MINUTES,
            f"delivery_minutes above {nc.MAX_DELIVERY_MINUTES}")
    low, high = nc.NORMAL_DELIVERY_RANGE
    c.check("delivery_minutes_normal_range", "Accuracy", "Warning",
            ((minutes < low) | (minutes > high)) & ~(minutes > nc.MAX_DELIVERY_MINUTES),
            f"delivery_minutes outside the normal {low}-{high} range")

    # Consistency: every key must exist in its master file AND point to a master row that passed
    for col, (all_ids, valid_ids) in refs.items():
        master = col.replace("_id", "s")
        value = _clean(d[col])
        present = ~_blank(d[col])
        unknown = present & ~value.isin(set(all_ids))
        c.check(f"{col}_exists", "Consistency", "Critical", unknown, f"{col} not found in {master}")
        c.check(f"{col}_master_valid", "Consistency", "Critical",
                present & ~unknown & ~value.isin(set(valid_ids)), f"{col} points to a quarantined {master} row")

    # Timeliness
    c.check("order_ts_not_future", "Timeliness", "Critical", ts > as_of, "order_ts is in the future")
    newest = ts[ts <= as_of].max()
    stale = pd.isna(newest) or (as_of - newest) > pd.Timedelta(hours=nc.FRESHNESS_MAX_AGE_HOURS)
    c.file_check("freshness", "Timeliness", "Warning", stale,
                 f"newest order_ts {newest} vs run time {as_of:%Y-%m-%d %H:%M} "
                 f"(limit {nc.FRESHNESS_MAX_AGE_HOURS}h)")
    return c.result()


# ─────────────────────────── payments ───────────────────────────
def validate_payments(df: pd.DataFrame, known_order_ids, as_of: pd.Timestamp,
                      file_name: str = "payments.csv") -> ValidationResult:
    c = _Checker(df, file_name)
    d = c.df
    c.unique_key("payment_id")
    c.check("order_id_not_null", "Completeness", "Critical", _blank(d["order_id"]), "order_id is missing")

    ts = _timestamp(d["payment_ts"], nc.TIMESTAMP_FORMAT)
    c.check("payment_ts_valid", "Validity", "Critical", ts.isna(), "payment_ts missing or not a valid timestamp")
    status = _clean(d["payment_status"]).str.upper()
    c.check("payment_status_valid", "Validity", "Critical", ~status.isin(nc.VALID_PAYMENT_STATUS),
            "payment_status is not SUCCESS/FAILED/REFUNDED")
    mode = standard_payment_mode(d["payment_mode"])
    c.check("payment_mode_valid", "Validity", "Critical", mode.isna(), "payment_mode is not a known mode")
    c.check("payment_mode_standardised", "Validity", "Warning", mode.notna() & (d["payment_mode"] != mode),
            "payment_mode needed standardising")
    c.check("amount_paid_is_number", "Validity", "Critical", _number(d["amount_paid"]).isna(),
            "amount_paid missing or not a number")
    c.check("amount_paid_not_negative", "Accuracy", "Critical", _number(d["amount_paid"]) < 0,
            "amount_paid is negative")

    # A payment for an unknown order is not a bad payment record: it is a finance mismatch.
    # It is logged here and listed in the reconciliation table instead of being quarantined.
    d["order_in_source"] = _clean(d["order_id"]).isin(set(known_order_ids))
    c.check("order_id_exists", "Consistency", "Warning", ~_blank(d["order_id"]) & ~d["order_in_source"],
            "payment for an order_id not in the orders files")
    c.check("gateway_ref_for_online_success", "Completeness", "Warning",
            _blank(d["gateway_ref"]) & (status == "SUCCESS") & mode.notna() & (mode != "Cash on Delivery"),
            "online SUCCESS payment has no gateway_ref")
    c.check("payment_ts_not_future", "Timeliness", "Critical", ts > as_of, "payment_ts is in the future")
    return c.result()


# ─────────────────────────── volume (file-level) ───────────────────────────
def volume_check(check_name: str, current_rows: int, previous_rows) -> dict:
    """Warning if the row count dropped by more than VOLUME_DROP_WARN_PCT % compared with the previous run.
    previous_rows is None on the very first run (nothing to compare with -> PASS)."""
    dropped = (previous_rows is not None and previous_rows > 0
               and current_rows < previous_rows * (1 - nc.VOLUME_DROP_WARN_PCT / 100))
    print(f"[DQ] {check_name} (Warning): {'FAILED' if dropped else 'ok'} - "
          f"{current_rows} rows now vs {previous_rows} in the previous run")
    return dict(check_name=check_name, dimension="Completeness", severity="Warning",
                rows_checked=1, rows_failed=int(dropped))

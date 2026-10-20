"""Step 1: profile every file (rows, nulls, duplicate keys, min/max, distinct values).

Run from the project root:
    python profiling/profile_files.py > profiling/profile_output.txt
"""
import pandas as pd
from common import FILES, load, is_missing

KEYS = {
    "orders": "order_id", "payments": "payment_id", "customers": "customer_id",
    "products": "product_id", "stores": "store_id", "increment": "order_id",
}
NUMERIC = {
    "orders": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
    "payments": ["amount_paid"],
    "customers": ["pincode"],
    "products": ["mrp", "cost_price"],
    "stores": ["pincode", "capacity_orders_per_day"],
    "increment": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
}
DATES = {
    "orders": ["order_ts"], "payments": ["payment_ts"], "customers": ["signup_date"],
    "products": ["launch_date"], "stores": ["open_date"], "increment": ["order_ts"],
}
CATEGORIES = {
    "orders": ["order_status", "payment_mode", "channel"],
    "payments": ["payment_mode", "payment_status"],
    "customers": ["loyalty_tier", "city", "is_active"],
    "products": ["category", "unit", "is_perishable"],
    "stores": ["zone"],
    "increment": ["order_status", "payment_mode", "channel"],
}

for name in FILES:
    df = load(name)
    print("=" * 70)
    print(f"FILE: {FILES[name]}   rows={len(df)}   columns={len(df.columns)}")
    print("=" * 70)

    print("\n-- Missing/blank values per column (blank, NULL, N/A...) --")
    miss = df.apply(is_missing).sum()
    print(miss[miss > 0].to_string() if (miss > 0).any() else "none")

    key = KEYS[name]
    dup = df[df[key].duplicated(keep=False)].sort_values(key)
    print(f"\n-- Duplicate {key} values: {df[key].duplicated().sum()} extra rows "
          f"({dup[key].nunique()} keys repeated) --")
    if len(dup):
        print(dup.head(6).to_string())

    print("\n-- Whole-row exact duplicates:", int(df.duplicated().sum()))

    print("\n-- Numeric min / max (non-numeric values listed separately) --")
    for col in NUMERIC[name]:
        num = pd.to_numeric(df[col], errors="coerce")
        bad = df.loc[num.isna() & ~is_missing(df[col]), col].unique()[:5]
        print(f"{col:22} min={num.min()}  max={num.max()}  non-numeric examples={list(bad)}")

    print("\n-- Date min / max (unparseable values listed separately) --")
    for col in DATES[name]:
        dt = pd.to_datetime(df[col], errors="coerce")
        bad = df.loc[dt.isna() & ~is_missing(df[col]), col].unique()[:5]
        print(f"{col:22} min={dt.min()}  max={dt.max()}  unparseable examples={list(bad)}")

    print("\n-- Distinct values of category columns (raw, with counts) --")
    for col in CATEGORIES[name]:
        print(f"[{col}]")
        vc = df[col].value_counts(dropna=False)
        vc.index = [repr(v) for v in vc.index]   # repr() makes trailing spaces visible, e.g. 'UPI '
        print(vc.to_string())
    print()

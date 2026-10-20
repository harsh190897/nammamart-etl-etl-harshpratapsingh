"""Steps 2-4: referential integrity + business-rule checks. Builds the Data Issues Log.

Run from the project root:
    python profiling/find_issues.py

Outputs (in profiling/output/):
  issues_log_detail.csv   one row per bad record (file, row_key, column, problem, ...)
  issues_log_summary.csv  one row per issue type, with count and example keys (goes into SUBMISSION.md)
"""
from pathlib import Path
import pandas as pd
from common import load, is_missing

OUT_DIR = Path(__file__).resolve().parent / "output"
OUT_DIR.mkdir(exist_ok=True)

# Profiling uses a fixed "as of" date instead of today's date so results never change between runs.
AS_OF = pd.Timestamp("2026-10-02")

VALID_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_MODE = {"UPI", "Card", "Cash on Delivery", "Wallet"}
MODE_MAP = {"UPI": "UPI", "CARD": "Card", "CASH ON DELIVERY": "Cash on Delivery", "COD": "Cash on Delivery", "WALLET": "Wallet"}

issues = []  # list of dicts


def log(file, keys, column, problem, dimension, rule, severity):
    """Record one issue for every key in `keys`."""
    for k in keys:
        issues.append(dict(file=file, row_key=k, column=column, problem=problem,
                           quality_dimension=dimension, proposed_rule=rule, severity=severity))


orders, payments = load("orders"), load("payments")
customers, products = load("customers"), load("products")
stores, inc = load("stores"), load("increment")

# ───────────────────────── ORDERS (and the increment file, same rules) ─────────────────────────
for fname, o in (("orders.csv", orders), ("orders_increment.csv", inc)):
    k = o["order_id"] + "@row" + (o.index + 2).astype(str)  # key + CSV line number (line 1 = header)
    if fname == "orders.csv":
        # Uniqueness
        dup = o["order_id"].duplicated(keep="first")
        exact = o.duplicated(keep="first")
        log(fname, k[dup & exact], "order_id", "Exact duplicate row", "Uniqueness", "Drop repeated copy, keep first", "Critical")
        log(fname, k[dup & ~exact], "order_id", "Duplicate order_id with different values", "Uniqueness", "Quarantine all copies", "Critical")

    # Completeness
    log(fname, k[is_missing(o["customer_id"])], "customer_id", "Missing customer_id", "Completeness", "customer_id NOT NULL", "Critical")
    log(fname, k[is_missing(o["order_ts"])], "order_ts", "Missing order_ts", "Completeness", "order_ts NOT NULL", "Critical")

    # Validity
    ts = pd.to_datetime(o["order_ts"], errors="coerce")
    log(fname, k[ts.isna() & ~is_missing(o["order_ts"])], "order_ts", "Unparseable timestamp (bad format or month)", "Validity", "order_ts must parse as timestamp", "Critical")
    status = o["order_status"]
    fixable = ~status.isin(VALID_STATUS) & status.str.strip().str.upper().isin(VALID_STATUS)
    log(fname, k[fixable], "order_status", "Valid status but wrong casing (e.g. 'delivered')", "Validity", "TRIM + UPPER before validating", "Warning")
    bad_status = ~status.str.strip().str.upper().isin(VALID_STATUS)
    log(fname, k[bad_status], "order_status", "Invalid status (e.g. 'Pending??')", "Validity", "status IN (DELIVERED, CANCELLED, RETURNED)", "Critical")
    mode = o["payment_mode"]
    mode_std = mode.str.strip().str.upper().map(MODE_MAP)
    log(fname, k[~mode.isin(VALID_MODE) & mode_std.notna()], "payment_mode", "Mode needs standardising (case/space/'cod')", "Validity", "Map to UPI/Card/Cash on Delivery/Wallet", "Warning")
    log(fname, k[mode_std.isna()], "payment_mode", "Unrecognised payment_mode (e.g. 'Cashh')", "Validity", "Quarantine or ask client", "Critical")
    disc = pd.to_numeric(o["discount_pct"], errors="coerce")
    log(fname, k[(disc < 0) | (disc > 100)], "discount_pct", "Discount outside 0-100", "Validity", "0 <= discount_pct <= 100", "Critical")

    # Accuracy
    qty = pd.to_numeric(o["quantity"], errors="coerce")
    price = pd.to_numeric(o["unit_price"], errors="coerce")
    amt = pd.to_numeric(o["order_amount"], errors="coerce")
    dm = pd.to_numeric(o["delivery_minutes"], errors="coerce")
    log(fname, k[qty <= 0], "quantity", "Quantity zero or negative", "Accuracy", "quantity > 0", "Critical")
    expected = (qty * price * (1 - disc / 100)).round(2)
    log(fname, k[(expected - amt).abs() > 0.01], "order_amount", "order_amount != qty x price x (1-disc/100)", "Accuracy", "Recompute and compare (tolerance 0.01)", "Critical")
    log(fname, k[amt < 0], "order_amount", "Negative order_amount", "Accuracy", "order_amount >= 0", "Critical")
    log(fname, k[dm > 120], "delivery_minutes", "Impossible delivery time (> 120 min)", "Accuracy", "delivery_minutes < 120", "Critical")
    log(fname, k[((dm >= 0) & (dm < 8)) | ((dm > 35) & (dm <= 120))], "delivery_minutes", "Outside normal 8-35 min range", "Accuracy", "Log as warning only", "Warning")
    log(fname, k[(status.str.strip().str.upper() == "DELIVERED") & dm.isna()], "delivery_minutes", "DELIVERED order without delivery_minutes", "Completeness", "DELIVERED => delivery_minutes NOT NULL", "Critical")

    # Timeliness
    log(fname, k[ts > AS_OF], "order_ts", f"order_ts in the future (after {AS_OF.date()})", "Timeliness", "order_ts <= run time", "Critical")

    # Consistency (referential integrity)
    present = ~is_missing(o["customer_id"])
    log(fname, k[present & ~o["customer_id"].isin(customers["customer_id"])], "customer_id", "customer_id not found in customers", "Consistency", "FK customer_id -> customers", "Critical")
    log(fname, k[~o["product_id"].isin(products["product_id"])], "product_id", "product_id not found in products", "Consistency", "FK product_id -> products", "Critical")
    log(fname, k[~o["store_id"].isin(stores["store_id"])], "store_id", "store_id not found in stores", "Consistency", "FK store_id -> stores", "Critical")

# ───────────────────────── PAYMENTS ─────────────────────────
pk = payments["payment_id"] + "@row" + (payments.index + 2).astype(str)
dup = payments["payment_id"].duplicated(keep="first")
log("payments.csv", pk[dup & payments.duplicated(keep="first")], "payment_id", "Exact duplicate row", "Uniqueness", "Drop repeated copy, keep first", "Critical")
log("payments.csv", pk[dup & ~payments.duplicated(keep="first")], "payment_id", "Duplicate payment_id with different values", "Uniqueness", "Quarantine all copies", "Critical")
log("payments.csv", pk[~payments["order_id"].isin(orders["order_id"])], "order_id", "Payment for an order that does not exist", "Consistency", "FK order_id -> orders", "Critical")
pmode = payments["payment_mode"]
pstd = pmode.str.strip().str.upper().map(MODE_MAP)
log("payments.csv", pk[~pmode.isin(VALID_MODE) & pstd.notna()], "payment_mode", "Mode needs standardising", "Validity", "Map to standard names", "Warning")
log("payments.csv", pk[pstd.isna()], "payment_mode", "Unrecognised payment_mode", "Validity", "Quarantine or ask client", "Critical")
pamt = pd.to_numeric(payments["amount_paid"], errors="coerce")
log("payments.csv", pk[pamt < 0], "amount_paid", "Negative amount_paid", "Accuracy", "amount_paid >= 0 (refunds should be status REFUNDED)", "Critical")
pts = pd.to_datetime(payments["payment_ts"], errors="coerce")
log("payments.csv", pk[pts > AS_OF], "payment_ts", "payment_ts in the future", "Timeliness", "payment_ts <= run time", "Critical")

# Compare with the (de-duplicated) orders
o1 = orders.drop_duplicates("order_id").set_index("order_id")
o1_amt = pd.to_numeric(o1["order_amount"], errors="coerce")
succ = payments[payments["payment_status"] == "SUCCESS"].copy()
succ["order_amount"] = succ["order_id"].map(o1_amt)
succ["amount_paid_num"] = pd.to_numeric(succ["amount_paid"], errors="coerce")
mismatch = succ[succ["order_amount"].notna() & ((succ["order_amount"] - succ["amount_paid_num"]).abs() > 0.01)]
log("payments.csv", mismatch["payment_id"] + "@" + mismatch["order_id"], "amount_paid", "SUCCESS payment amount != order_amount", "Consistency", "SUCCESS => amount_paid == order_amount", "Critical")
cancelled_ids = set(o1.index[o1["order_status"].str.strip().str.upper() == "CANCELLED"])
log("payments.csv", pk[payments["order_id"].isin(cancelled_ids)], "order_id", "Payment exists for a CANCELLED order", "Consistency", "Cancelled orders should have no payment", "Warning")
pm = payments.merge(o1[["payment_mode"]].rename(columns={"payment_mode": "order_mode"}), left_on="order_id", right_index=True, how="inner")
diff_mode = pm[pm["payment_mode"].str.strip().str.upper().map(MODE_MAP) != pm["order_mode"].str.strip().str.upper().map(MODE_MAP)]
log("payments.csv", diff_mode["payment_id"], "payment_mode", "Payment mode differs from the order's mode", "Consistency", "Modes should match", "Warning")
log("payments.csv", pk[is_missing(payments["gateway_ref"]) & (payments["payment_mode"].str.strip().str.upper() != "CASH ON DELIVERY") & (payments["payment_status"] == "SUCCESS")],
    "gateway_ref", "Online SUCCESS payment without gateway_ref", "Completeness", "gateway_ref required for non-COD SUCCESS", "Warning")

# Orders that should have a payment but don't
need_pay = o1[o1["order_status"].str.strip().str.upper().isin(["DELIVERED", "RETURNED"])]
no_pay = need_pay.index[~need_pay.index.isin(payments["order_id"])]
log("orders.csv", pd.Series(no_pay), "order_id", "DELIVERED/RETURNED order has no payment record", "Consistency", "Goes to reconciliation table", "Warning")

# ───────────────────────── CUSTOMERS ─────────────────────────
ck = customers["customer_id"] + "@row" + (customers.index + 2).astype(str)
log("customers.csv", ck[customers["customer_id"].duplicated(keep="first")], "customer_id", "Duplicate customer_id (rows differ in phone/area)", "Uniqueness", "One row per customer_id; keep latest/first, log conflict", "Critical")
log("customers.csv", ck[is_missing(customers["email"])], "email", "Missing email", "Completeness", "Warn; mask nothing", "Warning")
log("customers.csv", ck[is_missing(customers["city"])], "city", "Missing city", "Completeness", "Warn", "Warning")
em = customers["email"]
log("customers.csv", ck[~is_missing(em) & ~em.str.match(r"^[\w.+-]+@[\w-]+\.[\w.]+$")], "email", "Invalid email format", "Validity", "Email regex", "Warning")
log("customers.csv", ck[~customers["phone"].str.fullmatch(r"\d{10}")], "phone", "Phone is not 10 digits", "Validity", "phone = 10 digits", "Warning")
log("customers.csv", ck[~customers["pincode"].str.fullmatch(r"\d{6}")], "pincode", "Pincode is not 6 digits", "Validity", "pincode = 6 digits", "Warning")
tier = customers["loyalty_tier"]
log("customers.csv", ck[~tier.isin(["Bronze", "Silver", "Gold", "Platinum"])], "loyalty_tier", "Inconsistent casing (BRONZE/bronze/SILVER)", "Consistency", "Standardise to Title case", "Warning")
sd = pd.to_datetime(customers["signup_date"], errors="coerce")
log("customers.csv", ck[sd > AS_OF], "signup_date", "signup_date in the future", "Timeliness", "signup_date <= run time", "Critical")

# ───────────────────────── PRODUCTS ─────────────────────────
pr = products["product_id"] + "@row" + (products.index + 2).astype(str)
log("products.csv", pr[products["product_id"].duplicated(keep="first")], "product_id", "Duplicate product_id ('(old)' rows with different mrp)", "Uniqueness", "One row per product_id; drop '(old)' version", "Critical")
log("products.csv", pr[is_missing(products["category"])], "category", "Missing category", "Completeness", "category NOT NULL", "Critical")
mrp = pd.to_numeric(products["mrp"], errors="coerce")
cost = pd.to_numeric(products["cost_price"], errors="coerce")
log("products.csv", pr[mrp <= 0], "mrp", "mrp zero or negative", "Accuracy", "mrp > 0", "Critical")
log("products.csv", pr[cost > mrp], "cost_price", "cost_price greater than mrp", "Accuracy", "cost_price <= mrp", "Critical")

# ───────────────────────── STORES ─────────────────────────
sk = stores["store_id"] + "@row" + (stores.index + 2).astype(str)
log("stores.csv", sk[is_missing(stores["zone"])], "zone", "Missing zone", "Completeness", "zone NOT NULL (needed for zone revenue)", "Critical")
log("stores.csv", sk[pd.to_numeric(stores["capacity_orders_per_day"], errors="coerce") <= 0], "capacity_orders_per_day", "Capacity is zero", "Accuracy", "capacity > 0", "Warning")
log("stores.csv", sk[~stores["pincode"].str.fullmatch(r"\d{6}")], "pincode", "Pincode is not 6 digits", "Validity", "pincode = 6 digits", "Warning")

# ───────────────────────── INCREMENT: what does it change? ─────────────────────────
overlap = inc[inc["order_id"].isin(orders["order_id"])]
print(f"\nIncrement rows: {len(inc)}  | already in orders.csv (corrections): {len(overlap)}  | brand new: {len(inc) - len(overlap)}")
cmp = overlap.merge(orders.drop_duplicates("order_id"), on="order_id", suffixes=("_new", "_old"))
print(cmp[["order_id", "order_status_old", "order_status_new", "order_amount_old", "order_amount_new"]].to_string(index=False))

# ───────────────────────── Save ─────────────────────────
log_df = pd.DataFrame(issues)
log_df.to_csv(OUT_DIR / "issues_log_detail.csv", index=False)
summary = (log_df.groupby(["file", "column", "problem", "quality_dimension", "severity", "proposed_rule"])
           .agg(rows=("row_key", "count"), example_keys=("row_key", lambda s: ", ".join(s.head(3))))
           .reset_index().sort_values(["file", "quality_dimension", "column"]))
summary.to_csv(OUT_DIR / "issues_log_summary.csv", index=False)

pd.set_option("display.width", 250)
pd.set_option("display.max_colwidth", 60)
print("\n=========== ISSUES SUMMARY ===========")
print(summary[["file", "column", "problem", "quality_dimension", "severity", "rows"]].to_string(index=False))
print("\nIssues per file:\n", log_df["file"].value_counts().to_string())

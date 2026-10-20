"""
include/nammamart_etl_transform.py
----------------------------------
Transform step of the ETL pipeline (pandas). Input: rows that passed validation (still text).
Output: typed, standardised, PII-masked tables ready for the star schema.

    build_dim_customer  -> etl_dim_customer   (email / phone / name masked, raw PII dropped)
    build_dim_product   -> etl_dim_product
    build_dim_store     -> etl_dim_store
    build_fact_orders   -> etl_fact_orders    (joined with products, derived measures)
    build_payments      -> etl_fact_payments  (used for reconciliation)
"""
import hashlib

import pandas as pd

import nammamart_common as nc
from nammamart_etl_checks import standard_payment_mode

FACT_BUSINESS_COLUMNS = [
    "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct",
    "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes",
]


def _clean(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def _or_none(s: pd.Series) -> pd.Series:
    """Blank text or a placeholder like 'N/A' -> None, so it is stored as NULL."""
    return _clean(s).where(~nc.is_missing(s), None)


def _date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(_clean(s), format=nc.DATE_FORMAT, errors="coerce")


def _row_hash(df: pd.DataFrame, columns: list) -> pd.Series:
    """Fingerprint of the business columns. Load uses it to tell 'changed' rows from 'unchanged' ones."""
    text = df[columns].astype(str).agg("|".join, axis=1)
    return text.map(lambda value: hashlib.md5(value.encode()).hexdigest())


def build_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    d = customers.copy()
    d["customer_id"] = _clean(d["customer_id"])
    d = d.drop_duplicates("customer_id", keep="first")   # safety net: validation already quarantined copies
    for col in ("full_name", "email", "phone"):
        d[col] = _or_none(d[col])                         # 'N/A' -> NULL before masking
    d["city"] = _or_none(d["city"]).fillna(nc.UNKNOWN)
    d["area"] = _or_none(d["area"])
    d["pincode"] = _or_none(d["pincode"])
    d["signup_date"] = _date(d["signup_date"])
    d["loyalty_tier"] = _clean(d["loyalty_tier"]).str.title()
    d["is_active"] = _clean(d["is_active"]).str.upper() == "Y"
    d = nc.mask_customer_pii(d)
    return d[["customer_id", "name_masked", "email_masked", "phone_masked", "city", "area", "pincode",
              "signup_date", "loyalty_tier", "is_active"]].reset_index(drop=True)


def build_dim_product(products: pd.DataFrame) -> pd.DataFrame:
    d = products.copy()
    d["product_id"] = _clean(d["product_id"])
    d = d.drop_duplicates("product_id", keep="first")
    for col in ("product_name", "category", "sub_category", "brand", "unit"):
        d[col] = _or_none(d[col])
    d["mrp"] = pd.to_numeric(d["mrp"])
    d["cost_price"] = pd.to_numeric(d["cost_price"])
    d["is_perishable"] = _clean(d["is_perishable"]).str.upper() == "TRUE"
    d["launch_date"] = _date(d["launch_date"])
    return d[["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit",
              "is_perishable", "launch_date"]].reset_index(drop=True)


def build_dim_store(stores: pd.DataFrame) -> pd.DataFrame:
    d = stores.copy()
    d["store_id"] = _clean(d["store_id"])
    d = d.drop_duplicates("store_id", keep="first")
    zone = _clean(d["zone"])
    d["zone"] = zone.where(zone.isin(nc.VALID_ZONES), nc.UNKNOWN)
    for col in ("store_name", "area", "pincode", "manager_name"):
        d[col] = _or_none(d[col])
    d["open_date"] = _date(d["open_date"])
    d["capacity_orders_per_day"] = pd.to_numeric(d["capacity_orders_per_day"], errors="coerce").astype("Int64")
    return d[["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
              "capacity_orders_per_day"]].reset_index(drop=True)


def build_fact_orders(order_frames: list, dim_product: pd.DataFrame) -> pd.DataFrame:
    """order_frames = passed rows of orders.csv first, then each increment file in date order.
    If the same order_id appears in a later file, the later row wins (e.g. DELIVERED -> RETURNED)."""
    o = pd.concat(order_frames, ignore_index=True)
    for col in ("order_id", "customer_id", "store_id", "product_id", "channel"):
        o[col] = _clean(o[col])
    o = o.drop_duplicates("order_id", keep="last")

    o["order_ts"] = pd.to_datetime(_clean(o["order_ts"]), format=nc.TIMESTAMP_FORMAT)
    o["quantity"] = pd.to_numeric(o["quantity"]).astype("int64")
    for col in ("unit_price", "discount_pct", "order_amount"):
        o[col] = pd.to_numeric(o[col]).astype("float64")
    o["delivery_minutes"] = pd.to_numeric(_or_none(o["delivery_minutes"])).astype("float64")
    o["payment_mode"] = standard_payment_mode(o["payment_mode"])
    o["order_status"] = _clean(o["order_status"]).str.upper()

    # Join with products to get cost_price (validation guarantees every product_id exists and is valid)
    o = o.merge(dim_product[["product_id", "cost_price"]], on="product_id", how="left", validate="many_to_one")

    delivered = o["order_status"] == nc.REVENUE_STATUS
    o["order_date"] = o["order_ts"].dt.normalize()
    o["net_revenue"] = o["order_amount"].where(delivered, 0.0).round(2)
    o["gross_margin"] = (o["order_amount"] - o["quantity"] * o["cost_price"]).where(delivered, 0.0).round(2)
    o["within_15_min"] = (o["delivery_minutes"] <= nc.SLA_MINUTES).where(delivered, pd.NA).astype("boolean")
    o["row_hash"] = _row_hash(o, FACT_BUSINESS_COLUMNS)
    o["source_row"] = pd.to_numeric(o["source_row"]).astype("int64")

    return o[["order_id", "order_ts", "order_date", "customer_id", "store_id", "product_id", "quantity",
              "unit_price", "discount_pct", "order_amount", "cost_price", "payment_mode", "order_status",
              "channel", "delivery_minutes", "net_revenue", "gross_margin", "within_15_min",
              "source_file", "source_row", "row_hash"]].reset_index(drop=True)


def build_payments(payments: pd.DataFrame) -> pd.DataFrame:
    p = payments.copy()
    p["payment_id"] = _clean(p["payment_id"])
    p["order_id"] = _clean(p["order_id"])
    p["payment_ts"] = pd.to_datetime(_clean(p["payment_ts"]), format=nc.TIMESTAMP_FORMAT)
    p["payment_mode"] = standard_payment_mode(p["payment_mode"])
    p["amount_paid"] = pd.to_numeric(p["amount_paid"]).astype("float64")
    p["payment_status"] = _clean(p["payment_status"]).str.upper()
    p["gateway_ref"] = _or_none(p["gateway_ref"])
    p["order_in_source"] = _clean(p["order_in_source"]).str.upper() == "TRUE"
    p["source_row"] = pd.to_numeric(p["source_row"]).astype("int64")
    return p[["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status",
              "gateway_ref", "order_in_source", "source_file", "source_row"]].reset_index(drop=True)

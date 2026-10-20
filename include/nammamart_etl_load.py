"""
include/nammamart_etl_load.py
-----------------------------
Load, Reconcile and Report steps of the ETL pipeline (DuckDB SQL).

Star schema in include/warehouse_<name>.duckdb:
    etl_dim_customer, etl_dim_product, etl_dim_store   full refresh every run (master files are full exports)
    etl_fact_orders                                     MERGE (upsert) on order_id, so re-runs never double-count
    etl_fact_payments                                   full refresh (used for reconciliation)
Reporting:
    etl_payment_reconciliation   one row per order/payment mismatch, with a reason
    etl_daily_zone_revenue       daily net revenue by zone and store
    etl_category_margin          revenue and gross margin by category
"""
import nammamart_common as nc

DDL = {
    "etl_dim_customer": """
        CREATE TABLE IF NOT EXISTS etl_dim_customer (
            customer_id   VARCHAR PRIMARY KEY,
            name_masked   VARCHAR,
            email_masked  VARCHAR,
            phone_masked  VARCHAR,
            city          VARCHAR,
            area          VARCHAR,
            pincode       VARCHAR,
            signup_date   DATE,
            loyalty_tier  VARCHAR,
            is_active     BOOLEAN
        )""",
    "etl_dim_product": """
        CREATE TABLE IF NOT EXISTS etl_dim_product (
            product_id     VARCHAR PRIMARY KEY,
            product_name   VARCHAR,
            category       VARCHAR,
            sub_category   VARCHAR,
            brand          VARCHAR,
            mrp            DOUBLE,
            cost_price     DOUBLE,
            unit           VARCHAR,
            is_perishable  BOOLEAN,
            launch_date    DATE
        )""",
    "etl_dim_store": """
        CREATE TABLE IF NOT EXISTS etl_dim_store (
            store_id                 VARCHAR PRIMARY KEY,
            store_name               VARCHAR,
            zone                     VARCHAR,
            area                     VARCHAR,
            pincode                  VARCHAR,
            open_date                DATE,
            manager_name             VARCHAR,
            capacity_orders_per_day  INTEGER
        )""",
    "etl_fact_orders": """
        CREATE TABLE IF NOT EXISTS etl_fact_orders (
            order_id          VARCHAR PRIMARY KEY,
            order_ts          TIMESTAMP,
            order_date        DATE,
            customer_id       VARCHAR,
            store_id          VARCHAR,
            product_id        VARCHAR,
            quantity          INTEGER,
            unit_price        DOUBLE,
            discount_pct      DOUBLE,
            order_amount      DOUBLE,
            cost_price        DOUBLE,
            payment_mode      VARCHAR,
            order_status      VARCHAR,
            channel           VARCHAR,
            delivery_minutes  DOUBLE,
            net_revenue       DOUBLE,
            gross_margin      DOUBLE,
            within_15_min     BOOLEAN,
            source_file       VARCHAR,
            source_row        INTEGER,
            row_hash          VARCHAR,
            run_id            VARCHAR,
            loaded_at         TIMESTAMP
        )""",
    "etl_fact_payments": """
        CREATE TABLE IF NOT EXISTS etl_fact_payments (
            payment_id      VARCHAR PRIMARY KEY,
            order_id        VARCHAR,
            payment_ts      TIMESTAMP,
            payment_mode    VARCHAR,
            amount_paid     DOUBLE,
            payment_status  VARCHAR,
            gateway_ref     VARCHAR,
            order_in_source BOOLEAN,
            source_file     VARCHAR,
            source_row      INTEGER
        )""",
}

FULL_REFRESH_TABLES = ("etl_dim_customer", "etl_dim_product", "etl_dim_store", "etl_fact_payments")


def load_star_schema(con, parquet_paths: dict, run_id: str) -> dict:
    """Load all tables in ONE transaction: either everything is loaded or nothing changes.

    parquet_paths = {"etl_dim_customer": path, ..., "etl_fact_orders": path, "etl_fact_payments": path}
    """
    loaded_at = nc.utc_now()
    counts = {}
    con.execute("BEGIN TRANSACTION")
    try:
        for table, ddl in DDL.items():
            con.execute(ddl)

        # Dimensions + payments: replace the contents (BY NAME matches columns by name, casts types)
        for table in FULL_REFRESH_TABLES:
            con.execute(f"DELETE FROM {table}")
            con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM read_parquet(?)", [str(parquet_paths[table])])
            counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]

        # Fact orders: upsert. New order_id -> insert. Same order_id with changed values -> update.
        # Same order_id with identical values -> untouched (so a re-run changes nothing).
        con.execute("""
            CREATE OR REPLACE TEMP TABLE new_orders AS
            SELECT * EXCLUDE (order_ts, order_date),
                   CAST(order_ts AS TIMESTAMP) AS order_ts,
                   CAST(order_date AS DATE)    AS order_date,
                   ? AS run_id,
                   CAST(? AS TIMESTAMP) AS loaded_at
            FROM read_parquet(?)""", [run_id, loaded_at, str(parquet_paths["etl_fact_orders"])])
        inserted = con.execute("""
            SELECT count(*) FROM new_orders n
            WHERE NOT EXISTS (SELECT 1 FROM etl_fact_orders f WHERE f.order_id = n.order_id)""").fetchone()[0]
        updated = con.execute("""
            SELECT count(*) FROM new_orders n JOIN etl_fact_orders f USING (order_id)
            WHERE f.row_hash <> n.row_hash""").fetchone()[0]
        update_columns = [c for c in _columns(con, "etl_fact_orders") if c != "order_id"]
        con.execute(f"""
            MERGE INTO etl_fact_orders AS f
            USING new_orders AS n
            ON f.order_id = n.order_id
            WHEN MATCHED AND f.row_hash <> n.row_hash THEN
                UPDATE SET {", ".join(f"{c} = n.{c}" for c in update_columns)}
            WHEN NOT MATCHED THEN
                INSERT BY NAME""")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    counts["etl_fact_orders"] = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
    counts["fact_rows_in_batch"] = con.execute("SELECT count(*) FROM new_orders").fetchone()[0]
    counts["fact_inserted"] = inserted
    counts["fact_updated"] = updated
    counts["fact_unchanged"] = counts["fact_rows_in_batch"] - inserted - updated
    return counts


def _columns(con, table: str) -> list:
    return [row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()]


RECONCILIATION_SQL = f"""
CREATE OR REPLACE TABLE etl_payment_reconciliation AS
WITH joined AS (
    SELECT
        coalesce(o.order_id, p.order_id) AS order_id,
        p.payment_id,
        o.order_status,
        p.payment_status,
        o.order_amount,
        p.amount_paid,
        o.payment_mode AS order_payment_mode,
        p.payment_mode AS gateway_payment_mode,
        o.order_id IS NOT NULL AS has_order,
        p.payment_id IS NOT NULL AS has_payment,
        coalesce(p.order_in_source, false) AS order_in_source
    FROM etl_fact_orders o
    FULL OUTER JOIN etl_fact_payments p ON o.order_id = p.order_id
),
mismatches AS (
    SELECT *, 'ORDER_WITHOUT_PAYMENT' AS reason, order_amount AS amount_at_risk
    FROM joined WHERE has_order AND NOT has_payment AND order_status IN ('DELIVERED', 'RETURNED')
    UNION ALL
    SELECT *, 'PAYMENT_FAILED', order_amount
    FROM joined WHERE order_status = 'DELIVERED' AND payment_status = 'FAILED'
    UNION ALL
    SELECT *, 'AMOUNT_MISMATCH', abs(order_amount - amount_paid)
    FROM joined WHERE order_status = 'DELIVERED' AND payment_status = 'SUCCESS'
                  AND abs(order_amount - amount_paid) > {nc.AMOUNT_TOLERANCE}
    UNION ALL
    SELECT *, 'REFUNDED_BUT_DELIVERED', amount_paid
    FROM joined WHERE order_status = 'DELIVERED' AND payment_status = 'REFUNDED'
    UNION ALL
    SELECT *, 'RETURNED_NOT_REFUNDED', amount_paid
    FROM joined WHERE order_status = 'RETURNED' AND payment_status = 'SUCCESS'
    UNION ALL
    SELECT *, 'PAID_BUT_CANCELLED', amount_paid
    FROM joined WHERE order_status = 'CANCELLED' AND payment_status = 'SUCCESS'
    UNION ALL
    SELECT *, 'PAYMENT_FOR_UNKNOWN_ORDER', amount_paid
    FROM joined WHERE has_payment AND NOT has_order AND NOT order_in_source
    UNION ALL
    -- the order exists in the source file but was quarantined, so it is not in revenue yet
    SELECT *, 'PAYMENT_FOR_QUARANTINED_ORDER', amount_paid
    FROM joined WHERE has_payment AND NOT has_order AND order_in_source
    UNION ALL
    SELECT *, 'PAYMENT_MODE_MISMATCH', 0
    FROM joined WHERE has_order AND has_payment AND order_payment_mode <> gateway_payment_mode
)
SELECT order_id, payment_id, reason, round(amount_at_risk, 2) AS amount_at_risk,
       order_status, payment_status, order_amount, amount_paid, order_payment_mode, gateway_payment_mode
FROM mismatches
ORDER BY reason, order_id
"""


def reconcile(con) -> dict:
    con.execute(RECONCILIATION_SQL)
    summary = con.execute("""
        SELECT reason, count(*) AS rows, round(sum(amount_at_risk), 2) AS amount_at_risk
        FROM etl_payment_reconciliation GROUP BY reason ORDER BY rows DESC""").df()
    print("[Reconcile] Mismatches by reason:\n" + summary.to_string(index=False))
    return {
        "mismatches": int(summary["rows"].sum()),
        "amount_at_risk": float(summary["amount_at_risk"].sum()),
        "by_reason": dict(zip(summary["reason"], summary["rows"].astype(int))),
    }


REPORT_SQL = {
    "etl_daily_zone_revenue": """
        CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
        SELECT f.order_date, s.zone, f.store_id, s.store_name,
               count(*)                     AS delivered_orders,
               round(sum(f.net_revenue), 2) AS net_revenue
        FROM etl_fact_orders f
        JOIN etl_dim_store s USING (store_id)
        WHERE f.order_status = 'DELIVERED'
        GROUP BY ALL
        ORDER BY f.order_date, s.zone, f.store_id""",
    "etl_category_margin": """
        CREATE OR REPLACE TABLE etl_category_margin AS
        SELECT p.category,
               count(*)                                   AS delivered_orders,
               sum(f.quantity)                            AS units,
               round(sum(f.net_revenue), 2)               AS revenue,
               round(sum(f.quantity * f.cost_price), 2)   AS cost,
               round(sum(f.gross_margin), 2)              AS gross_margin,
               round(100 * sum(f.gross_margin) / nullif(sum(f.net_revenue), 0), 2) AS margin_pct
        FROM etl_fact_orders f
        JOIN etl_dim_product p USING (product_id)
        WHERE f.order_status = 'DELIVERED'
        GROUP BY ALL
        ORDER BY revenue DESC""",
}


def build_reports(con) -> dict:
    for sql in REPORT_SQL.values():
        con.execute(sql)
    zone = con.execute("""
        SELECT zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
        FROM etl_daily_zone_revenue GROUP BY zone ORDER BY net_revenue DESC""").df()
    category = con.execute("SELECT * FROM etl_category_margin").df()
    print("[Report] Net revenue by zone (all days):\n" + zone.to_string(index=False))
    print("[Report] etl_category_margin:\n" + category.to_string(index=False))
    return {
        "total_net_revenue": float(zone["net_revenue"].sum()),
        "zone_revenue_rows": con.execute("SELECT count(*) FROM etl_daily_zone_revenue").fetchone()[0],
        "category_rows": len(category),
    }

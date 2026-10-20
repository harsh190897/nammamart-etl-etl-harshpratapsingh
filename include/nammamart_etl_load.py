"""
include/nammamart_etl_load.py
-----------------------------
Load step of the ETL pipeline (DuckDB SQL). Reconcile and Report are added in A.4.

Star schema in include/warehouse_<name>.duckdb:
    etl_dim_customer, etl_dim_product, etl_dim_store   full refresh every run (master files are full exports)
    etl_fact_orders                                     MERGE (upsert) on order_id, so re-runs never double-count
    etl_fact_payments                                   full refresh (used for reconciliation)
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

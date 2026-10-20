# NammaMart Quick Commerce: ETL & ELT Pipeline Submission

**Learner:** Harsh Pratap Singh
**Module:** FDAIE Module 2, Data Engineering Foundations
**Repository:** https://github.com/harsh190897/nammamart-etl-etl-harshpratapsingh
**Assignment brief:** [assignment/ASSIGNMENT.md](assignment/ASSIGNMENT.md)

---

## Submission checklist

Status of every item in section 6 of the assignment brief. ✅ done · 🟡 in progress · ⬜ not started

| # | Checklist item (ASSIGNMENT.md §6) | Status | Where |
|---|---|---|---|
| 1 | `dags/nammamart_etl_harshpratapsingh.py`, parsing with no import errors | ✅ Complete (16 tasks), no import errors | [dags/nammamart_etl_harshpratapsingh.py](dags/nammamart_etl_harshpratapsingh.py) |
| 2 | `dags/nammamart_elt_harshpratapsingh.py`, parsing with no import errors | ✅ Complete (19 tasks: 5 extracts, raw, staging, marts, run audit), no import errors | [dags/nammamart_elt_harshpratapsingh.py](dags/nammamart_elt_harshpratapsingh.py) |
| 3 | Helper code in `include/` (quality checks, masking functions) | ✅ ETL helpers (quality rules, masking, transform, load, reconciliation, reports, audit tables) and ELT helpers (SQL runner, rule variables) | [include/nammamart_common.py](include/nammamart_common.py), [include/nammamart_elt.py](include/nammamart_elt.py), [include/nammamart_etl_checks.py](include/nammamart_etl_checks.py), [include/nammamart_etl_transform.py](include/nammamart_etl_transform.py), [include/nammamart_etl_load.py](include/nammamart_etl_load.py) |
| 4 | SQL files in `include/sql/` | ✅ Raw, staging, data quality and mart SQL (16 files) | [include/sql/](include/sql/) |
| 5 | `SUBMISSION.md`: Data Issues Log (Part 0) | ✅ | [Part 0](#part-0-discovery-and-data-profiling) |
| 6 | `SUBMISSION.md`: answers to Part C | ✅ Answered, with schedule screenshot | [Part C](#part-c-scheduling) |
| 7 | `SUBMISSION.md`: governance answers (Part E) | ⬜ | [Part E](#part-e-data-governance) |
| 8 | `SUBMISSION.md`: observability results table (Part F) | ⬜ | [Part F](#part-f-observability-tests) |
| 9 | `SUBMISSION.md`: recommendation (Part G) | ⬜ | [Part G](#part-g-recommendation-to-the-cto) |
| 10 | Screenshots: both DAGs green in Graph view, 9:00 AM IST schedule, one quarantine file, `dq_results`, `pipeline_runs`, payment reconciliation | ✅ 7 of 7 | [Screenshots](#screenshots) |
| 11 | Six business questions, SQL + result, from both pipelines | ✅ All six answered from both pipelines; results identical | [Business questions](#business-questions) |
| 12 | No API keys, passwords, webhook URLs or unmasked customer data outside `include/data/` | ✅ Checked on every commit | [.gitignore](.gitignore) |

## Contents

- [Part 0: Discovery and data profiling](#part-0-discovery-and-data-profiling)
- [Part A: ETL pipeline](#part-a-etl-pipeline)
- [Part B: ELT pipeline](#part-b-elt-pipeline)
- [Part C: Scheduling](#part-c-scheduling)
- [Part D: Data quality dimensions](#part-d-data-quality-dimensions)
- [Business questions](#business-questions)
- [Part E: Data governance](#part-e-data-governance)
- [Part F: Observability tests](#part-f-observability-tests)
- [Part G: Recommendation to the CTO](#part-g-recommendation-to-the-cto)
- [Screenshots](#screenshots)

---

## Part 0: Discovery and data profiling

### 0.1 Approach and how to reproduce

Before writing any pipeline code, every file was profiled in plain Python (outside Airflow), so that the quality rules used later in the ETL and ELT pipelines come from evidence, not guesses.

- Every CSV is read **as text** (`dtype=str, keep_default_na=False`) so that blanks, placeholders, bad numbers and bad dates are seen exactly as the client sent them.
- Timeliness checks use a fixed "as of" date of **2026-10-02** (the day after the increment file), so results are repeatable.

| Script | Purpose |
|---|---|
| [`profiling/common.py`](profiling/common.py) | Shared file paths and a text-only CSV loader |
| [`profiling/profile_files.py`](profiling/profile_files.py) | Row counts, blanks per column, duplicate keys, min/max of numeric and date columns, distinct values of category columns |
| [`profiling/find_issues.py`](profiling/find_issues.py) | Referential-integrity and business-rule checks; writes the Data Issues Log |
| [`profiling/output/issues_log_summary.csv`](profiling/output/issues_log_summary.csv) | One row per issue type (the table in 0.5) |
| [`profiling/output/issues_log_detail.csv`](profiling/output/issues_log_detail.csv) | One row per bad record (row-level evidence) |

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install pandas duckdb
python profiling/profile_files.py > profiling/profile_output.txt   # not committed: contains raw personal data
python profiling/find_issues.py
```

Row keys in the log are written as `<id>@row<N>`, where `N` is the line number in the CSV (line 1 is the header). This identifies the exact row even when the same ID appears twice.

### 0.2 Profiling summary

| File | Rows | Cols | Duplicate keys | Key profiling findings |
|---|---|---|---|---|
| `orders.csv` | 2,412 | 13 | 12 `order_id` (all exact copies) | `quantity` min −3; `discount_pct` max 250; `order_amount` min −1,593; `delivery_minutes` max 1,440; `order_ts` up to 2027-01-01 and unparseable values (`2026-13-05 11:00:00`, `31/09/2026 10:00`); status values `Pending??`, `delivered`; payment modes `upi`, `UPI ` (trailing space), `CARD`, `cod`, `Cashh`; 18 blank `customer_id`, 2 blank `order_ts`, 164 blank `delivery_minutes` |
| `payments.csv` | 2,228 | 7 | 4 `payment_id` (all exact copies) | `amount_paid` min −1,593; `payment_ts` up to 2027-01-01; same payment-mode variants as orders; 301 blank `gateway_ref` |
| `customers.csv` | 405 | 10 | 5 `customer_id` (rows differ in phone/area) | `loyalty_tier` in mixed case (`BRONZE`, `bronze`, `SILVER`); `pincode` values `0` and `5600XX`; `signup_date` up to 2027-01-05; 4 blank `email`, 6 blank `city` |
| `products.csv` | 152 | 10 | 2 `product_id` (`(old)` copies with a different `mrp`) | `mrp` min −203; 3 blank `category` |
| `stores.csv` | 15 | 8 | none | 1 blank `zone`; `capacity_orders_per_day` min 0 |
| `orders_2026-10-01_increment.csv` | 160 | 13 | none | Clean: no blanks, all values in range. 10 rows are corrections to existing orders (DELIVERED → RETURNED, amount unchanged); 150 are new orders |

### 0.3 Referential integrity

| Relationship | Result |
|---|---|
| `orders.customer_id` → `customers.customer_id` | 18 orders have a blank `customer_id`; every non-blank ID exists |
| `orders.product_id` → `products.product_id` | 9 orders reference a product that does not exist |
| `orders.store_id` → `stores.store_id` | 5 orders reference a store that does not exist |
| `payments.order_id` → `orders.order_id` | 8 payments reference an order that does not exist; in the other direction, 28 DELIVERED/RETURNED orders have no payment record. No CANCELLED order has a payment, as expected |

### 0.4 Business-rule checks

| Rule from the client | Result |
|---|---|
| `order_amount = quantity × unit_price × (1 − discount_pct/100)`, rounded to 2 decimals | 18 orders do not match (tolerance ₹0.01) |
| Valid `order_status`: DELIVERED, CANCELLED, RETURNED | 3 invalid (`Pending??`); 2 valid but lower case (`delivered`) |
| Valid `payment_mode`: UPI, Card, Cash on Delivery, Wallet | 2 unrecognised (`Cashh`) in each of orders and payments; 8 fixable variants in each (case, trailing space, `cod`) |
| Deliveries take 8–35 minutes; DELIVERED orders must have `delivery_minutes` | 6 orders above 120 minutes (up to 1,440); 8 DELIVERED orders with no delivery time; no values between 35 and 120 or below 8 |
| A SUCCESS payment must equal its order's `order_amount` | 14 SUCCESS payments differ from the order amount; 6 payments have a negative `amount_paid` |
| Increment file contains new orders and corrections | Confirmed: 150 new + 10 status corrections, no quality issues |

### 0.5 Data Issues Log

40 issue types and 259 affected records, across all five data files (orders 151, payments 51, customers 44, products 11, stores 2). Example keys only are listed here; the full row-level list is in [`issues_log_detail.csv`](profiling/output/issues_log_detail.csv).

**Critical** = the row is quarantined. **Warning** = the row is loaded (after standardisation where possible) and the issue is logged.

| # | File | Row key (examples) | Column | Problem | Quality dimension | Proposed rule | Severity | Rows |
|---|---|---|---|---|---|---|---|---|
| 1 | `orders.csv` | ORD002255@row36, ORD000701@row251, ORD000997@row550 | `customer_id` | Missing customer_id | Completeness | customer_id NOT NULL | Critical | 18 |
| 2 | `orders.csv` | ORD001554@row162, ORD001800@row253, ORD000731@row652 | `delivery_minutes` | DELIVERED order without delivery_minutes | Completeness | DELIVERED => delivery_minutes NOT NULL | Critical | 8 |
| 3 | `orders.csv` | ORD000403@row273, ORD000362@row899 | `order_ts` | Missing order_ts | Completeness | order_ts NOT NULL | Critical | 2 |
| 4 | `orders.csv` | ORD001462@row271, ORD001441@row1121, ORD001692@row1318 | `order_id` | Exact duplicate row | Uniqueness | Drop repeated copy, keep first | Critical | 12 |
| 5 | `orders.csv` | ORD002103@row66, ORD001468@row2131, ORD001237@row2292 | `discount_pct` | Discount outside 0-100 | Validity | 0 <= discount_pct <= 100 | Critical | 3 |
| 6 | `orders.csv` | ORD000500@row747, ORD001970@row2027, ORD001637@row2129 | `order_status` | Invalid status (e.g. 'Pending??') | Validity | status IN (DELIVERED, CANCELLED, RETURNED) | Critical | 3 |
| 7 | `orders.csv` | ORD001610@row513, ORD000534@row825, ORD002012@row1100 | `order_ts` | Unparseable timestamp (bad format or month) | Validity | order_ts must parse as timestamp | Critical | 4 |
| 8 | `orders.csv` | ORD001812@row48, ORD001414@row563 | `payment_mode` | Unrecognised payment_mode (e.g. 'Cashh') | Validity | Quarantine or ask client | Critical | 2 |
| 9 | `orders.csv` | ORD001066@row143, ORD000153@row1881 | `order_status` | Valid status but wrong casing (e.g. 'delivered') | Validity | TRIM + UPPER before validating | Warning | 2 |
| 10 | `orders.csv` | ORD000159@row886, ORD001364@row1473, ORD000433@row2003 | `payment_mode` | Mode needs standardising (case/space/'cod') | Validity | Map to UPI/Card/Cash on Delivery/Wallet | Warning | 8 |
| 11 | `orders.csv` | ORD001037@row275, ORD002101@row511, ORD000455@row626 | `delivery_minutes` | Impossible delivery time (> 120 min) | Accuracy | delivery_minutes < 120 | Critical | 6 |
| 12 | `orders.csv` | ORD001296@row801, ORD001459@row816, ORD000867@row860 | `order_amount` | Negative order_amount | Accuracy | order_amount >= 0 | Critical | 6 |
| 13 | `orders.csv` | ORD002103@row66, ORD002305@row312, ORD002004@row480 | `order_amount` | order_amount != qty x price x (1-disc/100) | Accuracy | Recompute and compare (tolerance 0.01) | Critical | 18 |
| 14 | `orders.csv` | ORD000469@row127, ORD001296@row801, ORD001459@row816 | `quantity` | Quantity zero or negative | Accuracy | quantity > 0 | Critical | 10 |
| 15 | `orders.csv` | ORD000986@row207, ORD001058@row305, ORD001685@row590 | `product_id` | product_id not found in products | Consistency | FK product_id -> products | Critical | 9 |
| 16 | `orders.csv` | ORD001799@row735, ORD001046@row742, ORD001492@row831 | `store_id` | store_id not found in stores | Consistency | FK store_id -> stores | Critical | 5 |
| 17 | `orders.csv` | ORD001113, ORD000737, ORD001398 | `order_id` | DELIVERED/RETURNED order has no payment record | Consistency | Goes to reconciliation table | Warning | 28 |
| 18 | `orders.csv` | ORD002119@row140, ORD001693@row288, ORD001089@row363 | `order_ts` | order_ts in the future (after 2026-10-02) | Timeliness | order_ts <= run time | Critical | 7 |
| 19 | `payments.csv` | PAY0000509@row1032, PAY0001651@row1855, PAY0002000@row1877 | `payment_id` | Exact duplicate row | Uniqueness | Drop repeated copy, keep first | Critical | 4 |
| 20 | `payments.csv` | PAY0000522@row1454, PAY0000044@row1625 | `payment_mode` | Unrecognised payment_mode | Validity | Quarantine or ask client | Critical | 2 |
| 21 | `payments.csv` | PAY0002188@row96, PAY0001850@row609, PAY0001954@row822 | `payment_mode` | Mode needs standardising | Validity | Map to standard names | Warning | 8 |
| 22 | `payments.csv` | PAY0001568@row71, PAY0002175@row346, PAY0000751@row434 | `amount_paid` | Negative amount_paid | Accuracy | amount_paid >= 0 (refunds should be status REFUNDED) | Critical | 6 |
| 23 | `payments.csv` | PAY0001568@ORD001031, PAY0000354@ORD001937, PAY0000779@ORD001840 | `amount_paid` | SUCCESS payment amount != order_amount | Consistency | Report in the payment reconciliation table (AMOUNT_MISMATCH), see 0.7 | Warning | 14 |
| 24 | `payments.csv` | PAY0002219@row188, PAY0002220@row689, PAY0002223@row966 | `order_id` | Payment for an order that does not exist | Consistency | FK order_id -> orders; report in the payment reconciliation table, see 0.7 | Warning | 8 |
| 25 | `payments.csv` | PAY0000522, PAY0000044 | `payment_mode` | Payment mode differs from the order's mode | Consistency | Modes should match | Warning | 2 |
| 26 | `payments.csv` | PAY0000396@row146, PAY0000127@row309, PAY0001908@row411 | `payment_ts` | payment_ts in the future | Timeliness | payment_ts <= run time | Critical | 7 |
| 27 | `customers.csv` | C00355@row4, C00394@row202, C00233@row276 | `city` | Missing city | Completeness | Warn | Warning | 6 |
| 28 | `customers.csv` | C00337@row17, C00045@row81, C00327@row199 | `email` | Missing email | Completeness | Warn; mask nothing | Warning | 4 |
| 29 | `customers.csv` | C00103@row46, C00252@row215, C00226@row246 | `customer_id` | Duplicate customer_id (rows differ in phone/area) | Uniqueness | One row per customer_id; keep latest/first, log conflict | Critical | 5 |
| 30 | `customers.csv` | C00307@row24, C00063@row79, C00363@row82 | `email` | Invalid email format | Validity | Email regex | Warning | 8 |
| 31 | `customers.csv` | C00151@row60, C00240@row117, C00095@row141 | `phone` | Phone is not 10 digits | Validity | phone = 10 digits | Warning | 7 |
| 32 | `customers.csv` | C00132@row38, C00083@row109, C00166@row151 | `pincode` | Pincode is not 6 digits | Validity | pincode = 6 digits | Warning | 3 |
| 33 | `customers.csv` | C00199@row2, C00171@row40, C00133@row43 | `loyalty_tier` | Inconsistent casing (BRONZE/bronze/SILVER) | Consistency | Standardise to Title case | Warning | 8 |
| 34 | `customers.csv` | C00400@row86, C00398@row197, C00038@row239 | `signup_date` | signup_date in the future | Timeliness | signup_date <= run time | Critical | 3 |
| 35 | `products.csv` | P0051@row52, P0060@row61, P0068@row69 | `category` | Missing category | Completeness | category NOT NULL | Critical | 3 |
| 36 | `products.csv` | P0030@row152, P0140@row153 | `product_id` | Duplicate product_id ('(old)' rows with different mrp) | Uniqueness | One row per product_id; drop '(old)' version | Critical | 2 |
| 37 | `products.csv` | P0019@row20, P0073@row74, P0074@row75 | `cost_price` | cost_price greater than mrp | Accuracy | cost_price <= mrp | Critical | 5 |
| 38 | `products.csv` | P0109@row110 | `mrp` | mrp zero or negative | Accuracy | mrp > 0 | Critical | 1 |
| 39 | `stores.csv` | S07@row8 | `zone` | Missing zone | Completeness | Keep the store; show zone as 'Unknown' and log it, see 0.7 | Warning | 1 |
| 40 | `stores.csv` | S12@row13 | `capacity_orders_per_day` | Capacity is zero | Accuracy | capacity > 0 | Warning | 1 |

### 0.6 Questions for the client before building

1. **Duplicate records with different content.** "Five customers appear twice with the same `customer_id` but a different phone number and area, and two products have an `(old)` copy with a different MRP. Which version is correct: the latest one, the first one, or is there an 'active' flag we should use?"
2. **Negative quantities and negative payments.** "Some orders have quantity 0 or negative and a negative order amount, and some payments have a negative `amount_paid`, yet most are marked DELIVERED. Are these returns or refunds that should be recorded as `RETURNED`/`REFUNDED`, or data-entry errors to reject?"
3. **Orders and payments that don't match.** "28 delivered or returned orders have no payment, 8 payments point to orders we can't find, and 14 successful payments differ from the order amount. Is the gateway export incomplete or delayed, or are these real collection problems finance needs to chase? And should a mismatched order still count as revenue?"

### 0.7 Severity changes made when designing the pipelines

Three proposed severities in the log above were changed from Critical to Warning while designing the pipelines (the rows are already updated). Each one would otherwise have removed valid data from the finance tables:

| Issue | First proposal | Final rule | Reason |
|---|---|---|---|
| `stores.csv`: S07 has no `zone` | Critical | Warning; zone shown as `Unknown` | S07 is a real store with 176 orders (about 7% of all orders). Quarantining it would hide all of them from revenue; only the zone label is missing |
| `payments.csv`: payment for an order that does not exist | Critical | Warning; listed in the payment reconciliation table | The payment record itself is valid. The mismatch is a finance question, and the reconciliation table is where finance looks for it |
| `payments.csv`: SUCCESS payment ≠ order amount | Critical | Listed in the payment reconciliation table | It needs both files to detect, so it is a reconciliation finding, not a fault in one row |

---

## Part A: ETL pipeline

**Status:** ✅ complete (A.1–A.4).

**DAG:** [`dags/nammamart_etl_harshpratapsingh.py`](dags/nammamart_etl_harshpratapsingh.py), flow **Extract → Validate → Transform → Load**.
**Helper code:** [`include/nammamart_common.py`](include/nammamart_common.py) (paths, business rules, DuckDB connection, masking, audit tables), [`include/nammamart_etl_checks.py`](include/nammamart_etl_checks.py) (data quality rules in pandas), [`include/nammamart_etl_transform.py`](include/nammamart_etl_transform.py) (standardise, mask, join, derive in pandas), [`include/nammamart_etl_load.py`](include/nammamart_etl_load.py) (star schema load, reconciliation and reports in DuckDB SQL).

| Step | Status | Tasks |
|---|---|---|
| A.1 Extract | ✅ | `extract_customers`, `extract_products`, `extract_stores`, `extract_orders`, `extract_payments` (run in parallel) |
| A.2 Validate + quarantine | ✅ | `validate_customers`, `validate_products`, `validate_stores`, `validate_orders`, `validate_payments`, `quarantine_and_log_dq` |
| A.3 Transform + load (star schema) | ✅ | `transform`, `load` |
| A.4 Reconcile, report, run audit | ✅ | `reconcile`, `report`, `audit_run` |

```
extract_customers ─► validate_customers ─┐
extract_products  ─► validate_products  ─┼─► validate_orders ─┬─► quarantine_and_log_dq ─┐
extract_stores    ─► validate_stores    ─┘         ▲          │                          ├─► load ─► reconcile ─► report ─► audit_run
extract_orders    ─────────────────────────────────┘          └─► transform ─────────────┘
extract_payments  ─► validate_payments (also uses extract_orders)
```

Green run in the Airflow UI (Graph view, all 16 tasks `success`):

![ETL DAG in Graph view, green run](docs/screenshots/01_etl_graph_green.png)

**Extract (A.1).** Each task copies its source file into a per-run folder (`include/tmp/harshpratapsingh/<run_id>/extract/`), reads it as text, checks the header against the expected columns, and passes only the file path and row count to the next task. `extract_orders` also picks up every next-day file matching `orders_*_increment.csv`.

**Validate (A.2).** Every file is checked row by row with the rules in [Part D](#part-d-data-quality-dimensions). A row that fails any **Critical** check is set aside with a `failure_reason` listing every rule it broke; **Warning** checks are only counted. The three master files are validated first, because an order is only valid if its customer, product and store exist **and** passed their own checks. Payments only need the order IDs from the orders files, so `validate_payments` runs in parallel with the master validations.

**Quarantine (A.2).** `quarantine_and_log_dq` writes the failed rows of each file, exactly as received plus `source_file`, `source_row` and `failure_reason`, to `include/quarantine/harshpratapsingh/<file>_failed_<run_id>.csv`, and writes every check to `dq_results`. It is the only task in this step that writes to the warehouse, so DuckDB never has two writers.

Example quarantine file (`orders_failed_<run_id>.csv`; the last column is `failure_reason`):

![Quarantine file for orders.csv with the failure_reason column](docs/screenshots/04_quarantine_file.png)

Result of a run on 2026-10-20:

| File | Rows | Passed | Quarantined | Main reasons |
|---|---|---|---|---|
| `customers.csv` | 405 | 397 | 8 | 5 duplicate `customer_id`, 3 future `signup_date` |
| `products.csv` | 152 | 142 | 10 | 2 duplicate `(old)` rows, 3 missing category, 5 cost > mrp, 1 mrp ≤ 0 |
| `stores.csv` | 15 | 15 | 0 | S07's missing zone is a Warning (see 0.7) |
| `orders.csv` | 2,412 | 2,177 | 235 | 126 point to a quarantined product, 18 missing customer, 18 amount ≠ formula, 12 duplicates, … |
| `orders_2026-10-01_increment.csv` | 160 | 145 | 15 | 14 point to a quarantined product, 1 to a quarantined customer |
| `payments.csv` | 2,228 | 2,209 | 19 | 4 duplicates, 6 negative amounts, 7 future-dated, 2 unknown mode `Cashh` |

**Transform (A.3, pandas).** Runs on the passed rows only, in parallel with `quarantine_and_log_dq`:

- **Standardise:** trim every value; `order_status` upper case; `loyalty_tier` title case; payment modes mapped (`upi`, `UPI `, `CARD`, `cod` → `UPI`, `UPI`, `Card`, `Cash on Delivery`); placeholders such as `N/A` become NULL; a missing store zone or customer city becomes `Unknown`.
- **Deduplicate:** one row per `customer_id`, `product_id` and `store_id` (validation already quarantined the later copies; the transform step is a safety net, and every dimension also has a `PRIMARY KEY`).
- **Mask PII:** `email_masked` (`pr***@example.com`), `phone_masked` (`98******21`) and `name_masked` (`D*** D***`) are added and the raw `email`, `phone` and `full_name` columns are dropped before anything is written.
- **Join and derive:** orders are joined to products for `cost_price`, then `order_date`, `net_revenue` (`order_amount` for DELIVERED orders, else 0), `gross_margin` (`net_revenue − quantity × cost_price`) and `within_15_min` (DELIVERED only) are derived.
- **Next-day files:** orders from `orders.csv` and each increment file are combined in date order and the **latest version of each `order_id` wins**, so a correction such as DELIVERED → RETURNED replaces the old row. A fingerprint (`row_hash`) of the business columns is stored for the load step.

The typed tables are handed to `load` as Parquet files in the run folder.

**Load (A.3, DuckDB) into a star schema** in `include/warehouse_harshpratapsingh.duckdb`, inside **one transaction** (all tables or nothing):

| Table | Grain | Load method |
|---|---|---|
| `etl_fact_orders` | one row per order | **MERGE on `order_id`**: new order → insert; same order with a different `row_hash` → update; identical → untouched |
| `etl_dim_customer` | one row per customer (masked) | full refresh (the master file is a full export) |
| `etl_dim_product` | one row per product | full refresh |
| `etl_dim_store` | one row per store | full refresh |
| `etl_fact_payments` | one row per payment (used for reconciliation) | full refresh |

Every fact row keeps `source_file`, `source_row`, `run_id` and `loaded_at`, so any number can be traced back to the exact CSV line and the run that loaded it. The `load` task declares the five tables as **Airflow Assets** (group `nammamart_etl`), so the Assets page shows each table, the task that produces it and the time of its last successful load.

Results of runs on 2026-10-20:

| Run | `load` log | `etl_fact_orders` rows |
|---|---|---|
| First run | 2,313 inserted, 0 updated, 0 unchanged | 2,313 |
| Re-run with the same files (idempotency) | **0 inserted, 0 updated, 2,313 unchanged** | 2,313 |
| Day 1: `orders.csv` only (test) | 2,177 inserted | 2,177 |
| Day 2: increment file added (test) | **136 inserted, 9 updated, 2,168 unchanged** | 2,313 |

Of the 10 corrections in the increment file, 9 were applied; the tenth points to a quarantined product and is in the increment's quarantine file. Table sizes: `etl_dim_customer` 397, `etl_dim_product` 142, `etl_dim_store` 15, `etl_fact_payments` 2,209. `etl_fact_orders` holds 2,091 DELIVERED orders (net revenue ₹1,128,410.45), 147 CANCELLED and 75 RETURNED.

**Reconcile (A.4).** `reconcile` compares every order in `etl_fact_orders` with every payment in `etl_fact_payments` (a full outer join on `order_id`) and rebuilds `etl_payment_reconciliation` on every run: one row per mismatch, with a `reason` and the `amount_at_risk`.

| Reason | Rule | Amount at risk | Rows (2026-10-20) | ₹ at risk |
|---|---|---|---|---|
| `PAYMENT_FOR_QUARANTINED_ORDER` | Payment whose order exists in the source file but was quarantined | amount paid | 199 | 152,961.48 |
| `ORDER_WITHOUT_PAYMENT` | DELIVERED or RETURNED order with no payment | order amount | 164 | 89,441.50 |
| `PAYMENT_FAILED` | DELIVERED order whose payment FAILED | order amount | 42 | 25,722.25 |
| `AMOUNT_MISMATCH` | SUCCESS payment ≠ order amount (more than ₹0.01) | difference | 11 | 560.00 |
| `RETURNED_NOT_REFUNDED` | RETURNED order whose payment is still SUCCESS | amount paid | 9 | 3,662.70 |
| `PAYMENT_FOR_UNKNOWN_ORDER` | Payment whose `order_id` is in no orders file | amount paid | 8 | 4,580.57 |
| `REFUNDED_BUT_DELIVERED`, `PAID_BUT_CANCELLED`, `PAYMENT_MODE_MISMATCH` | Checked on every run | | 0 | 0 |

Of the 164 orders without a payment, 28 are September orders (the same 28 found in profiling) and 136 are new orders from 1 October: the payments export ends at 2026-09-30 23:47, so their payments have not arrived yet. The 9 `RETURNED_NOT_REFUNDED` rows are exactly the 9 DELIVERED → RETURNED corrections applied from the increment file. Their payments are still `SUCCESS`, so finance should check whether the refunds were issued.

![Query result: etl_payment_reconciliation, mismatches by reason and example rows](docs/screenshots/07_payment_reconciliation.png)

**Report (A.4).** `report` rebuilds two reporting tables and prints them in the task log:

- `etl_daily_zone_revenue`: net revenue and delivered orders per day, zone and store (459 rows).
- `etl_category_margin`: delivered orders, units, revenue, cost, gross margin and margin % per category (6 rows).

The total of both tables equals the DELIVERED net revenue in `etl_fact_orders` (₹1,128,410.45), which is checked by a unit test. `reconcile` and `report` also publish their tables as Airflow Assets, so the Assets page shows 8 `nammamart_etl` tables.

**Run audit (A.4).** `audit_run` uses `trigger_rule="all_done"`, so it runs even when an upstream task failed, and writes one row per run to `pipeline_runs` (see [Part F](#part-f-observability-tests)). If the run did not reach the report, it records `FAILED` and then fails itself, so the DAG run is shown as failed instead of green. It also deletes run folders in `include/tmp/` that are older than 3 days.

---

## Part B: ELT pipeline

**Status:** ✅ complete (B.1–B.4).

**DAG:** [`dags/nammamart_elt_harshpratapsingh.py`](dags/nammamart_elt_harshpratapsingh.py), flow **Extract → Load raw → Transform in the warehouse with SQL**. Same schedule settings as the ETL DAG (see [Part C](#part-c-scheduling)).
**Helper code:** [`include/nammamart_elt.py`](include/nammamart_elt.py) (file snapshot and header check, and a runner that executes `.sql` files in one transaction with the run's parameters). **SQL:** [`include/sql/`](include/sql/).

| Step | Status | Tasks | SQL |
|---|---|---|---|
| B.1 Extract + raw layer | ✅ | `extract_customers`, `extract_products`, `extract_stores`, `extract_orders`, `extract_payments` (parallel), `load_raw`, `load_raw_increments` | [`raw/raw_tables.sql`](include/sql/raw/raw_tables.sql), [`raw/load_raw.sql`](include/sql/raw/load_raw.sql), [`raw/load_raw_increments.sql`](include/sql/raw/load_raw_increments.sql) |
| B.2 Staging layer + data quality in SQL | ✅ | `staging.stg_stores` → `stg_products` → `stg_customers` → `stg_orders` → `stg_payments` (task group, one after another) | [`common/macros.sql`](include/sql/common/macros.sql), [`common/dq_setup.sql`](include/sql/common/dq_setup.sql), [`staging/stg_*.sql`](include/sql/staging/) |
| B.3 Mart layer + run audit | ✅ | `marts.mart_daily_store_kpis` → `mart_category_margin` → `mart_delivery_sla` → `mart_payment_reconciliation` → `mart_payment_mode_revenue` → `mart_top_customers` (task group), `audit_run` | [`marts/mart_*.sql`](include/sql/marts/) |
| B.4 ETL vs ELT comparison | ✅ | [`analysis/business_questions.py`](analysis/business_questions.py) | [`analysis/sql/`](analysis/sql/) |

Green run in the Airflow UI (Graph view, `staging` and `marts` task groups expanded, all 19 tasks `success`):

![ELT DAG in Graph view, green run](docs/screenshots/02_elt_graph_green.png)

**Extract (B.1).** Each task copies its source file into the run folder (`include/tmp/harshpratapsingh/<run_id>/elt/extract/`) and checks only the header line with Python's `csv` module. Nothing is parsed or cleaned. `extract_orders` also picks up next-day files matching `orders_*_increment.csv`.

**Raw layer (B.1).** `load_raw` loads the five files into `raw_customers`, `raw_products`, `raw_stores`, `raw_orders` and `raw_payments` with DuckDB SQL, in one transaction:

- Every column is read and stored as **text** (`read_csv(..., all_varchar = true)`); empty cells stay empty strings. A unit test compares every cell of every raw table with its source file, so the raw tables are an **exact copy**, bad rows included (for example `payment_mode = 'UPI '` with the trailing space, `order_ts = 'N/A'`, 18 empty `customer_id`).
- Four metadata columns are added: `_source_file`, `_source_row` (CSV line number, header = line 1), `_run_id` and `_loaded_at` (UTC). Together they trace any row back to the exact file line and the run that loaded it.
- Columns are matched by name (`INSERT ... BY NAME`), so a renamed or missing column fails the task with a clear error, and the transaction loads nothing.
- Raw is **append-only**: every run adds its own batch, and nothing is ever updated or deleted. Each file is loaded only once per run, so a retried or cleared task never duplicates a batch. Staging reads only the current run's batch, so the growth of raw never double-counts anything.

**Incremental load, raw step (B.1).** `load_raw_increments` is a separate step that **appends** `orders_2026-10-01_increment.csv` to `raw_orders`, keeping its own `_source_file`. If there is no increment file, the step logs that and succeeds.

Result of a run on 2026-10-20 (rows per run): `raw_customers` 405, `raw_products` 152, `raw_stores` 15, `raw_orders` 2,412 + 160 (increment), `raw_payments` 2,228. A second run with the same files appends a second batch of the same size (`raw_orders` 2,572 → 5,144 rows in total).

**Staging layer (B.2), SQL only.** Each staging task runs three SQL files: [`common/macros.sql`](include/sql/common/macros.sql) (small SQL functions for cleaning, type conversion and PII masking), [`common/dq_setup.sql`](include/sql/common/dq_setup.sql) (the rule catalog `elt_dq_rules` and the `stg_rejected_rows` table) and its own [`staging/stg_<table>.sql`](include/sql/staging/). Each staging file reads only **this run's batch** of its raw table and follows the same four steps:

1. **Type, standardise and check.** Cast types (`TRY_CAST`, `try_strptime` with the exact formats), trim values, map payment modes (`upi`, `UPI `, `cod` → `UPI`, `UPI`, `Cash on Delivery`), title-case `loyalty_tier`, turn placeholders such as `N/A` into NULL, show a missing store zone or customer city as `Unknown`, and **mask PII** in SQL (`mask_email`, `mask_phone`, `mask_name`; the raw columns are not carried into staging). Every rule becomes one TRUE/FALSE column named `fail__<rule>`.
2. **failure_reason.** The `fail__` columns are unpivoted and joined to the rule catalog; the reasons of every Critical rule a row breaks are joined in rule order.
3. **Results.** One `dq_results` row per rule (and per file for orders), plus the file-level **freshness** and **volume** checks; every rejected row (duplicate copies included) is listed in `stg_rejected_rows` with its file, CSV line and reason. Re-running the same run replaces its rows, so retries never create duplicates.
4. **Staging table.** One row per key (later copies of a duplicate are removed), with `dq_status` (`VALID` / `REJECTED`) and `failure_reason`. Rejected rows are **kept and flagged**, not deleted; the marts will use only `VALID` rows.

The rule catalog holds the **same 72 row rules plus 2 file-level checks (freshness, volume)** as the ETL pipeline (same names, dimensions, severities, order and reason text), and every threshold and allowed list (120 minutes, 8–35 minutes, ₹0.01, 24 hours, 90 %, valid statuses, modes, zones, tiers) is passed to the SQL from `include/nammamart_common.py`, so a rule is changed in one place for both pipelines.

**Incremental load, staging step (B.2): MERGE on `order_id`.** `stg_orders` is a persistent table. For each order the latest **valid** version wins (a next-day file is newer than `orders.csv`); if no version is valid, the latest version is kept as `REJECTED`. The batch is then merged with a fingerprint (`row_hash`) of the source values and the quality result: new order → **insert**, changed order → **update**, identical order → untouched.

| Run | MERGE result | `stg_orders` |
|---|---|---|
| First run (both files) | 2,550 inserted, 0 updated | 2,550 orders (2,313 VALID, 237 REJECTED) |
| Re-run with the same files | **0 inserted, 0 updated, 2,550 unchanged** | 2,550 |
| Day 1 without the increment file (test) | 2,400 inserted | 2,400 |
| Day 2 with the increment file (test) | **150 inserted, 10 updated** | 2,550, no duplicates; e.g. `ORD000017` is now `RETURNED`, `_source_file = orders_2026-10-01_increment.csv` |

ELT updates all 10 corrections (one of them is rejected, but staging keeps rejected rows with their flag), while the ETL fact table updated 9 (it holds only valid rows).

**Same result as ETL.** A unit test runs the ETL Python rules and the ELT SQL on the same files and checks that they agree exactly:

| | ETL | ELT |
|---|---|---|
| Checks in `dq_results` (name, dimension, severity, rows checked, rows failed) | 110 | 110, identical |
| Rejected rows (file, line, reason text) | 287 quarantined | 287 in `stg_rejected_rows`, identical |
| Valid orders | 2,313 in `etl_fact_orders` | 2,313 `VALID` in `stg_orders`, same IDs, status, amount, mode and source file |
| Delivered net revenue | ₹1,128,410.45 | ₹1,128,410.45 |
| Masked name / email / phone of the 397 valid customers | | identical |

Staging tables after a run: `stg_customers` 397 VALID + 3 REJECTED, `stg_products` 142 + 8, `stg_stores` 15 + 0, `stg_orders` 2,313 + 237, `stg_payments` 2,209 + 15.

**Mart layer (B.3), SQL only.** Six business-ready tables, each built by one SQL file in [`include/sql/marts/`](include/sql/marts/) from the **VALID** staging rows only, and rebuilt in full on every run (so they always match staging and never double-count):

| Mart | Grain | Answers | Rows |
|---|---|---|---|
| `mart_daily_store_kpis` | day × store: delivered orders, net revenue, gross margin, average delivery minutes, orders and % within 15 minutes | Q1 (daily net revenue by zone and store) | 459 |
| `mart_category_margin` | category: delivered orders, units, revenue, cost, gross margin, margin % | Q2 | 6 |
| `mart_delivery_sla` | store: delivered orders, average / fastest / slowest delivery, orders and % within 15 minutes | Q3 | 15 |
| `mart_payment_reconciliation` | one row per order/payment mismatch, with reason and amount at risk (same rules as ETL) | Q4, acceptance criterion 7 | 433 |
| `mart_payment_mode_revenue` | payment mode: delivered orders, net revenue, mismatches, amount not reconciled | Q4 | 4 |
| `mart_top_customers` | customer: spend rank, masked name, loyalty tier, city, delivered orders, total spend (no email or phone) | Q5 | 396 |

`mart_payment_mode_revenue` is one extra mart beyond the five in the brief: none of the five has a payment-mode breakdown, and it makes Q4 a single simple query. Q6 (rows rejected today, by file and reason) is answered from `stg_rejected_rows`.

**Same numbers as ETL.** A unit test builds the ETL report tables and the ELT marts from the same files in one database and checks them row by row: `mart_daily_store_kpis` equals `etl_daily_zone_revenue` (all 459 day × store rows), `mart_category_margin` equals `etl_category_margin`, and `mart_payment_reconciliation` equals `etl_payment_reconciliation` (433 mismatches, ₹276,928.50 at risk). Net revenue (₹1,128,410.45), gross margin (₹277,758.40) and the 15-minute SLA counts are the same in both pipelines, and every mart adds up to the same revenue.

**Run audit (B.3).** `audit_run` (`trigger_rule="all_done"`) writes one row per run to `pipeline_runs`, like the ETL DAG. In ELT `rows_loaded` is the number of rows written to the raw tables: ELT loads **every** row it receives (5,372), and `rows_quarantined` is the number of rows rejected in staging (287, listed in `stg_rejected_rows`). If the marts were not built, the audit records `FAILED` and fails itself, so the run is shown as failed.

**Both DAGs at 09:00.** The two DAGs write to the same DuckDB file, which allows one writer at a time. When both were started together, the ELT DAG found the warehouse locked once, waited and retried (`get_connection` retries for up to 30 seconds), and both runs succeeded with their own `dq_results` and `pipeline_runs` rows.

**Assets.** The DAG publishes 17 tables as Airflow Assets in group `nammamart_elt` (5 raw, 5 staging, `stg_rejected_rows`, 6 marts), so the Assets page shows every ELT table and the task that produces it.

**ETL vs ELT for September (B.4).** The brief requires the ELT mart totals to match the ETL totals for September. They match exactly, in the latest run of each DAG:

| September 2026 (delivered orders) | ETL | ELT | Difference |
|---|---|---|---|
| Delivered orders | 1,955 | 1,955 | 0 |
| Net revenue | ₹1,056,219.25 | ₹1,056,219.25 | 0.00 |
| Gross margin | ₹258,846.51 | ₹258,846.51 | 0.00 |
| Delivered within 15 minutes | 548 | 548 | 0 |
| Day × store rows that differ (`etl_daily_zone_revenue` vs `mart_daily_store_kpis`) | 444 | 444 | 0 |

Every zone matches as well (see the [business questions](#business-questions) section, which is generated from the warehouse by [`analysis/business_questions.py`](analysis/business_questions.py) together with the six answers). September excludes the 136 valid new orders dated 1 October from the increment file; both pipelines include them in their all-dates totals (₹1,128,410.45).

**Why they match.** Both pipelines apply the same rules with the same thresholds (taken from `include/nammamart_common.py`), the same duplicate handling (first copy kept), the same "master row must be valid" rule for orders, the same "latest valid version of an order wins" rule for next-day files, and the same formulas (net revenue = `order_amount` of DELIVERED orders; margin rounded per order). Unit tests check that the two pipelines agree check by check, row by row and table by table, so any future change that makes them drift apart fails a test.

**What is different (by design, not in the totals).** ELT loads all 5,372 rows into raw and keeps the 287 rejected rows inside the warehouse (`stg_*` with `dq_status = 'REJECTED'`, and `stg_rejected_rows`), while ETL loads 5,076 rows and writes the 287 rejected rows to quarantine CSV files. In the incremental test ELT's MERGE updated 10 orders and ETL's updated 9: the tenth correction belongs to a rejected order, which ELT keeps (flagged) and ETL does not hold at all.

---

## Part C: Scheduling

**Status:** ✅ answered for the ETL DAG; the ELT DAG will use the same settings.

Both DAGs in the Airflow UI (display timezone +05:30, bottom-left): schedule `0 9 * * 1-6`, next run `2026-10-21 09:00:00` IST.

![ETL and ELT DAG schedule: 0 9 * * 1-6, next run 2026-10-21 09:00 IST](docs/screenshots/03_schedule_9am_ist.png)

| Setting | Value | Why |
|---|---|---|
| `schedule` | `"0 9 * * 1-6"` | 09:00, Monday to Saturday |
| `start_date` | `pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata")` | Makes Airflow read the cron in IST |
| `catchup` | `False` | Do not create runs for past days (see below) |
| `retries` / `retry_delay` | 2 / 1 minute | A missing file or a locked warehouse often clears within minutes |
| `execution_timeout` | 10 minutes per task | A hung task is stopped instead of blocking the report |
| `dagrun_timeout` | 25 minutes | A run not finished by 09:25 is marked failed, before the 09:30 review |
| `max_active_runs` | 1 | A manual run and the scheduled run never write to the warehouse at the same time |

**Why `catchup=False`.** Every run reads the same full set of files, so back-filling one run per missed day since `start_date` would only reload identical data and delay today's run. If a day is missed, one run with the latest files is what the business needs.

**1. What is your cron expression?**
`0 9 * * 1-6`: minute 0, hour 9, every day of the month, every month, days of the week 1–6 (Monday to Saturday).

**2. What time does the run show in UTC in the Airflow UI?**
**03:30 UTC.** IST is UTC+5:30 with no daylight saving, so 09:00 IST is always 03:30 UTC. Observed on 2026-10-20: the scheduled run is named `scheduled__2026-10-20T03:30:00+00:00`.

**3. What would happen at 9 AM if you had left the `start_date` in UTC?**
Airflow would read `0 9 * * 1-6` as 09:00 **UTC**, which is **14:30 IST**: five hours after the 09:30 review, every day. Nothing at 9 AM IST would show that anything was wrong; the numbers would simply arrive in the afternoon.

**4. What happens if your laptop or Docker is off at 9 AM?**
The scheduler is not running, so nothing runs at 09:00 and the numbers are not ready for the review. When Airflow starts again, because `catchup=False` it creates **one** run for the most recent missed schedule (not one per missed day) and starts it straight away. Observed on 2026-10-20: when the DAG was unpaused in the afternoon, after 09:00 IST had already passed, Airflow immediately ran `scheduled__2026-10-20T03:30:00+00:00` (started 08:51 UTC = 14:21 IST), and only that one run. In production Airflow should run on an always-on server or a managed service, with an alert when the 09:00 run has not succeeded by 09:25.

---

## Part D: Data quality dimensions

**Status:** ✅ ETL (Python) and ELT (SQL) checks done; both write the same 110 checks to `dq_results` on every run.

**Critical** = the row is quarantined with a `failure_reason`. **Warning** = the row is loaded and the problem is counted. Every check, of either severity, writes one row to `dq_results` on every run (`run_id`, `dag_id`, `check_name`, `dimension`, `severity`, `rows_checked`, `rows_failed`, `status`, `checked_at`); `status` is PASS, WARN (Warning check with failures) or FAIL (Critical check with failures). Check names are `<file>.<rule>`, for example `orders.order_amount_matches_formula`.

**ETL checks (Python, [`include/nammamart_etl_checks.py`](include/nammamart_etl_checks.py))**

| Dimension | Critical (quarantine) | Warning (load + log) |
|---|---|---|
| Completeness | Key not missing in all 5 files; order `customer_id`, `order_ts`, `quantity`, `unit_price`, `discount_pct`, `order_amount`; DELIVERED order has `delivery_minutes`; payment `order_id` | Missing `email` or `city`; store `zone`; online SUCCESS payment without `gateway_ref`; orders volume dropped > 90% vs the previous run |
| Uniqueness | `order_id`, `payment_id`, `customer_id`, `product_id`, `store_id` (first copy kept, later copies quarantined) | — |
| Validity | `order_ts`, `payment_ts` (`YYYY-MM-DD HH:MM:SS`) and `signup_date` (`YYYY-MM-DD`) parse; `order_status` and `payment_status` in the allowed lists; `payment_mode` maps to UPI / Card / Cash on Delivery / Wallet; numeric columns are numbers; whole-number `quantity`; `discount_pct` 0–100; known `loyalty_tier` | Status or mode needed standardising (`delivered`, `upi`, `UPI `, `cod`); email format; phone 10 digits; pincode 6 digits; `channel` App/Web; `is_active` Y/N; `is_perishable` True/False |
| Accuracy | `quantity` > 0; `unit_price` > 0; `order_amount` = quantity × unit_price × (1 − discount/100) within ₹0.01; `order_amount` ≥ 0; `delivery_minutes` ≤ 120; `mrp` > 0; `cost_price` ≤ `mrp`; `amount_paid` ≥ 0 | `delivery_minutes` outside 8–35; store capacity ≤ 0 |
| Consistency | Order's `customer_id`, `product_id`, `store_id` exist in the master file **and** point to a master row that passed its own checks | `loyalty_tier` casing; payment for an order not in the orders files (listed in reconciliation) |
| Timeliness | No future `order_ts`, `payment_ts` or `signup_date` (compared with Bengaluru time) | Freshness: newest order older than 24 hours |

Result of a run on 2026-10-20: **110 checks**, all 6 dimensions, **29 FAIL, 14 WARN, 67 PASS**. The failure counts match the [Data Issues Log](#05-data-issues-log) exactly (verified by [`tests/test_nammamart_etl.py`](tests/test_nammamart_etl.py)).

Query result: the `dq_results` scorecard of every run of both DAGs, and the latest run of each DAG by quality dimension (ETL and ELT identical):

![Query result: dq_results scorecard per run and per dimension, both pipelines](docs/screenshots/05_dq_results.png)

**ELT checks (SQL, [`include/sql/common/dq_setup.sql`](include/sql/common/dq_setup.sql) + [`include/sql/staging/`](include/sql/staging/))**

The ELT pipeline implements **the same checks in SQL**: same names, dimensions, severities and reasons, listed once in the rule catalog table `elt_dq_rules`. Each staging file computes one TRUE/FALSE column per rule, and generic SQL turns them into `failure_reason`, `dq_status` and the `dq_results` rows (with `dag_id = nammamart_elt_harshpratapsingh`). A unit test checks that, on the same files, the two pipelines report identical results for all 110 checks and reject the same 287 rows for the same reasons. The one difference is the meaning of Critical: ETL removes the row before loading (quarantine file), ELT keeps it in staging with `dq_status = 'REJECTED'` and lists it in `stg_rejected_rows`; in both cases it never reaches the reporting tables.

---

## Business questions

Each question is answered with **one query on the final tables of each pipeline**: the ETL star schema and report tables (`etl_*`), and the ELT marts (`mart_*`, `stg_rejected_rows`). For every question the script runs both queries and checks that they return the same rows; open **SQL** under each question to see both queries.

To refresh this section after a new run (with no DAG running): `python analysis/business_questions.py --update-submission`.

<!-- business-questions:start -->

Results below are from the latest successful run of each DAG: `nammamart_elt_harshpratapsingh` run `manual__2026-10-20T11:52:44.250688+00:00`; `nammamart_etl_harshpratapsingh` run `manual__2026-10-20T10:10:27.361804+00:00`. Generated by [`analysis/business_questions.py`](analysis/business_questions.py) from the SQL in [`analysis/sql/`](analysis/sql/).

### Q1. Daily net revenue by zone and by store (delivered orders only, after discount)

✅ ETL and ELT return **identical** results.

<details><summary>SQL</summary>

**ETL** ([`analysis/sql/q1_etl.sql`](analysis/sql/q1_etl.sql))

```sql
-- Q1 (ETL): daily net revenue by zone and by store (delivered orders only, after discount).
SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
FROM etl_daily_zone_revenue
ORDER BY order_date, zone, store_id;
```

**ELT** ([`analysis/sql/q1_elt.sql`](analysis/sql/q1_elt.sql))

```sql
-- Q1 (ELT): daily net revenue by zone and by store (delivered orders only, after discount).
SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
FROM mart_daily_store_kpis
ORDER BY order_date, zone, store_id;
```

</details>

**Result (ETL and ELT):**

| order_date | zone | store_id | store_name | delivered_orders | net_revenue |
|---|---|---|---|---|---|
| 2026-09-01 | East | S08 | NammaMart Whitefield | 4 | 2,984.70 |
| 2026-09-01 | East | S09 | NammaMart Indiranagar | 4 | 2,089.60 |
| 2026-09-01 | East | S10 | NammaMart Marathahalli | 3 | 2,141.60 |
| 2026-09-01 | East | S11 | NammaMart HSR Layout | 6 | 2,121.15 |
| 2026-09-01 | North | S01 | NammaMart Hebbal | 3 | 1,410.80 |
| 2026-09-01 | North | S02 | NammaMart Yelahanka | 6 | 2,385.55 |
| 2026-09-01 | North | S03 | NammaMart RT Nagar | 6 | 4,583.80 |
| 2026-09-01 | South | S05 | NammaMart JP Nagar | 2 | 803.00 |
| 2026-09-01 | South | S06 | NammaMart Banashankari | 6 | 2,812.00 |
| 2026-09-01 | Unknown | S07 | NammaMart BTM Layout | 9 | 5,844.75 |

_First 10 of 459 rows._

### Q2. Revenue and gross margin by product category (margin = revenue − quantity × cost_price)

✅ ETL and ELT return **identical** results.

<details><summary>SQL</summary>

**ETL** ([`analysis/sql/q2_etl.sql`](analysis/sql/q2_etl.sql))

```sql
-- Q2 (ETL): revenue and gross margin by product category (margin = revenue - quantity x cost_price).
SELECT category, delivered_orders, revenue, cost, gross_margin, margin_pct
FROM etl_category_margin
ORDER BY revenue DESC;
```

**ELT** ([`analysis/sql/q2_elt.sql`](analysis/sql/q2_elt.sql))

```sql
-- Q2 (ELT): revenue and gross margin by product category (margin = revenue - quantity x cost_price).
SELECT category, delivered_orders, revenue, cost, gross_margin, margin_pct
FROM mart_category_margin
ORDER BY revenue DESC;
```

</details>

**Result (ETL and ELT):**

| category | delivered_orders | revenue | cost | gross_margin | margin_pct |
|---|---|---|---|---|---|
| Staples | 300 | 264,456.90 | 198,021.62 | 66,435.28 | 25.12 |
| Household | 359 | 210,229.95 | 156,174.68 | 54,055.27 | 25.71 |
| Personal Care | 319 | 208,774.70 | 162,065.13 | 46,709.57 | 22.37 |
| Dairy & Bakery | 402 | 177,404.85 | 130,204.03 | 47,200.82 | 26.61 |
| Snacks & Beverages | 339 | 174,575.60 | 133,236.36 | 41,339.24 | 23.68 |
| Fruits & Vegetables | 372 | 92,968.45 | 70,950.23 | 22,018.22 | 23.68 |

### Q3. Average delivery time per store, and the % of delivered orders within 15 minutes

✅ ETL and ELT return **identical** results.

<details><summary>SQL</summary>

**ETL** ([`analysis/sql/q3_etl.sql`](analysis/sql/q3_etl.sql))

```sql
-- Q3 (ETL): average delivery time per store, and the % of delivered orders within 15 minutes.
SELECT s.zone, f.store_id, s.store_name,
       count(*)                                                              AS delivered_orders,
       round(avg(f.delivery_minutes), 1)                                     AS avg_delivery_minutes,
       round(100.0 * count(*) FILTER (WHERE f.within_15_min) / count(*), 1)  AS pct_within_15_min
FROM etl_fact_orders AS f
JOIN etl_dim_store AS s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY s.zone, f.store_id;
```

**ELT** ([`analysis/sql/q3_elt.sql`](analysis/sql/q3_elt.sql))

```sql
-- Q3 (ELT): average delivery time per store, and the % of delivered orders within 15 minutes.
SELECT zone, store_id, store_name, delivered_orders, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla
ORDER BY zone, store_id;
```

</details>

**Result (ETL and ELT):**

| zone | store_id | store_name | delivered_orders | avg_delivery_minutes | pct_within_15_min |
|---|---|---|---|---|---|
| East | S08 | NammaMart Whitefield | 155 | 20.60 | 35.50 |
| East | S09 | NammaMart Indiranagar | 127 | 21.10 | 27.60 |
| East | S10 | NammaMart Marathahalli | 141 | 21.90 | 27.00 |
| East | S11 | NammaMart HSR Layout | 144 | 21.00 | 27.80 |
| North | S01 | NammaMart Hebbal | 141 | 20.60 | 31.20 |
| North | S02 | NammaMart Yelahanka | 139 | 21.00 | 30.90 |
| North | S03 | NammaMart RT Nagar | 137 | 20.70 | 29.20 |
| South | S04 | NammaMart Jayanagar | 130 | 21.60 | 26.90 |
| South | S05 | NammaMart JP Nagar | 134 | 21.20 | 29.10 |
| South | S06 | NammaMart Banashankari | 145 | 22.30 | 22.10 |
| Unknown | S07 | NammaMart BTM Layout | 143 | 21.10 | 33.60 |
| West | S12 | NammaMart Rajajinagar | 113 | 22.00 | 23.90 |
| West | S13 | NammaMart Malleshwaram | 137 | 21.80 | 27.00 |
| West | S14 | NammaMart Vijayanagar | 162 | 21.50 | 31.50 |
| West | S15 | NammaMart Basaveshwaranagar | 143 | 22.30 | 21.00 |

### Q4. Revenue by payment mode, and the total amount that doesn't reconcile with the payment gateway

✅ ETL and ELT return **identical** results.

<details><summary>SQL</summary>

**ETL** ([`analysis/sql/q4_etl.sql`](analysis/sql/q4_etl.sql))

```sql
-- Q4 (ETL): revenue by payment mode, and the amount that does not reconcile with the payment gateway.
-- A mismatch is assigned to the order's payment mode, or to the gateway's when there is no valid order.
WITH revenue AS (
    SELECT payment_mode, count(*) AS delivered_orders, sum(net_revenue) AS net_revenue
    FROM etl_fact_orders
    WHERE order_status = 'DELIVERED'
    GROUP BY payment_mode
),
not_reconciled AS (
    SELECT coalesce(order_payment_mode, gateway_payment_mode) AS payment_mode,
           count(*) AS mismatches, sum(amount_at_risk) AS amount_not_reconciled
    FROM etl_payment_reconciliation
    GROUP BY 1
),
per_mode AS (
    SELECT coalesce(r.payment_mode, n.payment_mode) AS payment_mode,
           coalesce(r.delivered_orders, 0) AS delivered_orders, coalesce(r.net_revenue, 0) AS net_revenue,
           coalesce(n.mismatches, 0) AS mismatches, coalesce(n.amount_not_reconciled, 0) AS amount_not_reconciled
    FROM revenue AS r FULL OUTER JOIN not_reconciled AS n USING (payment_mode)
)
SELECT coalesce(payment_mode, 'TOTAL') AS payment_mode, CAST(sum(delivered_orders) AS BIGINT) AS delivered_orders,
       round(sum(net_revenue), 2) AS net_revenue, CAST(sum(mismatches) AS BIGINT) AS mismatches,
       round(sum(amount_not_reconciled), 2) AS amount_not_reconciled
FROM per_mode
GROUP BY ROLLUP (payment_mode)
ORDER BY payment_mode IS NULL, net_revenue DESC;
```

**ELT** ([`analysis/sql/q4_elt.sql`](analysis/sql/q4_elt.sql))

```sql
-- Q4 (ELT): revenue by payment mode, and the amount that does not reconcile with the payment gateway.
SELECT coalesce(payment_mode, 'TOTAL') AS payment_mode, CAST(sum(delivered_orders) AS BIGINT) AS delivered_orders,
       round(sum(net_revenue), 2) AS net_revenue, CAST(sum(mismatches) AS BIGINT) AS mismatches,
       round(sum(amount_not_reconciled), 2) AS amount_not_reconciled
FROM mart_payment_mode_revenue
GROUP BY ROLLUP (payment_mode)
ORDER BY payment_mode IS NULL, net_revenue DESC;
```

</details>

**Result (ETL and ELT):**

| payment_mode | delivered_orders | net_revenue | mismatches | amount_not_reconciled |
|---|---|---|---|---|
| UPI | 1,131 | 598,618.20 | 208 | 127,400.55 |
| Card | 428 | 231,787.85 | 100 | 55,502.60 |
| Cash on Delivery | 298 | 169,026.60 | 70 | 63,143.05 |
| Wallet | 234 | 128,977.80 | 55 | 30,882.30 |
| TOTAL | 2,091 | 1,128,410.45 | 433 | 276,928.50 |

### Q5. Top 10 customers by spend, with loyalty tier (no raw email or phone shown)

✅ ETL and ELT return **identical** results.

<details><summary>SQL</summary>

**ETL** ([`analysis/sql/q5_etl.sql`](analysis/sql/q5_etl.sql))

```sql
-- Q5 (ETL): top 10 customers by spend, with loyalty tier. Only the masked name is shown (no email or phone).
SELECT row_number() OVER (ORDER BY sum(f.net_revenue) DESC, f.customer_id) AS spend_rank,
       f.customer_id, c.name_masked, c.loyalty_tier,
       count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders AS f
JOIN etl_dim_customer AS c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY f.customer_id, c.name_masked, c.loyalty_tier
ORDER BY spend_rank
LIMIT 10;
```

**ELT** ([`analysis/sql/q5_elt.sql`](analysis/sql/q5_elt.sql))

```sql
-- Q5 (ELT): top 10 customers by spend, with loyalty tier. Only the masked name is shown (no email or phone).
SELECT spend_rank, customer_id, name_masked, loyalty_tier, delivered_orders, total_spend
FROM mart_top_customers
WHERE spend_rank <= 10
ORDER BY spend_rank;
```

</details>

**Result (ETL and ELT):**

| spend_rank | customer_id | name_masked | loyalty_tier | delivered_orders | total_spend |
|---|---|---|---|---|---|
| 1 | C00136 | S*** B*** | Bronze | 9 | 8,738.10 |
| 2 | C00236 | S*** N*** | Platinum | 12 | 7,831.80 |
| 3 | C00115 | S*** K*** | Bronze | 7 | 7,337.70 |
| 4 | C00310 | K*** P*** | Bronze | 8 | 7,258.95 |
| 5 | C00265 | V*** H*** | Gold | 8 | 7,151.30 |
| 6 | C00338 | B*** I*** | Silver | 9 | 6,848.55 |
| 7 | C00283 | N*** R*** | Bronze | 7 | 6,766.40 |
| 8 | C00357 | A*** N*** | Bronze | 11 | 6,637.90 |
| 9 | C00209 | K*** P*** | Platinum | 8 | 6,519.40 |
| 10 | C00072 | N*** M*** | Silver | 11 | 6,433.95 |

### Q6. Number of rows rejected today, by file and by reason

✅ ETL and ELT return **identical** results.

<details><summary>SQL</summary>

**ETL** ([`analysis/sql/q6_etl.sql`](analysis/sql/q6_etl.sql))

```sql
-- Q6 (ETL): rows rejected in the latest successful ETL run, by file and by reason.
-- The ETL pipeline keeps rejected rows in quarantine CSV files, so this query reads those files
-- (getvariable('quarantine_glob') = include/quarantine/harshpratapsingh/*_failed_*.csv).
-- "(all reasons)" is the number of rejected rows; a row that broke two rules also counts once under each reason.
WITH latest AS (
    SELECT trim(regexp_replace(run_id, '[^A-Za-z0-9_-]+', '_', 'g'), '_') AS file_run_id
    FROM pipeline_runs
    WHERE dag_id = 'nammamart_etl_harshpratapsingh' AND status = 'SUCCESS'
    ORDER BY start_time DESC
    LIMIT 1
),
rejected AS (
    SELECT q.source_file, q.failure_reason
    FROM read_csv(getvariable('quarantine_glob'), union_by_name = true, all_varchar = true, filename = true) AS q
    CROSS JOIN latest
    WHERE parse_filename(q.filename) = regexp_replace(q.source_file, '\.csv$', '') || '_failed_' || latest.file_run_id || '.csv'
),
by_reason AS (
    SELECT source_file, unnest(string_split(failure_reason, '; ')) AS reason FROM rejected
)
SELECT * FROM (
    SELECT source_file, '(all reasons)' AS reason, count(*) AS rows_rejected FROM rejected GROUP BY ALL
    UNION ALL
    SELECT source_file, reason, count(*) FROM by_reason GROUP BY ALL
)
ORDER BY source_file, reason <> '(all reasons)', rows_rejected DESC, reason;
```

**ELT** ([`analysis/sql/q6_elt.sql`](analysis/sql/q6_elt.sql))

```sql
-- Q6 (ELT): rows rejected in the latest successful ELT run, by file and by reason (from stg_rejected_rows).
-- "(all reasons)" is the number of rejected rows; a row that broke two rules also counts once under each reason.
WITH latest AS (
    SELECT run_id
    FROM pipeline_runs
    WHERE dag_id = 'nammamart_elt_harshpratapsingh' AND status = 'SUCCESS'
    ORDER BY start_time DESC
    LIMIT 1
),
rejected AS (
    SELECT r.source_file, r.failure_reason
    FROM stg_rejected_rows AS r JOIN latest ON r._run_id = latest.run_id
),
by_reason AS (
    SELECT source_file, unnest(string_split(failure_reason, '; ')) AS reason FROM rejected
)
SELECT * FROM (
    SELECT source_file, '(all reasons)' AS reason, count(*) AS rows_rejected FROM rejected GROUP BY ALL
    UNION ALL
    SELECT source_file, reason, count(*) FROM by_reason GROUP BY ALL
)
ORDER BY source_file, reason <> '(all reasons)', rows_rejected DESC, reason;
```

</details>

**Result (ETL and ELT):**

| source_file | reason | rows_rejected |
|---|---|---|
| customers.csv | (all reasons) | 8 |
| customers.csv | duplicate customer_id (copy of an earlier row) | 5 |
| customers.csv | signup_date is in the future | 3 |
| orders.csv | (all reasons) | 235 |
| orders.csv | product_id points to a quarantined products row | 126 |
| orders.csv | customer_id is missing | 18 |
| orders.csv | customer_id points to a quarantined customers row | 18 |
| orders.csv | order_amount != quantity x unit_price x (1 - discount_pct/100) | 18 |
| orders.csv | duplicate order_id (copy of an earlier row) | 12 |
| orders.csv | quantity is zero or negative | 10 |
| orders.csv | product_id not found in products | 9 |
| orders.csv | DELIVERED order has no delivery_minutes | 8 |
| orders.csv | order_ts is in the future | 7 |
| orders.csv | delivery_minutes above 120 | 6 |
| orders.csv | order_amount is negative | 6 |
| orders.csv | store_id not found in stores | 5 |
| orders.csv | order_ts is not a valid YYYY-MM-DD HH:MM:SS timestamp | 4 |
| orders.csv | discount_pct outside 0-100 | 3 |
| orders.csv | order_status is not DELIVERED/CANCELLED/RETURNED | 3 |
| orders.csv | order_ts is missing | 2 |
| orders.csv | payment_mode is not a known mode | 2 |
| orders_2026-10-01_increment.csv | (all reasons) | 15 |
| orders_2026-10-01_increment.csv | product_id points to a quarantined products row | 14 |
| orders_2026-10-01_increment.csv | customer_id points to a quarantined customers row | 1 |
| payments.csv | (all reasons) | 19 |
| payments.csv | payment_ts is in the future | 7 |
| payments.csv | amount_paid is negative | 6 |
| payments.csv | duplicate payment_id (copy of an earlier row) | 4 |
| payments.csv | payment_mode is not a known mode | 2 |
| products.csv | (all reasons) | 10 |
| products.csv | cost_price is greater than mrp | 5 |
| products.csv | category is missing | 3 |
| products.csv | duplicate product_id (copy of an earlier row) | 2 |
| products.csv | mrp is zero or negative | 1 |

### ETL vs ELT totals for September 2026 (Part B4)

<details><summary>SQL</summary>

```sql
-- Part B4: ELT mart totals vs ETL totals for September 2026 (delivered orders, order_date 1-30 September).
-- Values are formatted as text so counts and rupee amounts can share one table.
WITH etl AS (
    SELECT count(*) AS delivered_orders, sum(net_revenue) AS net_revenue, sum(gross_margin) AS gross_margin,
           count(*) FILTER (WHERE within_15_min) AS within_15_min
    FROM etl_fact_orders
    WHERE order_status = 'DELIVERED' AND order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
),
elt AS (
    SELECT CAST(sum(delivered_orders) AS BIGINT) AS delivered_orders, sum(net_revenue) AS net_revenue,
           sum(gross_margin) AS gross_margin, CAST(sum(orders_within_15_min) AS BIGINT) AS within_15_min
    FROM mart_daily_store_kpis
    WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
),
day_store_diff AS (
    SELECT count(*) AS n FROM (
        (SELECT order_date, store_id, delivered_orders, net_revenue FROM etl_daily_zone_revenue
         WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
         EXCEPT
         SELECT order_date, store_id, delivered_orders, net_revenue FROM mart_daily_store_kpis
         WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30')
        UNION ALL
        (SELECT order_date, store_id, delivered_orders, net_revenue FROM mart_daily_store_kpis
         WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
         EXCEPT
         SELECT order_date, store_id, delivered_orders, net_revenue FROM etl_daily_zone_revenue
         WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'))
)
SELECT 'Delivered orders' AS metric, format('{:,}', etl.delivered_orders) AS etl,
       format('{:,}', elt.delivered_orders) AS elt, format('{:,}', elt.delivered_orders - etl.delivered_orders) AS difference
FROM etl, elt
UNION ALL
SELECT 'Net revenue (₹)', format('{:,.2f}', etl.net_revenue), format('{:,.2f}', elt.net_revenue),
       format('{:,.2f}', round(elt.net_revenue - etl.net_revenue, 2) + 0.0)
FROM etl, elt
UNION ALL
SELECT 'Gross margin (₹)', format('{:,.2f}', etl.gross_margin), format('{:,.2f}', elt.gross_margin),
       format('{:,.2f}', round(elt.gross_margin - etl.gross_margin, 2) + 0.0)
FROM etl, elt
UNION ALL
SELECT 'Delivered within 15 minutes', format('{:,}', etl.within_15_min), format('{:,}', elt.within_15_min),
       format('{:,}', elt.within_15_min - etl.within_15_min)
FROM etl, elt
UNION ALL
SELECT 'Day × store rows that differ (ETL report vs ELT mart)', format('{:,}', n), format('{:,}', n), '0'
FROM day_store_diff;
```

```sql
-- Part B4: September 2026 net revenue by zone, ETL report table vs ELT mart.
WITH etl AS (
    SELECT zone, sum(delivered_orders) AS orders, sum(net_revenue) AS revenue
    FROM etl_daily_zone_revenue
    WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
    GROUP BY zone
),
elt AS (
    SELECT zone, sum(delivered_orders) AS orders, sum(net_revenue) AS revenue
    FROM mart_daily_store_kpis
    WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
    GROUP BY zone
)
SELECT coalesce(zone, 'TOTAL') AS zone,
       CAST(sum(etl.orders) AS BIGINT) AS etl_orders, CAST(sum(elt.orders) AS BIGINT) AS elt_orders,
       round(sum(etl.revenue), 2) AS etl_net_revenue, round(sum(elt.revenue), 2) AS elt_net_revenue,
       round(sum(elt.revenue) - sum(etl.revenue), 2) + 0.0 AS difference
FROM etl FULL OUTER JOIN elt USING (zone)
GROUP BY ROLLUP (zone)
ORDER BY zone IS NULL, etl_net_revenue DESC;
```

</details>

| metric | etl | elt | difference |
|---|---|---|---|
| Delivered orders | 1,955 | 1,955 | 0 |
| Net revenue (₹) | 1,056,219.25 | 1,056,219.25 | 0.00 |
| Gross margin (₹) | 258,846.51 | 258,846.51 | 0.00 |
| Delivered within 15 minutes | 548 | 548 | 0 |
| Day × store rows that differ (ETL report vs ELT mart) | 0 | 0 | 0 |

| zone | etl_orders | elt_orders | etl_net_revenue | elt_net_revenue | difference |
|---|---|---|---|---|---|
| East | 517 | 517 | 271,139.95 | 271,139.95 | 0.00 |
| West | 522 | 522 | 270,322.30 | 270,322.30 | 0.00 |
| South | 387 | 387 | 225,606.30 | 225,606.30 | 0.00 |
| North | 391 | 391 | 215,332.65 | 215,332.65 | 0.00 |
| Unknown | 138 | 138 | 73,818.05 | 73,818.05 | 0.00 |
| TOTAL | 1,955 | 1,955 | 1,056,219.25 | 1,056,219.25 | 0.00 |

<!-- business-questions:end -->

---

## Part E: Data governance

**Status:** ⬜ not started.

### E1. Ownership
_To be completed._

### E2. Data dictionary
_To be completed._

### E3. Lineage
_To be completed._

### E4. Personal data
_To be completed._

### E5. Access control
_To be completed._

### E6. Retention
_To be completed._

### E7. Secrets
_To be completed._

### E8. Audit
_To be completed._

### E9. Change history
_To be completed._

---

## Part F: Observability tests

**Status:** ⬜ not started.

### Run audit (`pipeline_runs`)

Both DAGs end with an `audit_run` task (`trigger_rule="all_done"`) that writes one row per run to `pipeline_runs`: `run_id`, `dag_id`, `start_time`, `end_time` (UTC), `rows_extracted`, `rows_loaded`, `rows_quarantined` and `status` (SUCCESS / FAILED). A re-run of the same `run_id` replaces its row instead of adding a second one.

ETL examples from testing on 2026-10-20:

| Run | status | rows_extracted | rows_loaded | rows_quarantined | Duration |
|---|---|---|---|---|---|
| Normal run | SUCCESS | 5,372 | 5,076 | 287 | 2.4 s |
| Re-run, same files | SUCCESS | 5,372 | 5,076 | 287 | 2.3 s |
| `products.csv` missing | FAILED | 5,220 | 0 | 0 | 127 s (2 retries of 1 minute) |

`rows_loaded` counts the rows written to the star schema (dimensions, payments and the batch of fact orders). `rows_extracted` = `rows_loaded` + `rows_quarantined` + 9, because 9 rows of the increment file replace an older version of the same order.

![Query result: pipeline_runs for both pipelines](docs/screenshots/06_pipeline_runs.png)

ELT examples from testing on 2026-10-20:

| Run | status | rows_extracted | rows_loaded | rows_quarantined | Duration |
|---|---|---|---|---|---|
| Normal run | SUCCESS | 5,372 | 5,372 | 287 | 2.8 s |
| Re-run, same files | SUCCESS | 5,372 | 5,372 | 287 | 2.7 s |
| `products.csv` missing | FAILED | 5,220 | 0 | 0 | 135 s (2 retries of 1 minute) |

In ELT `rows_loaded` = `rows_extracted`: every row is loaded into raw, and the 287 rejected rows are flagged in staging instead of being removed.

### Experiments

Format for each: **Expected → Observed → Where I saw it → What I would change.**

| # | Experiment | Pipeline | Expected | Observed | Where I saw it | What I would change |
|---|---|---|---|---|---|---|
| 1 | Happy path | ETL | | | | |
| 1 | Happy path | ELT | | | | |
| 2 | Re-run (idempotency) | ETL | | | | |
| 2 | Re-run (idempotency) | ELT | | | | |
| 3 | Incremental load | ETL | | | | |
| 3 | Incremental load | ELT | | | | |
| 4 | Missing source file | ETL | | | | |
| 4 | Missing source file | ELT | | | | |
| 5 | New bad data | ETL | | | | |
| 5 | New bad data | ELT | | | | |
| 6 | Schema change | ETL | | | | |
| 6 | Schema change | ELT | | | | |
| 7 | Volume anomaly | ETL | | | | |
| 7 | Volume anomaly | ELT | | | | |
| 8 | Distribution drift | ETL | | | | |
| 8 | Distribution drift | ELT | | | | |
| 9 | Freshness | ETL | | | | |
| 9 | Freshness | ELT | | | | |
| 10 | Recovery | ETL | | | | |
| 10 | Recovery | ELT | | | | |

### Five pillars of data observability

| Pillar | How my setup covers it | Strength |
|---|---|---|
| Freshness | | |
| Volume | | |
| Schema | | |
| Distribution | | |
| Lineage | | |

_To be completed._

---

## Part G: Recommendation to the CTO

**Status:** ⬜ not started.

_To be completed._

---

## Screenshots

Stored in [`docs/screenshots/`](docs/screenshots/).

| # | Required screenshot | File | Status |
|---|---|---|---|
| 1 | ETL DAG in Graph view, green run | [`docs/screenshots/01_etl_graph_green.png`](docs/screenshots/01_etl_graph_green.png) | ✅ |
| 2 | ELT DAG in Graph view, green run | [`docs/screenshots/02_elt_graph_green.png`](docs/screenshots/02_elt_graph_green.png) | ✅ |
| 3 | Schedule showing 9:00 AM IST and the next run | [`docs/screenshots/03_schedule_9am_ist.png`](docs/screenshots/03_schedule_9am_ist.png) | ✅ |
| 4 | One quarantine file | [`docs/screenshots/04_quarantine_file.png`](docs/screenshots/04_quarantine_file.png) | ✅ |
| 5 | Query result: `dq_results` | [`docs/screenshots/05_dq_results.png`](docs/screenshots/05_dq_results.png) | ✅ |
| 6 | Query result: `pipeline_runs` | [`docs/screenshots/06_pipeline_runs.png`](docs/screenshots/06_pipeline_runs.png) | ✅ |
| 7 | Query result: payment reconciliation table | [`docs/screenshots/07_payment_reconciliation.png`](docs/screenshots/07_payment_reconciliation.png) | ✅ |

Screenshots of tables must not show unmasked customer data. All screenshots above were checked: none shows a name, email or phone number (the quarantine screenshot uses the orders file, which only contains customer IDs).

- **#2** shows all 19 ELT tasks green, with the `staging` and `marts` task groups expanded.
- **#3** shows both DAGs (filtered by the `nammamart` tag) with schedule `0 9 * * 1-6` and next run `2026-10-21 09:00:00`. The Airflow UI is set to the +05:30 (IST) display timezone (bottom-left), so the next run is shown in IST. Airflow stores it as 03:30 UTC (see [Part C](#part-c-scheduling)).
- **#5** shows the `dq_results` scorecard of every run of both DAGs (110 checks across 6 dimensions each) and, for the latest run of each DAG, the results per quality dimension: ETL and ELT are identical on every line.
- **#6** shows `pipeline_runs` for both DAGs: ETL loads 5,076 rows (rejected rows go to quarantine files), ELT loads all 5,372 rows into raw and flags the 287 rejected ones in staging.

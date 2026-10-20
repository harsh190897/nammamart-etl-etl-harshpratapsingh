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
| 2 | `dags/nammamart_elt_harshpratapsingh.py`, parsing with no import errors | ⬜ | Part B |
| 3 | Helper code in `include/` (quality checks, masking functions) | 🟡 ETL helpers complete (quality rules, masking, transform, load, reconciliation, reports, audit tables); ELT helpers in Part B | [include/nammamart_common.py](include/nammamart_common.py), [include/nammamart_etl_checks.py](include/nammamart_etl_checks.py), [include/nammamart_etl_transform.py](include/nammamart_etl_transform.py), [include/nammamart_etl_load.py](include/nammamart_etl_load.py) |
| 4 | SQL files in `include/sql/` | ⬜ | Part B |
| 5 | `SUBMISSION.md`: Data Issues Log (Part 0) | ✅ | [Part 0](#part-0-discovery-and-data-profiling) |
| 6 | `SUBMISSION.md`: answers to Part C | ✅ Answered, with schedule screenshot | [Part C](#part-c-scheduling) |
| 7 | `SUBMISSION.md`: governance answers (Part E) | ⬜ | [Part E](#part-e-data-governance) |
| 8 | `SUBMISSION.md`: observability results table (Part F) | ⬜ | [Part F](#part-f-observability-tests) |
| 9 | `SUBMISSION.md`: recommendation (Part G) | ⬜ | [Part G](#part-g-recommendation-to-the-cto) |
| 10 | Screenshots: both DAGs green in Graph view, 9:00 AM IST schedule, one quarantine file, `dq_results`, `pipeline_runs`, payment reconciliation | 🟡 6 of 7 (ELT graph pending, Part B) | [Screenshots](#screenshots) |
| 11 | Six business questions, SQL + result, from both pipelines | ⬜ | [Business questions](#business-questions) |
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

**Status:** ⬜ not started.

_To be completed._

---

## Part C: Scheduling

**Status:** ✅ answered for the ETL DAG; the ELT DAG will use the same settings.

DAG page in the Airflow UI (display timezone +05:30): schedule `0 9 * * 1-6`, next run `2026-10-21 09:00:00` IST.

![ETL DAG schedule: 0 9 * * 1-6, next run 2026-10-21 09:00 IST](docs/screenshots/03_schedule_9am_ist.png)

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

**Status:** 🟡 ETL checks done; ELT checks to be added in Part B.

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

Query result: the `dq_results` scorecard per run, and the checks that did not pass in the latest run:

![Query result: dq_results scorecard per run and failing checks](docs/screenshots/05_dq_results.png)

**ELT checks (SQL):** _to be completed in Part B._

---

## Business questions

Each question is answered with one query on the final tables of **both** pipelines.

### Q1. Daily net revenue by zone and by store (delivered orders only, after discount)
_ETL: SQL + result. ELT: SQL + result. To be completed._

### Q2. Revenue and gross margin by product category (margin = revenue − quantity × cost_price)
_To be completed._

### Q3. Average delivery time per store, and the % of delivered orders within 15 minutes
_To be completed._

### Q4. Revenue by payment mode, and the total amount that doesn't reconcile with the payment gateway
_To be completed._

### Q5. Top 10 customers by spend, with loyalty tier (no raw email or phone shown)
_To be completed._

### Q6. Number of rows rejected today, by file and by reason
_To be completed._

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

![Query result: pipeline_runs](docs/screenshots/06_pipeline_runs.png)

_ELT rows to be added in Part B._

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
| 2 | ELT DAG in Graph view, green run | `docs/screenshots/02_elt_graph_green.png` | ⬜ |
| 3 | Schedule showing 9:00 AM IST and the next run | [`docs/screenshots/03_schedule_9am_ist.png`](docs/screenshots/03_schedule_9am_ist.png) | ✅ |
| 4 | One quarantine file | [`docs/screenshots/04_quarantine_file.png`](docs/screenshots/04_quarantine_file.png) | ✅ |
| 5 | Query result: `dq_results` | [`docs/screenshots/05_dq_results.png`](docs/screenshots/05_dq_results.png) | ✅ |
| 6 | Query result: `pipeline_runs` | [`docs/screenshots/06_pipeline_runs.png`](docs/screenshots/06_pipeline_runs.png) | ✅ |
| 7 | Query result: payment reconciliation table | [`docs/screenshots/07_payment_reconciliation.png`](docs/screenshots/07_payment_reconciliation.png) | ✅ |

Screenshots of tables must not show unmasked customer data. All screenshots above were checked: none shows a name, email or phone number (the quarantine screenshot uses the orders file, which only contains customer IDs).

- **#3** shows the DAG header: schedule `0 9 * * 1-6` and next run `2026-10-21 09:00:00`. The Airflow UI is set to the +05:30 (IST) display timezone, so the next run is shown in IST. Airflow stores it as 03:30 UTC (see [Part C](#part-c-scheduling)).
- **#5** and **#6** currently show ETL runs only; they will be retaken after Part B so that they include the ELT DAG's rows as well.

# NammaMart Quick Commerce: ETL & ELT Pipeline Submission

**Learner:** Harsh Pratap Singh
**Module:** FDAIE Module 2, Data Engineering Foundations

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
| 23 | `payments.csv` | PAY0001568@ORD001031, PAY0000354@ORD001937, PAY0000779@ORD001840 | `amount_paid` | SUCCESS payment amount != order_amount | Consistency | SUCCESS => amount_paid == order_amount | Critical | 14 |
| 24 | `payments.csv` | PAY0002219@row188, PAY0002220@row689, PAY0002223@row966 | `order_id` | Payment for an order that does not exist | Consistency | FK order_id -> orders | Critical | 8 |
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
| 39 | `stores.csv` | S07@row8 | `zone` | Missing zone | Completeness | zone NOT NULL (needed for zone revenue) | Critical | 1 |
| 40 | `stores.csv` | S12@row13 | `capacity_orders_per_day` | Capacity is zero | Accuracy | capacity > 0 | Warning | 1 |

### 0.6 Questions for the client before building

1. **Duplicate records with different content.** "Five customers appear twice with the same `customer_id` but a different phone number and area, and two products have an `(old)` copy with a different MRP. Which version is correct: the latest one, the first one, or is there an 'active' flag we should use?"
2. **Negative quantities and negative payments.** "Some orders have quantity 0 or negative and a negative order amount, and some payments have a negative `amount_paid`, yet most are marked DELIVERED. Are these returns or refunds that should be recorded as `RETURNED`/`REFUNDED`, or data-entry errors to reject?"
3. **Orders and payments that don't match.** "28 delivered or returned orders have no payment, 8 payments point to orders we can't find, and 14 successful payments differ from the order amount. Is the gateway export incomplete or delayed, or are these real collection problems finance needs to chase? And should a mismatched order still count as revenue?"

---

## Part C: Scheduling

_To be completed._

## Part E: Data governance

_To be completed._

## Part F: Observability results

_To be completed._

## Part G: Recommendation to the CTO

_To be completed._

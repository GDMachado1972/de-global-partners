# Claude Code Task — Fix pipeline number/label bugs, then deploy the dashboard locally

## Context
Repo: `de-global-partners` (this working directory). Bronze→Silver→Gold PySpark pipeline
+ Streamlit dashboard. A review found several **metric-accuracy bugs** that inflate the
headline numbers, plus two misleading dashboard labels. This task fixes them at the code
level, verifies against ground truth, and runs the dashboard locally.

**All transformation logic stays in PySpark** (`glue/lib/`). Do **not** touch AWS, live
infra, IAM, KMS, or Secrets Manager. Do not add credentials. Keep the credential boundary.
Every fix must keep the test suite green (`pytest -m "not integration"`), and the
real-data integration tests must still pass with `DATA_DIR` set.

You will need the three source CSVs locally. Set `DATA_DIR` / `LANDING` to the folder that
holds `order_items.csv`, `order_item_options.csv`, `date_dim.csv`.

---

## Ground-truth numbers (the source of truth — verify against these)
Measured by direct read of the real files. After the fixes, the local pipeline **must**
reproduce these exactly:

| Quantity | Correct value |
|---|---|
| Raw `order_items` records | 203,519 |
| Silver `order_items` (after cleaning) | 202,692  (−1 malformed, −826 DEVELOPMENT) |
| **Identified customers** (guest-excluded) | **20,059** |
| Guest line-items (excluded from CLV) | 17,502 |
| **Sum of identified-customer CLV** (`cum_net_revenue`) | **$7,769,264.83** |
| **Avg CLV / customer** | **≈ $387.32** |
| Total **net** revenue — ALL orders incl. guests (sales-trends total) | $10,005,987.89 |
| Total **gross** revenue | $9,920,993.35 |
| Add-on / modifier revenue | $84,994.54 |
| **Distinct orders** (all) | **130,587** |
| Locations | 27 |
| CLV tier split | ~20 / 60 / 20 (High 4,012 / Medium 12,035 / Low 4,012) |

If any local build currently shows **20,060 customers** or **$10,005,987.89 as the CLV
total**, that is the bug in Task 2 — fix it, don't "match" it.

---

## Task 1 — Diagnose (do this first, report findings, then fix)
1. Build Gold locally from CSVs and print, for `g_fact_customer_clv_daily` at the latest
   snapshot: row count, `sum(cum_net_revenue)`, `avg(cum_net_revenue)`, and whether any
   `user_id` is an empty/blank string.
   - `make gold-local LANDING=<path>` or run `glue/jobs/job3_silver_to_gold_clv_daily.py`
     in `--source_mode csv`, or call `glue.lib.gold.build_clv_daily` directly on the
     Silver `s_orders`.
2. Confirm the hypothesis: guest `user_id` values that are **empty/whitespace strings**
   (not NULL) are not being flagged as guests, so they collapse into one "" customer that
   absorbs all guest revenue → 20,060 customers and a ~$10.0M CLV total.

## Task 2 — Fix the guest flag (root cause)
In `glue/lib/cleaning.py`, `clean_order_items`: **normalize blank `user_id` to NULL**
before deriving `is_guest`, so both NULL and empty/whitespace strings are treated as guests.

- Replace the current `is_guest` derivation so that:
  - `user_id` is set to NULL when `trim(user_id) == ""` (or is already null), and
  - `is_guest = user_id IS NULL` after that normalization.
- This keeps the local CSV result unchanged (CSV blanks already parse as NULL → 20,059) and
  fixes the live/RDS case where guests are empty strings.

**Acceptance:** local CLV rebuild now shows **20,059** customers and
`sum(cum_net_revenue) == 7,769,264.83`; there is **no** empty-string `user_id` in Gold.
Add a unit test in `tests/test_cleaning.py` asserting a row with `user_id = ""` (and one
with `user_id = "   "`) yields `is_guest = True` and a normalized NULL `user_id`.

## Task 3 — Fix the dashboard number/label issues (`dashboard/app.py`)
1. **CLV Overview** — make the labels unambiguous so CLV is never conflated with total net:
   - "Total CLV (net)" → **"Total CLV — identified customers"**, value = sum of
     `cum_net_revenue` from `g_fact_customer_clv_daily` (≈ $7.77M).
   - Add a separate, clearly-labelled metric **"Total net revenue (all orders, incl.
     guests)"** ≈ $10.0M, sourced from `g_fact_sales_trends` (`sum(net_revenue)`), so both
     numbers are visible and distinct. Avg CLV/customer must be ≈ $387, not $498.
2. **Sales Trends "Orders" KPI double-counts.** `g_fact_sales_trends` is at grain
   `date × restaurant_id × item_category`, so summing its `orders` counts multi-category
   orders multiple times (~170,642 vs the true 130,587 distinct orders). Fix by **either**:
   - relabelling the KPI to **"Category order-lines"** (make clear it is not distinct
     orders), **or**
   - showing a true distinct-order figure derived from an order-grain source (not by summing
     the category-level mart). Do not present ~170k as "orders."
   Add a one-line caption noting the grain.
3. **Loyalty view** — two fixes:
   - Column mismatch: `view_loyalty` references `avg_spend` / `avg_orders_per_customer`,
     but `g_fact_loyalty_impact` provides `avg_clv`, `avg_orders`, `repeat_order_rate`,
     `customers`. Align the view to the real columns so `avg_orders` renders (either rename
     the view refs, or add the missing measures to `glue.lib.gold.build_loyalty_impact`).
   - Add a caption: loyalty and non-loyalty cohorts **overlap** (loyalty is a per-order
     flag, so a customer with both order types is counted in both) — the cohort customer
     counts therefore do **not** sum to the identified-customer total.

**Acceptance:** dashboard CLV Overview shows ≈ $7.77M / ≈ $387 / 20,059; Sales Trends no
longer labels ~170k as distinct orders; Loyalty view renders `avg_orders` and shows the
overlap caption.

## Task 4 — Tests
- Run `pytest -m "not integration"` (must pass) and, with `DATA_DIR` set, the full suite
  including integration (must pass). Update assertions only where the *correct* value
  changed; do **not** relax a test to accept a wrong number.
- Confirm the integration tests still assert 202,692 Silver rows, 20,059 CLV customers,
  100% calendar coverage, and $7,769,264.83 identified CLV.

## Task 5 — Deploy the dashboard locally (the deliverable)
1. Ensure deps: `pip install -r requirements.txt` (needs `streamlit`, `pyarrow`, `pyspark`,
   `delta-spark`, `holidays`, `pandas`). Java 17+ must be on PATH for Spark.
2. Launch with the local backend:
   ```
   make dashboard LANDING=<path-to-csvs>
   # equivalently:
   DATA_BACKEND=local LANDING=<path-to-csvs> streamlit run dashboard/app.py
   ```
3. Verify it boots without errors and the first load builds the marts, then confirm the
   corrected figures render: **20,059 customers, ≈ $7.77M total CLV, ≈ $387 avg**, Sales
   Trends orders relabelled, Loyalty overlap caption present.
4. Print the local URL (default http://localhost:8501) for the user to open.

## (Optional) Task 6 — Correct the final report, if present in the repo
If `DE_Global_Partners_Final_Report.docx` (or a source that generates it) is in the repo,
correct these figures to match ground truth:
- "Total net revenue (CLV…) $10,005,987.89" → split into **Total CLV (identified
  customers) $7,769,264.83** and **Total net revenue (all orders) $10,005,987.89**.
- Avg CLV/customer $498.80 → **≈ $387.32**; identified customers 20,060 → **20,059**.
- Sales Trends "170,642 orders" → **130,587 distinct orders** (or "category order-lines").
- Add the loyalty-cohort-overlap note.
- Figure caption "SDD v1.1" → **v1.2**; capitalize "July 11th"; consider redacting the raw
  AWS account id.
Do not fabricate any figure — reconcile each against the live Gold output.

---

## Hard constraints
- PySpark for all transforms; no pandas reimplementations of pipeline logic.
- No AWS calls, no infra/IAM/KMS/Secrets changes, no credentials in code or config.
- Keep every existing test green; add tests for the new guest-normalization behavior.
- One focused commit with an accurate message, e.g.:
  `Fix: normalize blank user_id to guest (correct CLV total/customer count); relabel sales-trends orders; fix loyalty view + overlap note`.

## Definition of done
- [ ] Local Gold rebuild: 20,059 customers, CLV sum $7,769,264.83, avg ≈ $387, no empty-string user_id.
- [ ] Sales-trends "orders" no longer presented as ~170k distinct orders.
- [ ] Loyalty view renders correctly with an overlap caption.
- [ ] `pytest` green (unit + integration), with a new blank-user_id test.
- [ ] Dashboard runs locally on the `local` backend and shows the corrected numbers.
- [ ] Changes committed with an accurate message (and report corrected if it lives in the repo).

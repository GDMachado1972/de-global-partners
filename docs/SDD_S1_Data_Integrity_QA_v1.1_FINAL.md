# SDD §1 — Source Data Integrity QA (v1.1 — FINAL, all three files)
**Project:** DE Global Partners — Alltown Fresh (Global Partners LP)
**Scope:** Step 1 integrity verification prior to SME design gate
**Supersedes:** v1.0 (partial — order_items.csv was absent)
**Files (SHA-256 first 16):** order_items `03c8bf5030093e64` (37.9 MB) ·
order_item_options `9bd8a388b48d09e0` · date_dim `4168f66d142e8263`

> Tags: **[FACT]** measured directly · **[INFERENCE]** derived · **[ASSUMPTION]** confirm with SME ·
> **[NEEDS-VALIDATION]** contradicts prior claim / needs a decision.

---

## 1.0 Headline verdicts
| File | Rows | Verdict |
|------|------|---------|
| `order_items.csv` | **203,519** | Usable — **but analysis window is 2020–2024, not 2023** (§1.3 🚩A) |
| `order_item_options.csv` | 193,017 | Usable — **no discount signal exists** (§1.4 🚩B) |
| `date_dim.csv` | 365 | Clean — **but covers only 2023, matches ~40% of orders** (§1.3 🚩A) |

**Two findings reshape the design and need SME decisions before the SDD is finalized:**
🚩A — the calendar dimension covers 8× less time than the orders span.
🚩B — the discount/net-revenue metric has no data to compute from anywhere in the dataset.

---

## 1.1 Row-count reconciliation — RESOLVED ✅
**[FACT]** `order_items.csv`: 203,532 raw lines − 1 header = 203,531 physical lines, but
**203,519 logical records** (pandas). The 12-row gap = records with embedded newlines in
quoted text fields (e.g. `ITEM_NAME`).
**[INFERENCE]** This explains the spec-vs-config discrepancy exactly: **spec's 203,519 =
correct record count**; config's "~203,531" = raw-line miscount inflated by embedded
newlines. Use **203,519** as the record-count ground truth. Any line-based ingest (naive
`split`, mis-configured bulk load) will over-count — the loader must respect RFC-4180
quoting.

---

## 1.2 `date_dim.csv` — CLEAN (as data), SCOPE-LIMITED (as coverage)
**[FACT]** 365 rows; 2023-01-01→2023-12-31; 0 missing/dup days; `date_key` DD-MM-YYYY parses
dayfirst with 0 failures; `month` is int 1–12; 12 holidays. Internally flawless.
**[NEEDS-VALIDATION]** It only spans **2023**, while orders span **2020–2024** (§1.3). As-is
it will match only 39.6% of orders. **The dimension must be regenerated** to cover the true
order date range (recommend generating it programmatically from `min/max(CREATION_TIME_UTC)`
rather than shipping a static 2023 file).

---

## 1.3 `order_items.csv` — profile

| Field | Finding | Tag |
|-------|---------|-----|
| Records | 203,519 (grain = line item; (ORDER_ID,LINEITEM_ID) unique, 0 dup grain) | [FACT] |
| Distinct orders | 131,328 | [FACT] |
| `USER_ID` nulls | **17,808 (8.75%)** = guest/anonymous orders; 20,174 distinct real customers | [FACT] |
| `PRINTED_CARD_NUMBER` | 157,435 null (77.4%) — populated only for loyalty | [FACT] |
| `IS_LOYALTY` | TRUE 46,084 (22.6%) / FALSE 157,435 | [FACT] |
| `CURRENCY` | 100% USD — single-currency confirmed | [FACT] |
| `RESTAURANT_ID` | **28 distinct locations** (the sole location key) | [FACT] |
| `ITEM_CATEGORY` | 45 categories (Breakfast, Sandwiches, Smoothies…), 1 null | [FACT] |
| `ITEM_PRICE` | min 0, median 8.00, max **5,000** (outlier — catering or error?), 156 zero-price | [FACT] |
| `ITEM_QUANTITY` | median 1, max **500** (bulk), 1 row qty=0 | [FACT] |
| **Gross item revenue** | **Σ(ITEM_PRICE×ITEM_QUANTITY) = $9,933,754.69** | [FACT] |

### 🚩 FLAG A (CRITICAL) — orders span 2020–2024, calendar covers only 2023
**[FACT]** `CREATION_TIME_UTC` year distribution:
`2020: 10,210 · 2021: 43,448 · 2022: 60,015 · 2023: 80,579 · 2024: 9,080`.
Range 2020-04-21 → 2024-02-21. Joining to the 2023-only `date_dim` matches **80,579 / 203,519
= 39.6%**; **60.4% (122,940 rows) would be dropped** on an inner calendar join.
**[INFERENCE]** The config/seed "full-year 2023" window is wrong — 2023 is only the largest
single year, not the extent. **Impact on the PRIMARY goal:** CLV is inherently cumulative
over the *entire* customer relationship; restricting to 2023 would discard 60% of history and
break "how LTV evolves per customer." **Recommendation:** regenerate `date_dim` to span the
full order range (2020-2024) and treat the whole history as the analysis window; confirm with
SME. **[NEEDS-VALIDATION]**

### Cleaning rules this file requires (Bronze→Silver) — [ASSUMPTION], confirm
1. **1 malformed record** (row idx 16529): `LINEITEM_ID/ITEM_CATEGORY/ITEM_NAME` null,
   `ITEM_QUANTITY=0` — an embedded-newline artifact. Drop or quarantine.
2. **826 test rows** where `APP_NAME='Alltown Fresh - DEVELOPMENT'` — exclude from all
   metrics. (`Alltown Neighborhood Perks` 1,270 appears to be a real channel — keep.)
3. **187 timestamps** are valid ISO-8601 *without* fractional seconds (e.g. `...:59Z`).
   **[FACT]** parsing with `format='ISO8601'` recovers all 187 (0 failures). Do **not** drop
   them — configure the Silver parser for ISO8601, not a fixed millisecond mask.
4. `USER_ID` null = guest: **exclude from CLV/RFM**, **keep in sales/location totals**.

---

## 1.4 `order_item_options.csv` — profile & the discount question
**[FACT]** 193,017 rows, 6 cols, no nulls, keys clean. `OPTION_QUANTITY` constant = 1
(no information). 2,299 exact duplicate rows (all $0 modifiers) → dedup to 190,718.
`OPTION_GROUP_NAME` top: Milk Options, Bacon Egg & Cheese, Breakfast Burrito…
Some `OPTION_NAME` contain quoted commas → readers need RFC-4180 quote handling.

### 🚩 FLAG B (CRITICAL) — no discount signal exists anywhere
**[FACT]** `OPTION_PRICE`: min 0.00, max 8.00 — **0 negative rows** (raw-grep confirmed).
`ITEM_PRICE`: **0 negative rows** too. **The negative-price discount signal does not exist in
either file.**
**[NEEDS-VALIDATION]** This overturns the config's "verified" premise
(`option_price<0 = discount`, `net = gross + Σ(option_price×qty)`). Consequences:
- **Metric #6 (Pricing & Discount Effectiveness)** and the gross-vs-net split **cannot be
  built as specified** — there is no discount to net out.
- Options are strictly **additive** (paid add-ons raise revenue; free modifiers are $0).
- **Options for the SME:** (i) redefine "discount effectiveness" against a signal that does
  exist (e.g. paid-modifier attach-rate and its revenue lift; price-tier / promotional-item
  analysis via `ITEM_PRICE=0` items), or (ii) formally descope Metric #6 until a source with
  discount data is provided. Do not fabricate a net-revenue formula.

---

## 1.5 Cross-file integrity — EXCELLENT (joins are sound)
**[FACT]**
- `order_item_options` → `order_items` on (ORDER_ID, LINEITEM_ID): **15 orphans / 102,712
  (0.01%)** — effectively perfect referential integrity.
- **50.5%** of item line-items carry ≥1 option.
- Only blocker to the calendar join is coverage (🚩A), not key quality.

---

## 1.6 Decisions required from SME / Gerardo before SDD sign-off
| # | Decision | Recommended default |
|---|----------|---------------------|
| D1 | Analysis window & calendar (🚩A) | Regenerate `date_dim` to full 2020–2024 range; analysis window = full order history |
| D2 | Discount / Metric #6 (🚩B) | Redefine as paid-modifier attach-rate + revenue lift; OR descope with written note |
| D3 | Cleaning rules (§1.3) | Drop malformed row; exclude DEVELOPMENT app; ISO8601 parser; dedup options; guest = exclude-from-CLV |
| D4 | Outliers | Cap/flag ITEM_PRICE=5,000 & QTY=500 as QA review, not silent drop |
| D5 | Standing assumptions | CLV = net-historical-revenue-to-date (non-predictive over FULL history), cumulative daily snapshot — confirm |

## 1.7 Ground-truth numbers (use as SDD evidence — no placeholders)
- Records: order_items **203,519** · options **193,017** (190,718 deduped) · date_dim 365
- Customers: **20,174** real + **17,808** guest line-items · Locations: **28**
- Loyalty share: **22.6%** · Categories: **45** · Currency: 100% USD
- Order span: **2020-04-21 → 2024-02-21** · Gross item revenue: **$9,933,754.69**
- Discount signal: **none** · Referential integrity (options→items): **99.99%**

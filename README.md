# DE Global Partners — Daily CLV & Behavioral Analytics Pipeline

Production-grade, AWS-native pipeline that computes **daily Customer Lifetime Value** and
behavioral analytics for Alltown Fresh (Global Partners LP) ordering data, served through a
Streamlit dashboard. See `docs/DE_Global_Partners_SDD_v1_2.docx` for the approved design.

**Status:** SDD v1.2 approved by SME (Bansari Modi); build in progress against the §13 critical path.

## Architecture (SDD §6)
```
SQL Server (Windows) → Glue JDBC → S3 Bronze (Delta) → Glue ETL (PySpark) → S3 Silver
→ Glue ETL → S3 Gold (Delta) → Athena v3 + pyathena → Streamlit on App Runner
```
Cross-cutting: Glue Workflows (daily cron ~06:00 UTC) · Job Bookmarks · CloudWatch + SNS ·
Secrets Manager + KMS · IAM least-privilege. Diagram: `docs/DE_Global_Partners_Architecture.drawio`.

## Repository layout
```
config/    project_config.yaml     # single source of naming truth
infra/     bootstrap_infra.py      # boto3: S3/KMS/Glue-seccfg/Athena v3 (+emits IAM/Secrets)
glue/lib/  spark_session, schema, cleaning, calendar_gen   # unit-tested transform logic
glue/jobs/ job1_ingest_sqlserver_to_bronze, job2_bronze_to_silver, ...
glue/workflow/  create_workflow.py # Glue Workflow + triggers + failure->SNS + reload
glue/lib/  gold.py                 # CLV snapshot + 6 marts (implemented)
dashboard/ app.py + data_access.py # Streamlit 6-view dashboard (Athena / local)
tests/     pytest — synthetic unit + real-data integration
docs/      SDD, QA report, architecture diagram
```

## Two SME decisions baked in
- **D1 (approved):** the shipped `date_dim` covers only 2023 but orders span **2020-04-21 →
  2024-02-21**, so the calendar is **generated in Silver** from the true min/max order date
  (100% join coverage). See `glue/lib/calendar_gen.py`.
- **D2 (descope & document):** the dataset contains **no discount signal** (0 negative prices
  in either file), so the specified "Pricing & Discount Effectiveness" analysis **cannot be
  performed** and is documented as a data limitation. An add-on/modifier revenue view is
  retained only as an optional supplement (sixth dashboard slot).

## Credential boundary (hard rule)
Gerardo owns all AWS credentials, IAM, KMS approvals, and Secrets Manager values. The infra
script **provisions non-credential resources** and **emits** the IAM policy JSON and the
`aws secretsmanager` command for him to apply — it never creates IAM or handles secret values.

## Quickstart (local dev / test)
```bash
pip install -r requirements.txt

# run unit tests (no data needed)
pytest -m "not integration"

# run everything incl. real-data checks (asserts 202,692 Silver rows, 100% calendar coverage)
DATA_DIR=/path/to/the/three/csvs pytest
```

## Provision infrastructure (Gerardo, with his AWS creds)
```bash
python infra/bootstrap_infra.py --config config/project_config.yaml --dry-run   # preview
python infra/bootstrap_infra.py --config config/project_config.yaml             # apply
# then: create the SQL Server secret (printed command) + Glue connection, attach the emitted IAM policy
```

## Run Silver locally (CSV fallback, SDD risk R5)
```bash
spark-submit glue/jobs/job2_bronze_to_silver.py --source_mode csv \
  --landing_path /path/to/csvs --silver_path /tmp/silver
```

## Deploy + wire the Glue Workflow (Gerardo, with AWS creds)
```bash
make deploy                                   # upload job scripts + glue_lib.zip + config
make workflow-dryrun                          # preview the DAG (zero AWS calls)
make workflow-apply ROLE_ARN=arn:aws:iam::<acct>:role/global-partners-dev-glue \
     ALERT_EMAIL=you@example.com              # create workflow, triggers, failure->SNS
make reload DATE=2024-01-15 ROLE_ARN=...      # reprocess one batch_date (idempotent)
```
**DAG:** `schedule(06:00 UTC) → J1 → J2 → {J3 ∥ J4} → J5`; any job `FAILED/TIMEOUT/STOPPED`
→ EventBridge → SNS alert. Reload re-runs the workflow with `batch_date`, overwriting only
that partition (Delta, SDD §9.4). Jobs 3–5 are placeholders until the Gold increment.

## Canonical validated figures (source of truth for QA gates)
| Metric | Value |
|---|---|
| Raw order_items records | 203,519 |
| Silver order_items (post-clean) | **202,692** (−1 malformed, −826 DEVELOPMENT) |
| Options deduped | 190,718 (−2,299) |
| Order span | 2020-04-21 → 2024-02-21 (1,402 calendar days) |
| Identified customers / guest line-items | 20,059 / 17,502 (post-clean) |
| Locations (post-clean) | 27 (a test-only RESTAURANT_ID dropped with DEVELOPMENT rows) |
| Gross item revenue (post-clean) | $9,920,993.35 |
| Discount signal | none (D2) |

## Gold layer (implemented — validated on real data)
`job3` builds `g_fact_customer_clv_daily` (grain user_id × snapshot_date; cumulative net
revenue, orders, recency, as-of-date High/Medium/Low tiers). `job4` builds six marts:
RFM, churn, sales-trends, loyalty-impact, location-performance, and the optional add-on
supplement (D2). `job5` runs the QC gates and (in AWS) registers the Athena v3 tables.

| Gold check | Value |
|---|---|
| Identified customers (CLV/RFM/churn) | 20,059 |
| CLV tier split (latest snapshot) | ~20 / 60 / 20 (High/Medium/Low) |
| Identified-customer cumulative net revenue | $7,769,264.83 |
| Total net revenue (sales-trends, incl. guests) | $10,005,987.89 |
| Total gross revenue | $9,920,993.35 |
| Locations ranked | 27 |

```bash
make gold-local LANDING=/path/to/csvs      # build CLV + marts locally
```

## Dashboard (Streamlit — 6 views + CLV overview)
`dashboard/app.py` serves CLV Overview, Customer Segmentation (RFM), Churn Risk, Sales
Trends, Loyalty Impact, Location Performance, and Pricing & Add-on. The sixth view shows
the **D2 data-limitation note** (no discount signal) plus the optional add-on supplement.

Two backends via `DATA_BACKEND`:
- `athena` — queries the Gold tables through pyathena (engine-v3 workgroup).
- `local` — rebuilds the marts from CSVs with the same tested PySpark transforms (no Delta/
  network); used for local dev and the recorded-video descope.

```bash
make dashboard LANDING=/path/to/csvs                 # local backend
DATA_BACKEND=athena ATHENA_S3_STAGING=s3://global-partners-dev-athena-results/athena/ \
  streamlit run dashboard/app.py                     # Athena backend (needs AWS creds)
```

## CI/CD
- **CI** (`.github/workflows/ci.yml`): a secrets/data guard (fails if `.env`, `*.pem`,
  `credentials`, `glue_job_policy.json`, or `*.csv` are committed), then lint, infra +
  workflow **dry-run validation**, and unit tests. Concurrency cancels superseded runs.
  Integration tests are guarded (`if: false`) since the CSVs aren't in the repo.
- **CD** (`.github/workflows/deploy.yml`): on push to `main`, assumes an AWS role via
  **GitHub OIDC** (no long-lived keys), runs `make deploy` (uploads job scripts + `glue_lib.zip`
  + config), and creates/updates the Glue Workflow. Auto-skips until `AWS_DEPLOY_ROLE_ARN`
  is set. Configure repo variables `AWS_DEPLOY_ROLE_ARN`, `GLUE_ROLE_ARN`, `AWS_REGION`.

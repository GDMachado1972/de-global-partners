# Contributing — DE Global Partners

Short guide for working in this repo. See `README.md` for architecture and `docs/` for the
approved SDD v1.2.

## Branch strategy
Intended model: **`main`** (graded / submission-ready) ← **`dev`** (integration) ←
**`feature/*`** (one branch per increment). Approval unblocks merge into `main`.

> **Capstone note:** for the July-10 deadline this project commits **directly to `main`**.
> The `dev`/`feature` flow above is the production pattern to adopt once the deadline passes;
> CI already triggers on `main`, `dev`, and `feature/**`.

## Commit messages
Keep them scoped and descriptive, e.g.:
```
Increment 4: Gold CLV-daily snapshot + RFM/churn/sales/loyalty/location marts
Fix: ISO-8601 parser recovers timestamps without fractional seconds
```

## Dev workflow (one-liners)
```bash
make install        # install deps
make test           # unit tests (no data needed)
make test-all DATA_DIR=/path/to/csvs   # + real-data checks (asserts 202,692 Silver rows)
make infra-dryrun   # preview infra, zero AWS calls
make silver-local LANDING=/path/to/csvs   # build Silver from CSVs -> /tmp/silver
```
**Windows:** `make` isn't native. Run these under **Git Bash** or **WSL**, or install it
(`choco install make`). Without `make`, run the underlying commands from the Makefile directly.
`silver-local`'s Delta write downloads the Delta jars from Maven on first run (needs network).

## Credential boundary (non-negotiable)
Gerardo owns all AWS credentials, IAM, KMS, and Secrets Manager values. Code and scripts here
**never** embed, request, or handle secrets. `infra/bootstrap_infra.py` provisions
non-credential resources and *emits* the IAM policy JSON + the `aws secretsmanager` command
for you to apply. Never commit `.env`, `credentials`, `*.pem`, or `infra/glue_job_policy.json`
(all gitignored). Never commit the source CSVs.

## Adding a transform or mart
1. Put pure PySpark logic in `glue/lib/` (testable off-cluster).
2. Add a synthetic unit test (CI-safe) **and**, where a ground-truth number exists, a
   `@pytest.mark.integration` assertion against the real data.
3. Wire it into the relevant `glue/jobs/` entrypoint.
4. Keep every material claim/number tagged `[FACT]`/`[INFERENCE]`/`[ASSUMPTION]`/
   `[NEEDS-VALIDATION]` in docs, and `make test` green before pushing.

## Conventions
- Names: `global-partners-dev-{zone}` (buckets), `global-partners-dev-{job}` (Glue jobs).
- Region `us-east-1`, env `dev`. All transformation logic in PySpark. AWS-only.
- Every tech choice carries a "WHY" (see SDD §6.1).

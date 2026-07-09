# DE Global Partners — developer shortcuts
# Usage: make <target>   (Windows: run under Git Bash or WSL; see CONTRIBUTING.md)

PYTHON   ?= python3
CONFIG   ?= config/project_config.yaml
DATA_DIR ?=
LANDING  ?= $(DATA_DIR)
SILVER_OUT ?= /tmp/silver
export SPARK_LOCAL_IP := 127.0.0.1

.DEFAULT_GOAL := help

.PHONY: help install lint test test-all infra-dryrun infra-apply silver-local \
        gold-local workflow-dryrun workflow-apply deploy reload clean

help:            ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install:         ## Install Python dependencies
	$(PYTHON) -m pip install -r requirements.txt

lint:            ## Syntax-check all Python (glue, infra, streamlit)
	$(PYTHON) -m compileall -q glue infra streamlit && echo "lint OK"

test:            ## Unit tests only (no data required)
	$(PYTHON) -m pytest -m "not integration"

test-all:        ## All tests incl. real-data checks — requires DATA_DIR=/path/to/csvs
	@test -n "$(DATA_DIR)" || (echo "Set DATA_DIR=/path/to/the/three/csvs"; exit 1)
	DATA_DIR=$(DATA_DIR) $(PYTHON) -m pytest

infra-dryrun:    ## Preview infra provisioning (zero AWS calls)
	$(PYTHON) infra/bootstrap_infra.py --config $(CONFIG) --dry-run

infra-apply:     ## Provision infra (needs your AWS creds configured)
	$(PYTHON) infra/bootstrap_infra.py --config $(CONFIG)

silver-local:    ## Build Silver from CSVs locally — requires LANDING=/path/to/csvs
	@test -n "$(LANDING)" || (echo "Set LANDING=/path/to/the/three/csvs"; exit 1)
	$(PYTHON) glue/jobs/job2_bronze_to_silver.py --source_mode csv \
	  --landing_path $(LANDING) --silver_path $(SILVER_OUT)

gold-local:      ## Build Gold (CLV + marts) from CSVs locally — requires LANDING=/path/to/csvs
	@test -n "$(LANDING)" || (echo "Set LANDING=/path/to/the/three/csvs"; exit 1)
	$(PYTHON) glue/jobs/job3_silver_to_gold_clv_daily.py --source_mode csv \
	  --landing_path $(LANDING) --gold_path /tmp/gold
	$(PYTHON) glue/jobs/job4_silver_to_gold_marts.py --source_mode csv \
	  --landing_path $(LANDING) --gold_path /tmp/gold

workflow-dryrun: ## Preview Glue Workflow wiring (zero AWS calls)
	$(PYTHON) glue/workflow/create_workflow.py --config $(CONFIG) --dry-run

workflow-apply:  ## Create/update the Glue Workflow — needs ROLE_ARN=arn:aws:iam::...:role/...
	@test -n "$(ROLE_ARN)" || (echo "Set ROLE_ARN=<your glue role arn>"; exit 1)
	$(PYTHON) glue/workflow/create_workflow.py --config $(CONFIG) --role-arn $(ROLE_ARN) \
	  $(if $(ALERT_EMAIL),--alert-email $(ALERT_EMAIL),)

reload:          ## Reprocess one batch_date — make reload DATE=YYYY-MM-DD ROLE_ARN=...
	@test -n "$(DATE)" -a -n "$(ROLE_ARN)" || (echo "Set DATE=YYYY-MM-DD ROLE_ARN=..."; exit 1)
	$(PYTHON) glue/workflow/create_workflow.py --config $(CONFIG) --role-arn $(ROLE_ARN) --reload $(DATE)

deploy:          ## Package glue/lib + upload job scripts & config to the scripts bucket (needs AWS creds)
	@BUCKET=$$(grep -A6 '^buckets:' $(CONFIG) | grep 'scripts:' | awk '{print $$2}'); \
	  echo "packaging glue/lib -> glue_lib.zip"; \
	  (cd glue && zip -qr /tmp/glue_lib.zip lib -x '*__pycache__*'); \
	  echo "uploading to s3://$$BUCKET/ ..."; \
	  aws s3 cp /tmp/glue_lib.zip s3://$$BUCKET/lib/glue_lib.zip; \
	  aws s3 sync glue/jobs s3://$$BUCKET/jobs --exclude '*__pycache__*'; \
	  aws s3 cp $(CONFIG) s3://$$BUCKET/config/project_config.yaml; \
	  echo "deploy complete"

clean:           ## Remove local build/test artifacts
	rm -rf **/__pycache__ .pytest_cache spark-warehouse metastore_db derby.log \
	  infra/glue_job_policy.json $(SILVER_OUT) 2>/dev/null; echo "cleaned"

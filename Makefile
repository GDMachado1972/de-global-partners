# DE Global Partners — developer shortcuts
# Usage: make <target>   (Windows: run under Git Bash or WSL; see CONTRIBUTING.md)

PYTHON   ?= python3
CONFIG   ?= config/project_config.yaml
DATA_DIR ?=
LANDING  ?= $(DATA_DIR)
SILVER_OUT ?= /tmp/silver
export SPARK_LOCAL_IP := 127.0.0.1

.DEFAULT_GOAL := help

.PHONY: help install lint test test-all infra-dryrun infra-apply silver-local clean

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

clean:           ## Remove local build/test artifacts
	rm -rf **/__pycache__ .pytest_cache spark-warehouse metastore_db derby.log \
	  infra/glue_job_policy.json $(SILVER_OUT) 2>/dev/null; echo "cleaned"

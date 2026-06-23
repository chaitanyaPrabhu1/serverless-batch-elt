.DEFAULT_GOAL := help
SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
TF := terraform -chdir=terraform

.PHONY: help venv test local-run package plan apply destroy dbt-build clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

venv: ## Create .venv and install dev dependencies
	python3 -m venv $(VENV)
	$(PIP) install --quiet --upgrade pip
	$(PIP) install --quiet -r requirements-dev.txt
	@echo "venv ready -> activate with: source $(VENV)/bin/activate"

test: ## Run unit tests (moto-mocked S3, fake API)
	$(PY) -m pytest -q

local-run: ## Run ingest + transform end-to-end into ./.local_lake (real API, local FS)
	STORAGE_BACKEND=local LOCAL_LAKE_DIR=.local_lake $(PY) scripts/local_run.py

package: ## Assemble the Lambda deployment package into build/package/
	rm -rf build/package
	mkdir -p build/package
	cp lambda/*.py build/package/
	cp config/cities.json build/package/
	@echo "package assembled -> build/package/"

plan: package ## terraform plan (requires AWS creds + -var pandas_layer_arn)
	$(TF) init -input=false
	$(TF) plan

apply: package ## terraform apply
	$(TF) init -input=false
	$(TF) apply

destroy: ## terraform destroy (tear everything down)
	$(TF) destroy

dbt-build: ## Run dbt deps + run + test (requires DATA_BUCKET etc. exported)
	cd dbt && dbt deps && dbt run --target prod && dbt test --target prod

clean: ## Remove build artifacts and the local lake
	rm -rf build .local_lake dbt/target dbt/dbt_packages .pytest_cache

POSTGRES_USER=sms
POSTGRES_DB=sms
LOCAL_POSTGRES_HOST=localhost
LOCAL_POSTGRES_PORT=5555
LOCAL_GATEWAY_PORT=8888

POSTGRES_PORT=5432

CURRENT_VERSION := $(shell uv run python -c "from viva_api import version;print(f'{version.__version__}')")
VENV := $(shell uv run which python)
REPO_DIR := $(shell uv run python -c "from pathlib import Path; import os; print(Path(os.getcwd()).absolute())")

# "postgresql://sms:$$pw@localhost:$(port)/sms?sslmode=disable""postgresql://sms:$$pw@localhost:$(port)/sms?sslmode=disable"

.PHONY: install
install: ## Install the uv environment and install the pre-commit hooks
	@echo "🚀 Creating virtual environment using uv"
	@uv sync
	@uv run pre-commit install

.PHONY: check
check: ## Run code quality tools.
	@echo "🚀 Checking lock file consistency with 'pyproject.toml'"
	@uv lock --locked
	@echo "🚀 Linting code: Running pre-commit"
	@uv run pre-commit run -a
	@echo "🚀 Static type checking: Running mypy"
	@uv run mypy
	@echo "🚀 Checking for obsolete dependencies: Running deptry"
	@uv run deptry .

.PHONY: clean_cache
clean_cache:
	@rm -rf .pytest_cache
	@rm -rf .mypy_cache
	@rm -rf .ruff_cache
	@find . -name '__pycache__' -exec rm -r {} + -o -name '*.pyc' -delete
	@uv cache clean --force
	@rm -rf .results_cache && mkdir .results_cache && touch .results_cache/.gitkeep

.PHONY: clean
clean:
	@rm -rf .pytest_cache
	@rm -rf .mypy_cache
	@rm -rf .ruff_cache
	@find . -name '__pycache__' -exec rm -r {} + -o -name '*.pyc' -delete
	@uv cache clean
	@rm -r uv.lock
	@uv lock --no-cache

.PHONY: test
test: ## Test the code with pytest
	@echo "🚀 Testing code: Running pytest"
	@uv run python -m pytest -ra --cov --cov-config=pyproject.toml --cov-report=xml

.PHONY: logtest
logtest: ## Test the code with pytest
	@echo "🚀 Testing code: Running pytest"
	@uv run python -m pytest \
		--cov \
		--cov-config=pyproject.toml \
		--cov-report=xml \
		--log-file=tests/.pytest.log \
		--log-file-level=ERROR

.PHONY: build
build: clean-build ## Build wheel file
	@echo "🚀 Creating wheel file"
	@uvx --from build pyproject-build --installer uv

.PHONY: clean-build
clean-build: ## Clean build artifacts
	@echo "🚀 Removing build artifacts"
	@uv run python -c "import shutil; import os; shutil.rmtree('dist') if os.path.exists('dist') else None"

# ----------------------------------------------------------------------------
# PyPI publish — mirrors ../vecoli_deployment's publish workflow so that
# `pip install viva-api` ships the full end-user bundle (app.cli, app.tui,
# app.gui, app_data_service, etc.) to stakeholders.
#
# Usage:
#   make publish                              # reads token from ~/.ssh/.pypi-viva-api (falls back to ~/.ssh/.pypi-sms-api)
#   make publish token=pypi-AgEN...           # explicit token
#   make upload_package token=pypi-...        # upload already-built dist/
# ----------------------------------------------------------------------------

.PHONY: sync_publish
sync_publish: ## Recreate uv.lock and sync all groups (no-cache) for a clean publish
	@echo "🚀 Refreshing uv environment for publish"
	@uv cache clean
	@rm -f uv.lock
	@uv lock --no-cache
	@uv sync --no-cache --all-groups

.PHONY: build_package
build_package: clean-build ## Build sdist + wheel into dist/ using the standard PEP 517 builder
	@echo "🚀 Building viva_api package (sdist + wheel)"
	@uvx --from build pyproject-build --installer uv

.PHONY: upload_package
upload_package: ## Upload dist/* to PyPI (requires token=... and a pre-built dist/)
	@if [ -z "$(token)" ]; then \
		echo "❌ upload_package requires token=..."; exit 1; \
	fi
	@echo "🚀 Uploading viva_api package to PyPI"
	@uv publish --no-cache --token $(token)

.PHONY: publish
publish: ## Publish viva_api to PyPI (sync → build → upload). Reads token from ~/.ssh/.pypi-viva-api (or ~/.ssh/.pypi-sms-api) unless token=... is set.
	@TOKEN="$(token)"; \
	if [ -z "$$TOKEN" ] && [ -f "$$HOME/.ssh/.pypi-viva-api" ]; then \
		TOKEN=$$(cat "$$HOME/.ssh/.pypi-viva-api"); \
	fi; \
	if [ -z "$$TOKEN" ] && [ -f "$$HOME/.ssh/.pypi-sms-api" ]; then \
		TOKEN=$$(cat "$$HOME/.ssh/.pypi-sms-api"); \
	fi; \
	if [ -z "$$TOKEN" ]; then \
		echo "❌ No PyPI token. Set token=... or write one to ~/.ssh/.pypi-viva-api (or ~/.ssh/.pypi-sms-api)"; \
		exit 1; \
	fi; \
	$(MAKE) sync_publish && \
	$(MAKE) build_package && \
	$(MAKE) upload_package token=$$TOKEN

.PHONY: docs-test
docs-test: ## Test if documentation can be built without warnings or errors
	@cd docs && uv run make html SPHINXOPTS="-W"

.PHONY: docs
docs: ## Build the documentation
	@cd docs && uv run make html

.PHONY: docs-clean
docs-clean: ## Clean built documentation
	@cd docs && uv run make clean

.PHONY: help
help:
	@uv run python -c "import re; \
	[[print(f'\033[36m{m[0]:<20}\033[0m {m[1]}') for m in re.findall(r'^([a-zA-Z_-]+):.*?## (.*)$$', open(makefile).read(), re.M)] for makefile in ('$(MAKEFILE_LIST)').strip().split()]"

.PHONY: new-build
new-build:
	@./kustomize/scripts/build_and_push.sh

.PHONY: check-minikube
check-minikube:
	@is_minikube=$$(uv run python -c "import os; print(str('minikube' in os.getenv('KUBECONFIG', '')).lower())"); \
	if [ $$is_minikube = "true" ]; then \
		echo "You're using minikube"; \
	else \
		echo "Not using minikube. Exiting."; \
		exit 1; \
	fi

.PHONY: spec
spec:
	@uv run --no-cache ./viva_api/api/openapi_spec.py

.PHONY: new
new:
	@make check-minikube
	@make write-latest-commit
	@make spec
	@make new-build
	@kubectl kustomize kustomize/overlays/sms-api-local | kubectl apply -f -

.PHONY: whichkube
whichkube:
	@echo $${KUBECONFIG}

.PHONY: gateway
gateway:
	@make spec
	@uv run uvicorn viva_api.api.main:app \
		--env-file assets/dev/config/.dev_env \
		--host 0.0.0.0 \
		--port ${LOCAL_GATEWAY_PORT} \
		--reload

.PHONY: edit-app
edit-app:
	@uv run marimo edit app/ui/$(ui).py

.PHONY: pginit
pginit:
	@initdb -D $(path)

.PHONY: pgup
pgup:
	@touch "$(path)/.log"
	@pg_ctl -D $(path) -l "$(path)/.log" -o "-p $(port)" start

.PHONY: pgdown
pgdown:
	@pg_ctl stop -D $(dbname)

.PHONY: pgdb-new
pgdb-new:
	@createdb $(dbname)

.PHONY: pgdb-conn
pgdb-conn:
	@psql $(dbname)

.PHONY: pgdb-drop
pgdb-drop:
	@dropdb $(dbname)

# --name postgresql
.PHONY: dbup
dbup:
	@service_name="pgdb"; \
	[ -z "$(port)" ] && port=${LOCAL_POSTGRES_PORT} || port=$(port); \
	[ -z "$(password)" ] && password=${LOCAL_POSTGRES_PASSWORD} || password=$(password); \
	docker run -d \
		--name $$service_name \
		-e POSTGRES_PASSWORD=$$password \
		-e POSTGRES_USER=${POSTGRES_USER} \
		-e POSTGRES_HOST=localhost \
		-e POSTGRES_DB=${POSTGRES_DB} \
		-p $$port:${POSTGRES_PORT} \
		postgres:17

.PHONY: dbdown
dbdown:
	@docker rm -f pgdb

.PHONY: mongoup
mongoup:
	@docker run -d \
		--name mongodb \
		-p $(port):$(port) \
		mongo

.PHONY: redisup
redisup:
	@[ -z "$(port)" ] && port=30050 || port=$(port); \
	docker run -d --name redis --rm -p $$port:$$port redis

# this command should run psql -h localhost -p 65432 -U alexanderpatrie sms
.PHONY: pingpg
pingpg:
	@[ -z "$(user)" ] && user=${LOCAL_POSTGRES_USER} || user=$(user); \
	[ -z "$(port)" ] && port=${LOCAL_POSTGRES_PORT} || port=$(port); \
	psql -h localhost -p $$port -U ${LOCAL_POSTGRES_USER} sms;

.PHONY: pingdb
pingdb:
	@uri=$$(make pguri); \
	psql $$uri

.PHONY: test-mod
testmod:
	@uv run python -m pytest -s $(m)

.PHONY: run-workflow
workflow:
	curl -X POST \
		-H "Authorization: token $(token)" \
		-H "Accept: application/vnd.github.v3+json" \
		https://api.github.com/repos/vivarium-collective/viva-api/actions/workflows/build-and-push.yml/dispatches \
		-d '{"ref": $(branch)}'

.PHONY: generate-client
generate-client:
	@make spec
	@uv run ./scripts/generate-api-client.sh

.PHONY: pguri
pguri:
	@pg_user=sms; \
	pg_password=$$(uv run python -c "import dotenv;import os;dotenv.load_dotenv('assets/dev/config/.dev_env');print(os.getenv('POSTGRES_PASSWORD'))"); \
	echo postgresql://${POSTGRES_USER}:$$pg_password@${LOCAL_POSTGRES_HOST}:${LOCAL_POSTGRES_PORT}/${POSTGRES_DB}

.PHONY: apy
apy:
	@uv run python -m asyncio

.PHONY: set-wip
set-wip:
	@module=$(ui); \
	cp app/ui/$$module.py app/ui/wip_$$module.py; \
	echo Set WIP at app/ui/wip_$$module.py

.PHONY: transfer-wip
transfer-wip:
	@module=$(ui); \
	cp app/ui/wip_$$module.py app/ui/$$module.py; \
	cp app/ui/layouts/wip_$$module.grid.json app/ui/layouts/$$module.grid.json

.PHONY: image
image:
	@[ -z "$(tag)" ] && tag=$(CURRENT_VERSION) || tag=$(tag); \
	./kustomize/scripts/build_and_push.sh $$tag

.PHONY: exec-api
exec-api:
	@[ -z "$(tag)" ] && tag=0.2.8 || tag=$(tag); \
	docker run --rm --name sms -p 8000:8000 --platform linux/amd64 --entrypoint /usr/bin/env -it ghcr.io/vivarium-collective/sms-api:$$tag bash

.PHONY: exec
exec:
	@docker exec -it api /bin/bash

.PHONY: run-api
run-api:
	@docker run --rm --name api -p 8000:8000 --platform linux/amd64 --entrypoint /usr/bin/env -it sms-api:latest bash

.PHONY: api
api:
	@docker rmi -f sms-api:latest && docker compose build api && make run-api

.PHONY: available_simulation_configs
available_simulation_configs:
	@[ -z "$(hpc_dest)" ] && echo "You must enter an hpc dest" && exit 1 || ls -1 *.json > $(hpc_dest)

.PHONY: compose
compose:
	@docker rm -f api redis; \
	docker rmi sms-api; \
	docker compose build; \
	docker compose up

.DEFAULT_GOAL := help

.PHONY: api_client
api_client:
	@make spec; uv run --no-cache --refresh openapi-python-client generate \
		--path ./viva_api/api/spec/openapi_3_1_0_generated.yaml \
		--config ./client-generator-config.yml \
		--overwrite \
		--output-path ./viva_api/api
	@rm ./viva_api/api/pyproject.toml && rm ./viva_api/api/.gitignore && git restore ./viva_api/api/README.md

.PHONY: ui
ui:
	@uv run --no-cache marimo edit app/ui/$(id).py

.PHONY: e2e
e2e:
	@uv run --no-cache pytest tests/api/ecoli/test_simulations.py::TestRunSimulationE2E -v -s

.PHONY: tui
tui:
	@${VENV} app/tui/agent_app.py

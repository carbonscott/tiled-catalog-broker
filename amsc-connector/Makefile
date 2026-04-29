SHELL := /bin/bash

TILED_DIR := thirdparty/tiled
ENV_FILE := .env

.PHONY: tiled-up tiled-down tiled-logs connector-up connector-down connector-clean connector-logs env submodules \
        format lint ci-build ci-pr ci-push

TILED_COMPOSE_FILES := -f $(TILED_DIR)/compose.yml -f $(TILED_DIR)/compose.dev.yml
CONNECTOR_COMPOSE_FILES := -f compose.yml

submodules:
	git submodule update --init --recursive

$(ENV_FILE):
	@bash scripts/generate_env.sh $(ENV_FILE)

env: $(ENV_FILE)

tiled-up: submodules $(ENV_FILE)
	docker compose --env-file $(CURDIR)/$(ENV_FILE) $(TILED_COMPOSE_FILES) up --force-recreate --remove-orphans --build -d

tiled-down:
	docker compose --env-file $(CURDIR)/$(ENV_FILE) $(TILED_COMPOSE_FILES) down

tiled-clean:
	docker compose --env-file $(CURDIR)/$(ENV_FILE) $(TILED_COMPOSE_FILES) down -v --remove-orphans

connector-up: $(ENV_FILE)
	docker compose --env-file $(CURDIR)/$(ENV_FILE) $(CONNECTOR_COMPOSE_FILES) up --force-recreate --remove-orphans --build -d

connector-down:
	docker compose --env-file $(CURDIR)/$(ENV_FILE) $(CONNECTOR_COMPOSE_FILES) down

connector-clean:
	docker compose --env-file $(CURDIR)/$(ENV_FILE) $(CONNECTOR_COMPOSE_FILES) down -v --remove-orphans

format:
	uv run ruff format

lint:
	uv run ruff check --fix

REPO_ROOT := $(abspath $(CURDIR)/..)
WORKFLOW := .github/workflows/docker-amsc-connector.yml
ACT_FLAGS := --container-architecture linux/amd64

.check-act:
	@command -v act >/dev/null 2>&1 || { echo "act is not installed. https://github.com/nektos/act"; exit 1; }

.check-secrets:
	@test -f .secrets || { echo "Secrets file not found. Please create amsc-connector/.secrets with REGISTRY_USER and REGISTRY_TOKEN."; exit 1; }

ci-build: .check-act # fork PR
	cd $(REPO_ROOT) && act pull_request \
		$(ACT_FLAGS) \
		--workflows $(WORKFLOW) \
		--eventpath .github/act/pr-fork-event.json

ci-pr: .check-secrets .check-act # internal PR
	cd $(REPO_ROOT) && act pull_request \
		$(ACT_FLAGS) \
		--workflows $(WORKFLOW) \
		--eventpath .github/act/pr-event.json \
		--secret-file amsc-connector/.secrets

ci-push: .check-secrets .check-act # push to main
	cd $(REPO_ROOT) && act push \
		$(ACT_FLAGS) \
		--workflows $(WORKFLOW) \
		--secret-file amsc-connector/.secrets
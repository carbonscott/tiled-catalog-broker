SHELL := /bin/bash

TILED_DIR := thirdparty/tiled
ENV_FILE := .env

.PHONY: tiled-up tiled-down tiled-logs connector-up connector-down connector-clean connector-logs env submodules

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
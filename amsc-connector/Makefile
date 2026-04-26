SHELL := /bin/bash

TILED_DIR := thirdparty/tiled
ENV_FILE := .env

.PHONY: docker-up docker-down docker-logs env submodules

COMPOSE_FILES := -f $(TILED_DIR)/compose.yml -f $(TILED_DIR)/compose.dev.yml

## Ensure git submodules are initialized and up to date
submodules:
	git submodule update --init --recursive

# Generate .env with random secrets if it doesn't exist
$(ENV_FILE):
	@echo "Generating $@ with random secrets..."
	@echo "TILED_SINGLE_USER_API_KEY=$$(openssl rand -hex 32)" > $@
	@echo "POSTGRES_PASSWORD=$$(openssl rand -hex 32)" >> $@
	@echo "REDIS_PASSWORD=$$(openssl rand -hex 32)" >> $@
	@FERNET_KEY=$$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '\n'); \
	 echo 'TILED_WEBHOOKS_SECRET_KEYS=["'"$$FERNET_KEY"'"]' >> $@
	@echo "Created $@"

env: $(ENV_FILE)

## Bring up tiled services (dev mode with webhooks, no monitoring)
docker-up: submodules $(ENV_FILE)
	docker compose --env-file $(ENV_FILE) $(COMPOSE_FILES) up --force-recreate --remove-orphans --build -d

## Stop tiled services
docker-down:
	docker compose $(COMPOSE_FILES) down

## Tail logs
docker-logs:
	docker compose $(COMPOSE_FILES) logs -f

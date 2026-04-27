#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-.env}"

if [[ -f "$ENV_FILE" ]]; then
    echo "$ENV_FILE already exists, skipping generation."
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_FILE="$SCRIPT_DIR/../.env.example"

if [[ ! -f "$EXAMPLE_FILE" ]]; then
    echo "Error: $EXAMPLE_FILE not found." >&2
    exit 1
fi

echo "Generating $ENV_FILE from $EXAMPLE_FILE with random secrets..."

cp "$EXAMPLE_FILE" "$ENV_FILE"

TILED_API_KEY=$(openssl rand -hex 32)
POSTGRES_PASSWORD=$(openssl rand -hex 32)
REDIS_PASSWORD=$(openssl rand -hex 32)
FERNET_KEY=$(uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
WEBHOOK_SECRET=$(openssl rand -hex 32)

sed -i.bak \
    -e "s|TILED_SINGLE_USER_API_KEY=changeme|TILED_SINGLE_USER_API_KEY=$TILED_API_KEY|" \
    -e "s|POSTGRES_PASSWORD=changeme|POSTGRES_PASSWORD=$POSTGRES_PASSWORD|" \
    -e "s|REDIS_PASSWORD=changeme|REDIS_PASSWORD=$REDIS_PASSWORD|" \
    -e "s|TILED_WEBHOOKS_SECRET_KEYS=\[\"your-fernet-key-here\"\]|TILED_WEBHOOKS_SECRET_KEYS=[\"$FERNET_KEY\"]|" \
    -e "s|WEBHOOK_SECRET=changeme|WEBHOOK_SECRET=$WEBHOOK_SECRET|" \
    "$ENV_FILE"
rm -f "${ENV_FILE}.bak"

echo "Created $ENV_FILE"

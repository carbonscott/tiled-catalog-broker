# syntax=docker/dockerfile:1
FROM python:3.12-slim-trixie AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /uvx /bin/

# Compile bytecode for faster startup; copy files instead of hardlinks
# (required when cache mount and target are on different filesystems)
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=0

# git is needed to fetch the tiled dependency from GitHub
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies (cached independently of project source)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-editable --no-dev

# Copy project source and install the project itself
COPY src/ src/
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable --no-dev
    
# ── Final stage ─────────────────────────────────────────────────────────
FROM python:3.12-slim-trixie

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

CMD ["fastapi", "run", "amsc_connector.main:app", "--host", "0.0.0.0", "--port", "8001"]


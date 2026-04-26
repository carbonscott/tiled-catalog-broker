# syntax=docker/dockerfile:1
FROM python:3.12-slim-trixie

# Copy uv and uvx binaries from the official distroless image (pinned version)
COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /uvx /bin/

# Compile bytecode for faster startup; copy files instead of hardlinks
# (required when cache mount and target are on different filesystems)
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

# Install dependencies in a separate layer so they are cached independently
# of the project source. This layer only re-runs when uv.lock or
# pyproject.toml changes.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project

# Copy project source and install the project itself
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

# Place the virtual environment's bin directory on PATH
ENV PATH="/app/.venv/bin:$PATH"

CMD ["amsc-connector"]

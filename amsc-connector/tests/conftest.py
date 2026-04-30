"""Shared test fixtures for amsc-connector tests.

Provides:
- Testcontainers Redis (module-scoped container, function-scoped async client)
- Mock settings with test-appropriate defaults
- Mock httpx / tiled dependencies for context injection
- Lua retry-pop script registration
"""

import contextlib
import json
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from testcontainers.redis import RedisContainer
from tiled.structures.core import StructureFamily

from amsc_connector.worker import _RETRY_POP_LUA, app

# ---------------------------------------------------------------------------
# Redis fixtures (real Redis via testcontainers)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def redis_container():
    """Start a Redis container once per module."""
    with RedisContainer() as container:
        yield container


@pytest_asyncio.fixture
async def redis_client(redis_container):
    """Async Redis client connected to testcontainers; flushed between tests."""
    url = (
        f"redis://{redis_container.get_container_host_ip()}"
        f":{redis_container.get_exposed_port(6379)}"
    )
    client = aioredis.from_url(url)
    await client.flushdb()
    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def retry_pop_script(redis_client):
    """Register the Lua pop script on the test Redis."""
    return redis_client.register_script(_RETRY_POP_LUA)


# ---------------------------------------------------------------------------
# Context injection — sets app.context globals without running lifespan
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def inject_context(redis_client, mock_amsc_client, mock_tiled_root):
    """Inject test dependencies into the FastStream app context.

    Sets redis_client, amsc_client, and tiled_root as global context values,
    then cleans them up after the test.
    """
    app.context.set_global("redis_client", redis_client)
    app.context.set_global("amsc_client", mock_amsc_client)
    app.context.set_global("tiled_root", mock_tiled_root)
    yield
    for key in ("redis_client", "amsc_client", "tiled_root"):
        with contextlib.suppress(Exception):
            app.context.reset_global(key)


# ---------------------------------------------------------------------------
# Mock settings
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_settings(monkeypatch):
    """Monkeypatch worker.settings with test-appropriate values."""
    import amsc_connector.worker as worker_mod

    settings = MagicMock()
    settings.dlq_retry_delay_seconds = 3600
    settings.dlq_retry_alert_threshold = 10
    settings.openmetadata_fqn_prefix = "test-repo.test-catalog"
    settings.tiled_retry_attempts = 1
    settings.tiled_retry_timeout = 5.0
    settings.tiled_retry_wait_initial = 0.1
    settings.tiled_retry_wait_max = 0.5
    monkeypatch.setattr(worker_mod, "settings", settings)
    return settings


# ---------------------------------------------------------------------------
# Mock external dependencies
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_amsc_client():
    """Mock httpx.AsyncClient for the AmSC API."""
    client = AsyncMock(spec=httpx.AsyncClient)
    # Default: successful creation
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 201
    resp.is_error = False
    resp.json.return_value = {"id": "test-id"}
    client.post.return_value = resp
    client.put.return_value = resp
    return client


@pytest.fixture
def mock_tiled_root():
    """Mock tiled container root with configurable node structure."""
    root = MagicMock()
    return root


def make_tiled_node(
    *,
    metadata: dict | None = None,
    structure_family: StructureFamily = StructureFamily.container,
    mimetype: str | None = None,
):
    """Helper: create a mock tiled node with given properties."""
    node = MagicMock()
    node.metadata = metadata or {}
    node.structure_family = structure_family
    if structure_family != StructureFamily.container:
        source = MagicMock()
        source.mimetype = mimetype
        node.data_sources.return_value = [source]
    return node


def make_successful_response(status_code: int = 201, body: dict | None = None):
    """Helper: create a mock httpx.Response for a successful API call."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.is_error = False
    resp.json.return_value = body or {"id": "test-id"}
    resp.text = json.dumps(body or {"id": "test-id"})
    return resp


def make_error_response(status_code: int = 500, detail: str = "error"):
    """Helper: create a mock httpx.Response for an error."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.is_error = True
    resp.text = json.dumps({"detail": detail})
    resp.json.return_value = {"detail": detail}
    return resp

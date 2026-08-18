"""
Configuration Module.

Server connection settings, read from the environment (optionally seeded from
a `.env` file). `config.yml` is the *Tiled server's* config, consumed by
`tiled serve config config.yml` — the broker does not read it.
"""

import os
from pathlib import Path


def _load_dotenv(path=".env"):
    """Load key=value pairs from a .env file into os.environ.

    Skips blank lines and comments (#). Explicit env vars take precedence
    (uses setdefault). No external dependencies.
    """
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if key and sep:
            os.environ.setdefault(key.strip(), value.strip())


_load_dotenv()


def get_tiled_url():
    """Get Tiled server URL (from env or default).

    Set TILED_URL to override. Defaults to http://localhost:8005.
    """
    return os.environ.get(
        "TILED_URL",
        "http://localhost:8005",
    )


def get_api_key():
    """Get Tiled API key (from env).

    Checks TILED_API_KEY first, then TILED_KEY for compatibility
    with tiled_remote scripts. Returns empty string if neither is set.
    """
    return os.environ.get("TILED_API_KEY", os.environ.get("TILED_KEY", ""))

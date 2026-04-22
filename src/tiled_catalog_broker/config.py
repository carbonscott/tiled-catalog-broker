"""
Configuration Module.

Loads configuration from config.yml and provides accessor functions.
Uses ruamel.yaml to preserve comments for round-trip editing.
"""

import os
from pathlib import Path
from ruamel.yaml import YAML


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


# Module-level config cache
_config = None
_config_path = None


def load_config(config_path=None):
    """Load config from YAML file.

    Args:
        config_path: Path to config.yml. Defaults to config.yml in parent directory.

    Returns:
        dict: The 'broker' section of the config file (falls back to 'vdp' for compat).
    """
    if config_path is None:
        config_path = Path(__file__).parent.parent.parent / "config.yml"

    yaml = YAML()
    with open(config_path) as f:
        full_config = yaml.load(f)

    # Prefer 'broker' section, fall back to 'vdp' for backward compatibility
    return full_config.get("broker", full_config.get("vdp", {}))


def get_config():
    """Get cached config (loads once on first call)."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def get_service_dir():
    """Get the service directory (where catalog.db and storage/ live)."""
    return get_config()["service_dir"]


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


def get_host_data_root():
    """Authoring-host filesystem root from `TILED_HOST_DATA_ROOT`.

    The shared parent under which all dataset `directory:` paths sit on
    the host running `tcb inspect`. `tcb inspect` uses this to derive a
    `server_base_dir:` for the draft YAML when the Tiled server sees the
    same data at a different mount.
    """
    return os.environ.get("TILED_HOST_DATA_ROOT", "")


def get_server_data_root():
    """Server-side mount root from `TILED_SERVER_DATA_ROOT`.

    The same shared parent as `TILED_HOST_DATA_ROOT`, but as the Tiled
    server sees it (K8s pod, reverse-proxy, etc.). `tcb inspect` swaps
    this prefix into the YAML's `server_base_dir:` so registration is
    purely read-only of the YAML.
    """
    return os.environ.get("TILED_SERVER_DATA_ROOT", "")



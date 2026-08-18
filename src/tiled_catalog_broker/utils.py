"""
Shared Utilities.

Common functions used across registration scripts.
"""

import re

import numpy as np
import pandas as pd

from .config import get_tiled_url, get_api_key


def slugify_key(label):
    """Convert a human-readable label to the catalog key (UPPER_SNAKE).

    Rules: uppercase, replace runs of non-alphanumerics with a single
    underscore, strip leading/trailing underscores.

    Examples:
        "Broad Sigma"            -> "BROAD_SIGMA"
        "SUNNY NiPS3 10K"        -> "SUNNY_NIPS3_10K"
        "NiPS3 Multimodal"       -> "NIPS3_MULTIMODAL"
    """
    if not label:
        raise ValueError("slugify_key: label is empty")
    return re.sub(r"[^A-Z0-9]+", "_", str(label).upper()).strip("_")


# Dataset-container metadata marker recording where artifact bytes live.
# "external": array children point at HDF5 files the server reads from its
# own filesystem (`readable_storage`). "uploaded": arrays were written
# through the server into its writable storage — the server owns the bytes,
# and deleting the nodes deletes them.
STORAGE_KEY = "storage"
STORAGE_EXTERNAL = "external"
STORAGE_UPLOADED = "uploaded"


# Standard columns in the artifact manifest that are NOT stored as metadata.
# Everything else becomes artifact-level metadata dynamically.
# `shape`/`dtype` are here because registration re-emits them in structured form
# (a list of ints and a dtype string) — copying the manifest's JSON-encoded
# `shape` through as well would overwrite that with a raw string.
ARTIFACT_STANDARD_COLS = {"uid", "type", "file", "dataset", "index", "shape", "dtype"}


def to_json_safe(value):
    """Convert a value to a JSON-serializable type."""
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.ndarray,)):
        return value.tolist()
    if isinstance(value, (list, dict)):
        return value
    if pd.isna(value):
        return None
    return value


def check_server(url=None, api_key=None):
    """Check if a Tiled server is running.

    Args:
        url: Server URL. Defaults to get_tiled_url().
        api_key: Apikey. Defaults to get_api_key().

    Returns:
        bool: True if server responds, False otherwise.
    """
    import ssl
    import urllib.request
    import urllib.error

    if url is None:
        url = get_tiled_url()
    if api_key is None:
        api_key = get_api_key()

    headers = {}
    if api_key:
        headers["Authorization"] = f"Apikey {api_key}"

    try:
        req = urllib.request.Request(f"{url}/api/v1/", headers=headers)
        # Allow self-signed certificates for internal HTTPS servers
        ctx = ssl.create_default_context()
        if url.startswith("https"):
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=5, context=ctx) as response:
            return response.status == 200
    except (urllib.error.URLError, urllib.error.HTTPError):
        return False


def make_entity_key(ent_row, dataset_key):
    """Generate the Tiled node key for an entity from its uid.

    The entity key is derived at registration time from the dataset key
    (slug(label)) and the first 13 characters of the entity's manifest
    uid, so it is not persisted in the manifest itself. ``_make_uid``
    returns 16-char hex (sha256 truncated), so 13 chars gives ample
    collision resistance within a dataset.

    Examples:
        >>> make_entity_key({"uid": "636ce3e41ea05f0f"}, "VDP_SIM")
        'VDP_SIM_636ce3e41ea05'
    """
    return f"{dataset_key}_{str(ent_row['uid'])[:13]}"


def make_artifact_key(art_row):
    """The Tiled node key for an artifact: its manifest ``type`` verbatim.

    `tcb generate` is responsible for producing unique type values per entity,
    so the type is already the key (e.g. ``mh_powder_30T``, ``rixs``).
    """
    return art_row["type"]

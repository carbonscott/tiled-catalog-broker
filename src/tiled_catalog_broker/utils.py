"""
Shared Utilities.

Common functions used across registration scripts.
"""

import os
import re

import h5py
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


# Standard columns in the artifact manifest that are NOT stored as metadata.
# Everything else becomes artifact-level metadata dynamically.
ARTIFACT_STANDARD_COLS = {"uid", "type", "file", "dataset", "index"}


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


def get_artifact_info(base_dir, file_path, dataset_path, index=None, _cache={}):
    """Read artifact shape and dtype from HDF5, cached by (base_dir, file_path, dataset_path).

    Returns:
        tuple: (shape, dtype_str, kind, itemsize) where shape is a list of ints,
            dtype_str is the numpy dtype string (e.g. "float64"), kind is the
            single-char numpy kind code (e.g. "f", "i", "c"), and itemsize is
            the number of bytes per element.
    """
    cache_key = (base_dir, file_path, dataset_path)
    if cache_key not in _cache:
        full_path = os.path.join(base_dir, file_path)
        with h5py.File(full_path, "r") as f:
            ds = f[dataset_path]
            _cache[cache_key] = (ds.shape, str(ds.dtype), ds.dtype.kind, ds.dtype.itemsize)
    full_shape, dtype_str, kind, itemsize = _cache[cache_key]
    if index is not None:
        return list(full_shape[1:]), dtype_str, kind, itemsize
    return list(full_shape), dtype_str, kind, itemsize


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
    uid (two UUID segments for UUIDv5 inputs), so it is not persisted in
    the manifest itself.

    Examples:
        >>> make_entity_key({"uid": "636ce3e4-1ea0-5f0f-a515-a4378fa5c842"},
        ...                 "VDP_SIM")
        'VDP_SIM_636ce3e4-1ea0'
    """
    return f"{dataset_key}_{str(ent_row['uid'])[:13]}"


def make_artifact_key(art_row, prefix=""):
    """Generate key for artifact from its type.

    In the generic manifest standard, the ``type`` column already contains
    the unique artifact key (e.g., ``mh_powder_30T``, ``rixs``).  The
    manifest generator is responsible for producing unique type values
    per entity.

    Args:
        art_row: DataFrame row or dict with at least a ``type`` field.
        prefix: Optional prefix (e.g., ``"path_"`` for metadata keys).

    Returns:
        str: The artifact key, optionally prefixed.

    Examples:
        >>> make_artifact_key({"type": "mh_powder_30T"})
        'mh_powder_30T'
        >>> make_artifact_key({"type": "rixs"}, prefix="path_")
        'path_rixs'
    """
    key = art_row["type"]
    return f"{prefix}{key}" if prefix else key

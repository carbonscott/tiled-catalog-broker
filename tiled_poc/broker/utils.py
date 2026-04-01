"""
Shared Utilities.

Common functions used across registration scripts.
"""

import os

import h5py
import numpy as np
import pandas as pd

from .config import get_tiled_url, get_api_key


# Standard columns in the artifact manifest that are NOT stored as metadata.
# Everything else becomes artifact-level metadata dynamically.
ARTIFACT_STANDARD_COLS = {"uid", "type", "file", "dataset", "index"}


def to_json_safe(value):
    """Convert a value to a JSON-serializable type."""
    if isinstance(value, (np.integer,)):
        return int(value)
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
    """Read artifact shape and dtype from HDF5, cached by (file_path, dataset_path).

    Returns:
        tuple: (shape, dtype_str, kind, itemsize) where shape is a list of ints,
            dtype_str is the numpy dtype string (e.g. "float64"), kind is the
            single-char numpy kind code (e.g. "f", "i", "c"), and itemsize is
            the number of bytes per element.
    """
    cache_key = (file_path, dataset_path)
    if cache_key not in _cache:
        full_path = os.path.join(base_dir, file_path)
        with h5py.File(full_path, "r") as f:
            ds = f[dataset_path]
            _cache[cache_key] = (ds.shape, str(ds.dtype), ds.dtype.kind, ds.dtype.itemsize)
    full_shape, dtype_str, kind, itemsize = _cache[cache_key]
    if index is not None:
        return list(full_shape[1:]), dtype_str, kind, itemsize
    return list(full_shape), dtype_str, kind, itemsize


def get_artifact_shape(base_dir, file_path, dataset_path, index=None):
    """Read artifact shape from HDF5. Deprecated: use get_artifact_info instead."""
    shape, _, _, _ = get_artifact_info(base_dir, file_path, dataset_path, index)
    return shape


def check_server():
    """Check if Tiled server is running.

    Returns:
        bool: True if server responds, False otherwise.
    """
    import ssl
    import urllib.request
    import urllib.error

    url = get_tiled_url()
    api_key = get_api_key()

    try:
        req = urllib.request.Request(
            f"{url}/api/v1/",
            headers={"Authorization": f"Apikey {api_key}"}
        )
        # Allow self-signed certificates for internal HTTPS servers
        ctx = ssl.create_default_context()
        if url.startswith("https"):
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=5, context=ctx) as response:
            return response.status == 200
    except (urllib.error.URLError, urllib.error.HTTPError):
        return False


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

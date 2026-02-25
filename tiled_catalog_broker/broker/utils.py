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
    if pd.isna(value):
        return None
    return value


def get_artifact_shape(base_dir, file_path, dataset_path, index=None, _cache={}):
    """Read artifact shape from HDF5, with caching by dataset path.

    Caches by dataset_path to avoid re-opening files for artifacts
    that share the same HDF5 internal structure.
    """
    if dataset_path not in _cache:
        full_path = os.path.join(base_dir, file_path)
        with h5py.File(full_path, "r") as f:
            _cache[dataset_path] = f[dataset_path].shape
    full_shape = _cache[dataset_path]
    if index is not None:
        return list(full_shape[1:])  # Skip batch dimension
    return list(full_shape)


def check_server():
    """Check if Tiled server is running.

    Returns:
        bool: True if server responds, False otherwise.
    """
    import urllib.request
    import urllib.error

    url = get_tiled_url()
    api_key = get_api_key()

    try:
        req = urllib.request.Request(
            f"{url}/api/v1/",
            headers={"Authorization": f"Apikey {api_key}"}
        )
        with urllib.request.urlopen(req, timeout=5) as response:
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

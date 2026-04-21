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
# Standard columns in the artifact manifest that are NOT copied to metadata.
# `array_name` is added by the layout: manifest fan-out step; it's already
# encoded in the Tiled node key, so copying it to metadata would duplicate.
ARTIFACT_STANDARD_COLS = {"uid", "type", "file", "dataset", "index", "array_name"}


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
    uid, so it is not persisted in the manifest itself. ``_make_uid``
    returns 16-char hex (sha256 truncated), so 13 chars gives ample
    collision resistance within a dataset.

    Examples:
        >>> make_entity_key({"uid": "636ce3e41ea05f0f"}, "VDP_SIM")
        'VDP_SIM_636ce3e41ea05'
    """
    return f"{dataset_key}_{str(ent_row['uid'])[:13]}"


def make_artifact_key(art_row, prefix=""):
    """Generate key for an artifact node.

    Two schemes, selected by the columns present on the row:

    - Fan-out rows (``layout: manifest``) carry both ``array_name`` and
      ``auid``; the key is ``f"{array_name}_{auid[:8]}"``. The short auid
      suffix disambiguates sibling arrays fanned out from sibling files
      that share an internal HDF5 structure.
    - Non-fan-out rows use the ``type`` column verbatim — the manifest
      generator is responsible for producing unique type values per
      entity in those layouts.

    Args:
        art_row: DataFrame row or dict.
        prefix: Optional prefix (e.g., ``"path_"`` for metadata keys).

    Returns:
        str: The artifact key, optionally prefixed.

    Examples:
        >>> make_artifact_key({"type": "mh_powder_30T"})
        'mh_powder_30T'
        >>> make_artifact_key(
        ...     {"array_name": "H_T", "auid": "cfbc55c6-741b-5aa5-8f2b"}
        ... )
        'H_T_cfbc55c6'
    """
    array_name = art_row.get("array_name") if hasattr(art_row, "get") \
        else (art_row["array_name"] if "array_name" in art_row else None)
    if array_name:
        auid = str(art_row["auid"])
        key = f"{array_name}_{auid[:8]}"
    else:
        key = art_row["type"]
    return f"{prefix}{key}" if prefix else key


def split_constant_cols(df):
    """Partition DataFrame columns into constants vs. varying.

    Constant columns (a single non-null unique value across all rows) are
    candidates for promotion to dataset-level metadata. Varying columns
    (or all-null columns) stay per-row.

    Args:
        df: Input DataFrame.

    Returns:
        (constants, varying): dict of column-name -> single value,
        and list of column names that vary across rows.
    """
    constants = {}
    varying = []
    for col in df.columns:
        vals = df[col].dropna().unique()
        if len(vals) == 1:
            constants[col] = to_json_safe(vals[0])
        elif len(vals) == 0:
            # all-null column: skip (neither constant nor useful per-row)
            continue
        else:
            varying.append(col)
    return constants, varying

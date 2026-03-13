"""
HDF5 inspection engine for auto-generating dataset contract YAMLs.

Scans a directory of HDF5 files, classifies datasets by role
(parameter, artifact, shared axis), validates consistency,
and emits a draft YAML config with TODO markers.

Usage:
    python -m broker.inspect /path/to/data/ [--output datasets/draft.yml]
"""

import os
import sys
import datetime
from pathlib import Path
from collections import Counter
from dataclasses import dataclass, field

import h5py
import numpy as np

from .schema import load_catalog_model, get_allowed_values


# ---------------------------------------------------------------------------
# Data classes for inspection results
# ---------------------------------------------------------------------------

@dataclass
class DatasetInfo:
    """Metadata about a single HDF5 dataset."""
    name: str
    shape: tuple
    dtype: str
    ndim: int
    size: int
    category: str = ""  # PARAMETER, ARTIFACT, SHARED_AXIS, EXTRA_METADATA
    stats: dict = field(default_factory=dict)


@dataclass
class InspectionResult:
    """Complete inspection results for a data directory."""
    source_dir: str
    h5_files: list
    file_pattern: str
    layout: str  # per_entity, batched, grouped
    batch_size: int = 0
    total_entities: int = 0
    datasets: dict = field(default_factory=dict)  # name -> DatasetInfo
    groups: list = field(default_factory=list)
    root_attrs: dict = field(default_factory=dict)
    group_attrs: dict = field(default_factory=dict)
    dataset_attrs: dict = field(default_factory=dict)
    consistency_issues: list = field(default_factory=list)
    recommendations: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Step 1: Directory Reconnaissance
# ---------------------------------------------------------------------------

def find_h5_files(directory):
    """Find all HDF5 files and infer the glob pattern.

    Returns:
        (list[Path], str): Sorted HDF5 file paths and inferred glob pattern.
    """
    root = Path(directory)
    h5_files = sorted(root.rglob("*.h5"))
    if not h5_files:
        h5_files = sorted(root.rglob("*.hdf5"))

    if not h5_files:
        return [], "*.h5"

    # Infer file pattern from common structure
    rel_paths = [f.relative_to(root) for f in h5_files]

    if len(set(f.name for f in rel_paths)) == 1:
        # All files have the same name (e.g., simulations.h5) — pattern is parent/name
        sample = rel_paths[0]
        parts = list(sample.parts)
        pattern = "/".join(["*"] * (len(parts) - 1) + [parts[-1]])
    elif all(len(f.parts) == 1 for f in rel_paths):
        pattern = "*.h5"
    else:
        pattern = "**/*.h5"

    return h5_files, pattern


# ---------------------------------------------------------------------------
# Step 2: HDF5 Tree Walk
# ---------------------------------------------------------------------------

def walk_h5_tree(h5_path):
    """Walk an HDF5 file and collect all dataset/group metadata.

    Returns:
        (dict[str, DatasetInfo], list[str]): datasets and group names.
    """
    datasets = {}
    groups = []

    with h5py.File(h5_path, "r") as f:
        def visit(name, obj):
            if isinstance(obj, h5py.Dataset):
                datasets[name] = DatasetInfo(
                    name=name,
                    shape=obj.shape,
                    dtype=str(obj.dtype),
                    ndim=obj.ndim,
                    size=obj.size,
                )
            elif isinstance(obj, h5py.Group):
                groups.append(name)

        f.visititems(visit)

    return datasets, groups


# ---------------------------------------------------------------------------
# Step 3: Classify Datasets
# ---------------------------------------------------------------------------

def classify_datasets(datasets, groups, layout, batch_size=0):
    """Classify each dataset as PARAMETER, ARTIFACT, SHARED_AXIS, or EXTRA_METADATA."""
    # Identify parameter groups (groups that contain per-entity 1D arrays)
    param_groups = set()
    if layout == "batched":
        for g in groups:
            children = [n for n in datasets if n.startswith(g + "/")]
            if children and all(
                datasets[n].ndim == 1 and datasets[n].shape[0] == batch_size
                for n in children
            ):
                param_groups.add(g)

    for name, ds in datasets.items():
        if layout == "per_entity":
            if ds.ndim == 0:
                ds.category = "PARAMETER"
            else:
                ds.category = "ARTIFACT_OR_AXIS"

        elif layout == "batched":
            parent_group = name.rsplit("/", 1)[0] if "/" in name else ""
            if ds.shape and ds.shape[0] == batch_size:
                if ds.ndim == 1 and parent_group in param_groups:
                    ds.category = "PARAMETER"
                elif ds.ndim == 1:
                    ds.category = "EXTRA_METADATA"
                elif ds.ndim > 1:
                    ds.category = "ARTIFACT"
                else:
                    ds.category = "EXTRA_METADATA"
            elif ds.ndim == 0:
                ds.category = "PARAMETER"
            else:
                ds.category = "SHARED_AXIS"

        elif layout == "grouped":
            ds.category = "ARTIFACT_OR_AXIS"


def detect_layout(datasets, h5_files):
    """Detect whether the data is per_entity, batched, or grouped.

    Returns:
        (str, int): layout type and batch_size (0 if not batched).
    """
    if not datasets:
        return "per_entity", 0

    has_scalars = any(ds.ndim == 0 for ds in datasets.values())
    many_files = len(h5_files) > 1

    # Key insight: scalars + many files = per_entity.
    if has_scalars and many_files:
        return "per_entity", 0

    # Check shapes for batched pattern
    shapes_with_dim = [(name, ds.shape) for name, ds in datasets.items() if ds.ndim >= 1]

    if not shapes_with_dim:
        return "per_entity", 0

    axis0_lengths = [s[0] for _, s in shapes_with_dim]
    counts = Counter(axis0_lengths)
    most_common_len, most_common_count = counts.most_common(1)[0]

    # Batched if: few files, axis-0 shared by 3+ datasets, and axis-0 is large
    if most_common_count >= 3 and most_common_len > 1:
        if not many_files or most_common_len >= 100:
            return "batched", most_common_len

    if many_files:
        return "per_entity", 0

    return "per_entity", 0


def detect_grouped_layout(h5_path):
    """Check if a single HDF5 file uses group-per-entity pattern.

    Returns:
        (bool, list[str]): Whether it's grouped and the list of entity group names.
    """
    entity_groups = []
    with h5py.File(h5_path, "r") as f:
        for key in f.keys():
            if isinstance(f[key], h5py.Group):
                has_datasets = any(isinstance(f[key][k], h5py.Dataset) for k in f[key].keys())
                if has_datasets:
                    entity_groups.append(key)

    return len(entity_groups) > 5, entity_groups


# ---------------------------------------------------------------------------
# Step 4: Read Sample Values
# ---------------------------------------------------------------------------

def read_sample_values(h5_path, datasets, layout, batch_size=0, all_h5_files=None):
    """Read sample values from each dataset to compute statistics."""
    with h5py.File(h5_path, "r") as f:
        for name, ds in datasets.items():
            try:
                if ds.category == "PARAMETER":
                    data = f[name][()] if ds.ndim == 0 else f[name][:]
                    # For per-entity scalars, sample across multiple files
                    if layout == "per_entity" and ds.ndim == 0 and all_h5_files and len(all_h5_files) > 1:
                        sample_files = _sample_files(all_h5_files, n=100)
                        values = []
                        for sf in sample_files:
                            try:
                                with h5py.File(sf, "r") as g:
                                    values.append(float(g[name][()]))
                            except Exception:
                                pass
                        if values:
                            arr = np.array(values)
                            ds.stats = {
                                "min": _safe_float(arr.min()),
                                "max": _safe_float(arr.max()),
                                "n_unique": int(len(np.unique(arr))),
                                "has_nans": bool(np.isnan(arr).any()),
                                "is_constant": bool(arr.min() == arr.max()),
                                "sampled_from": len(values),
                            }
                            continue
                    flat = np.asarray(data).ravel()
                    finite = flat[np.isfinite(flat)] if flat.dtype.kind == "f" else flat
                    ds.stats = {
                        "min": _safe_float(np.nanmin(data)) if finite.size > 0 else None,
                        "max": _safe_float(np.nanmax(data)) if finite.size > 0 else None,
                        "n_unique": int(len(np.unique(finite))) if finite.size > 0 else 0,
                        "has_nans": bool(np.isnan(data).any()) if data.dtype.kind == "f" else False,
                        "is_constant": bool(np.nanmin(data) == np.nanmax(data)) if finite.size > 0 else True,
                    }
                elif ds.category == "ARTIFACT":
                    if layout == "batched" and ds.ndim > 1:
                        sample = f[name][0]
                    else:
                        sample = f[name][:]
                    ds.stats = {
                        "shape_per_entity": list(sample.shape),
                        "min": _safe_float(np.nanmin(sample)),
                        "max": _safe_float(np.nanmax(sample)),
                        "nan_fraction": float(np.isnan(sample).mean()) if sample.dtype.kind == "f" else 0.0,
                    }
                elif ds.category == "SHARED_AXIS":
                    data = f[name][:]
                    is_mono = False
                    if data.ndim == 1 and data.size > 1:
                        diffs = np.diff(data[:100].astype(float))
                        is_mono = bool(np.all(diffs > 0) or np.all(diffs < 0))
                    ds.stats = {
                        "shape": list(data.shape),
                        "range": [_safe_float(data.min()), _safe_float(data.max())],
                        "monotonic": is_mono,
                    }
                    if data.ndim == 1 and data.size > 1:
                        ds.stats["step"] = _safe_float(np.mean(np.diff(data.astype(float))))
                elif ds.category in ("EXTRA_METADATA", "ARTIFACT_OR_AXIS"):
                    if layout == "batched" and ds.ndim >= 1 and ds.shape[0] == batch_size:
                        sample = f[name][:10]
                    else:
                        sample = f[name][:]
                    data = np.asarray(sample)
                    ds.stats = {
                        "shape_per_entity": list(data.shape[1:]) if layout == "batched" and ds.ndim > 0 else list(data.shape),
                        "min": _safe_float(np.nanmin(data)) if data.dtype.kind == "f" and data.size > 0 else None,
                        "max": _safe_float(np.nanmax(data)) if data.dtype.kind == "f" and data.size > 0 else None,
                    }
            except Exception as e:
                ds.stats = {"error": str(e)}


def _sample_files(h5_files, n=100):
    """Sample up to n files evenly from a list."""
    if len(h5_files) <= n:
        return h5_files
    step = len(h5_files) // n
    return h5_files[::step][:n]


def _safe_float(val):
    """Convert numpy scalar to Python float, handling inf/nan."""
    v = float(val)
    if np.isnan(v):
        return "NaN"
    if np.isinf(v):
        return "-inf" if v < 0 else "inf"
    return v


# ---------------------------------------------------------------------------
# Step 5: Read HDF5 Attributes
# ---------------------------------------------------------------------------

def read_attributes(h5_path, datasets, groups):
    """Read attributes from root, groups, and datasets.

    Returns:
        (dict, dict, dict): root_attrs, group_attrs, dataset_attrs.
    """
    root_attrs = {}
    group_attrs = {}
    dataset_attrs = {}

    with h5py.File(h5_path, "r") as f:
        for k, v in f.attrs.items():
            root_attrs[k] = _attr_to_python(v)

        for g in groups:
            if g in f and f[g].attrs:
                attrs = {k: _attr_to_python(v) for k, v in f[g].attrs.items()}
                if attrs:
                    group_attrs[g] = attrs

        for name in datasets:
            if name in f and f[name].attrs:
                attrs = {k: _attr_to_python(v) for k, v in f[name].attrs.items()}
                if attrs:
                    dataset_attrs[name] = attrs

    return root_attrs, group_attrs, dataset_attrs


def _attr_to_python(val):
    """Convert HDF5 attribute value to a Python-native type."""
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    if isinstance(val, np.generic):
        return val.item()
    if isinstance(val, np.ndarray):
        if val.size <= 10:
            return val.tolist()
        return f"array({val.shape}, {val.dtype})"
    return val


# ---------------------------------------------------------------------------
# Step 6: Cross-File Consistency Check
# ---------------------------------------------------------------------------

def check_consistency(h5_files, reference_datasets, layout, batch_size=0, max_files=10):
    """Compare structure across multiple HDF5 files."""
    if len(h5_files) <= 1:
        return []

    issues = []
    ref_keys = set(reference_datasets.keys())
    ref_shapes = {n: d.shape for n, d in reference_datasets.items()}

    files_to_check = h5_files[1:]
    if len(files_to_check) > max_files:
        step = len(files_to_check) // max_files
        files_to_check = files_to_check[::step][:max_files]

    for h5_path in files_to_check:
        try:
            other = {}
            with h5py.File(h5_path, "r") as g:
                def collect(name, obj):
                    if isinstance(obj, h5py.Dataset):
                        other[name] = obj.shape
                g.visititems(collect)

            missing = ref_keys - set(other.keys())
            extra = set(other.keys()) - ref_keys
            if missing:
                issues.append(f"{Path(h5_path).name}: missing datasets {missing}")
            if extra:
                issues.append(f"{Path(h5_path).name}: extra datasets {extra}")

            for name in ref_keys & set(other.keys()):
                if ref_shapes[name] != other[name]:
                    issues.append(
                        f"{Path(h5_path).name}: {name} shape {other[name]} != reference {ref_shapes[name]}"
                    )
        except Exception as e:
            issues.append(f"{Path(h5_path).name}: could not open ({e})")

    # For shared axes, verify values are identical
    shared_names = [n for n, d in reference_datasets.items() if d.category == "SHARED_AXIS"]
    if shared_names and len(h5_files) > 1:
        with h5py.File(h5_files[0], "r") as ref_f:
            ref_data = {n: ref_f[n][:] for n in shared_names}

        check_files = files_to_check[:3]
        for h5_path in check_files:
            try:
                with h5py.File(h5_path, "r") as g:
                    for name, ref_arr in ref_data.items():
                        if name in g:
                            if not np.array_equal(ref_arr, g[name][:]):
                                issues.append(f"{Path(h5_path).name}: {name} values differ from reference")
            except Exception:
                pass

    return issues


# ---------------------------------------------------------------------------
# Step 7: Emit Draft YAML
# ---------------------------------------------------------------------------

def emit_draft_yaml(result, output_path=None):
    """Generate a draft YAML config from inspection results.

    Reads the semantic model (catalog_model.yml) to show available
    options for method, material, etc. in TODO comments.

    Args:
        result: InspectionResult.
        output_path: Path to write YAML (None = return as string).

    Returns:
        str: The YAML content.
    """
    model = load_catalog_model()

    lines = []

    def w(line=""):
        lines.append(line)

    # Header
    w(f"# AUTO-GENERATED by broker.inspect on {datetime.date.today().isoformat()}")
    w(f"# Source: {result.source_dir}")
    w(f"# Files scanned: {len(result.h5_files)} HDF5 ({result.file_pattern})")
    if result.layout == "batched":
        w(f"# Entities detected: {result.total_entities:,} ({len(result.h5_files)} files x {result.batch_size:,} batch size)")
    elif result.layout == "per_entity":
        w(f"# Entities detected: {result.total_entities:,} (one per file)")
    w()

    # Identity (TODO)
    w("# === REQUIRED: Fill in these identity fields ===")
    w("# Key convention: {METHOD}_{SIM|EXP|BENCH|OPT}_{DISTINGUISHING_FEATURE}")
    w('key: ""              # TODO: e.g., RIXS_SIM_BROAD_SIGMA')
    w('label: ""            # TODO: human-readable name (e.g., Broad Sigma)')
    w()

    # Dataset container metadata (TODO, with options from semantic model)
    w("# === REQUIRED: Dataset container metadata ===")
    w("# These fields describe the dataset as a whole and enable")
    w("# cross-dataset queries like client.search(Key('method') == 'RIXS')")
    w("metadata:")

    # method (list, required)
    method_ids = get_allowed_values(model, "methods") if model else []
    if method_ids:
        w(f"  method: []           # TODO: select from {method_ids}")
    else:
        w("  method: []           # TODO: scientific methods (list)")

    # data_type (required)
    dt_ids = get_allowed_values(model, "data_types") if model else []
    if dt_ids:
        w(f"  data_type: \"\"        # TODO: select from {dt_ids}")
    else:
        w('  data_type: ""        # TODO: "simulation" or "experimental"')

    # material (optional)
    mat_ids = get_allowed_values(model, "materials") if model else []
    if mat_ids:
        w(f"  # material: \"\"      # optional, select from {mat_ids}")
    else:
        w('  # material: ""      # optional')

    # producer (optional)
    prod_ids = get_allowed_values(model, "producers") if model else []
    if prod_ids:
        w(f"  # producer: \"\"      # optional (simulation code), select from {prod_ids}")
    else:
        w('  # producer: ""      # optional (simulation code)')

    # project (optional)
    proj_ids = get_allowed_values(model, "projects") if model else []
    if proj_ids:
        w(f"  # project: \"\"       # optional, select from {proj_ids}")
    else:
        w('  # project: ""       # optional')

    w('  # facility: ""      # optional (experimental data)')
    w('  # description: ""   # optional')
    w()

    # Data section
    w("# === Auto-detected ===")
    w("data:")
    w(f"  directory: {result.source_dir}")
    w(f'  file_pattern: "{result.file_pattern}"')
    w(f"  layout: {result.layout}")
    if result.layout == "batched":
        w(f"  # batch_size: {result.batch_size}")
    w()

    # Parameters
    params = {n: d for n, d in result.datasets.items() if d.category == "PARAMETER"}
    if params:
        w("parameters:")
        param_groups = set()
        for name in params:
            if "/" in name:
                param_groups.add(name.rsplit("/", 1)[0])

        if param_groups:
            group = sorted(param_groups)[0]
            w("  location: group")
            w(f"  group: /{group}")
        elif result.layout == "per_entity":
            w("  location: root_scalars")
        else:
            w("  location: root_scalars")

        w(f"  # {len(params)} parameters discovered:")
        for name, ds in sorted(params.items()):
            short_name = name.rsplit("/", 1)[-1] if "/" in name else name
            stat_str = f"  {ds.dtype}"
            if "min" in ds.stats and ds.stats["min"] is not None:
                stat_str += f"  range [{ds.stats['min']}, {ds.stats['max']}]"
            if ds.stats.get("is_constant"):
                stat_str += "  ** CONSTANT — consider moving to provenance **"
            w(f"  #   {short_name:<16s}{stat_str}")
        w()

    # Artifacts
    artifacts = {n: d for n, d in result.datasets.items() if d.category == "ARTIFACT"}
    if artifacts:
        w("# === TODO: Confirm artifact classification ===")
        if result.layout == "batched":
            w("# These datasets have shape (batch, ...) with ndim > 1 → classified as artifacts")
        else:
            w("# These datasets are multi-dimensional arrays → classified as artifacts")
        w("artifacts:")
        for name, ds in sorted(artifacts.items()):
            short_name = name.rsplit("/", 1)[-1] if "/" in name else name
            w(f"  - type: {short_name}           # TODO: rename if desired")
            w(f"    dataset: /{name}")
            shape_str = ds.stats.get("shape_per_entity", list(ds.shape))
            nan_str = ""
            if ds.stats.get("nan_fraction", 0) > 0:
                nan_str = f", NaN: {ds.stats['nan_fraction']:.1%}"
            w(f"    # shape per entity: {tuple(shape_str)}, dtype: {ds.dtype}, range: [{ds.stats.get('min', '?')}, {ds.stats.get('max', '?')}]{nan_str}")
        w()

    # Unclassified (per_entity arrays that need user disambiguation)
    unclassified = {n: d for n, d in result.datasets.items() if d.category == "ARTIFACT_OR_AXIS"}
    if unclassified:
        w("# === TODO: Classify these arrays as artifacts or shared axes ===")
        w("# Move each entry to either 'artifacts:' or 'shared:' section")
        w("# Artifacts = output observables (different per entity)")
        w("# Shared = axes/grids (same across all entities)")
        w("unclassified:")
        for name, ds in sorted(unclassified.items()):
            w(f"  - name: {name}")
            w(f"    dataset: /{name}")
            w(f"    # shape: {ds.shape}, dtype: {ds.dtype}")
            if ds.stats.get("min") is not None:
                w(f"    # range: [{ds.stats['min']}, {ds.stats['max']}]")
        w()

    # Shared axes
    shared = {n: d for n, d in result.datasets.items() if d.category == "SHARED_AXIS"}
    if shared:
        w("# === TODO: Confirm shared axes ===")
        w("# These datasets do NOT have the batch dimension → classified as shared")
        w("shared:")
        for name, ds in sorted(shared.items()):
            short_name = name.rsplit("/", 1)[-1] if "/" in name else name
            w(f"  - type: {short_name}")
            w(f"    dataset: /{name}")
            desc_parts = [f"shape: {tuple(ds.stats.get('shape', ds.shape))}"]
            if ds.stats.get("monotonic"):
                desc_parts.append("monotonic")
            if "range" in ds.stats:
                desc_parts.append(f"range [{ds.stats['range'][0]}, {ds.stats['range'][1]}]")
            if "step" in ds.stats:
                desc_parts.append(f"step={ds.stats['step']:.4g}")
            w(f"    # {', '.join(desc_parts)}")
        w()

    # Extra metadata
    extra = {n: d for n, d in result.datasets.items() if d.category == "EXTRA_METADATA"}
    if extra:
        w("# === Additional per-entity data (not under params/) ===")
        w("# TODO: Keep as metadata, promote to parameter, or remove?")
        w("extra_metadata:")
        for name, ds in sorted(extra.items()):
            w(f"  - dataset: /{name}")
            shape_str = ds.stats.get("shape_per_entity", list(ds.shape))
            w(f"    # shape per entity: {tuple(shape_str)}, dtype: {ds.dtype}")
            if ds.stats.get("min") is not None:
                w(f"    # range: [{ds.stats['min']}, {ds.stats['max']}]")
        w()

    # Provenance
    w()
    w("# === Provenance (optional) ===")
    w("# These fields are stored on the dataset container for tracking")
    w("# how and when data was generated.")
    w("provenance:")
    w('  # created_at: ""     # ISO date when data was generated')
    w('  # code_version: ""   # version of generating code')
    w('  # code_commit: ""    # git hash of generating code')
    if result.root_attrs or result.group_attrs:
        w("  # Discovered from HDF5 attributes:")
        for k, v in sorted(result.root_attrs.items()):
            w(f"  # {k}: {v}")
        for group_name, attrs in sorted(result.group_attrs.items()):
            w(f"  # {group_name}/ attrs: {attrs}")

    # Recommendations
    _add_recommendations(result)
    if result.recommendations:
        w()
        w("# === Recommendations for data producer ===")
        for rec in result.recommendations:
            w(f"# - {rec}")

    # Consistency verdict
    w()
    if result.consistency_issues:
        w(f"# === Consistency check: FAILED ({len(result.consistency_issues)} issues) ===")
        for issue in result.consistency_issues:
            w(f"# ! {issue}")
    else:
        n_checked = min(len(result.h5_files), 11)
        w(f"# === Consistency check: PASSED ({n_checked} files checked) ===")

    yaml_str = "\n".join(lines) + "\n"

    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w") as f:
            f.write(yaml_str)
        print(f"Draft YAML written to: {output_path}")

    return yaml_str


def _add_recommendations(result):
    """Add recommendations based on what's missing from the data."""
    recs = result.recommendations

    has_created_at = "created_at" in result.root_attrs or "generated_at" in result.root_attrs
    has_generator = any(k in result.root_attrs for k in ("generator", "code_version", "software"))
    has_material = any(k in result.root_attrs for k in ("material", "system", "compound"))

    if not has_created_at:
        recs.append("No 'created_at' timestamp — add as HDF5 root attribute")
    if not has_generator:
        recs.append("No 'generator' or 'code_version' — add as HDF5 root attribute")
    if not has_material:
        recs.append("No 'material' identifier — add as HDF5 root attribute")

    for name, ds in result.datasets.items():
        if ds.category == "PARAMETER" and ds.stats.get("is_constant"):
            short = name.rsplit("/", 1)[-1] if "/" in name else name
            recs.append(f"Parameter '{short}' is constant — consider moving to provenance/metadata")

    if result.layout == "per_entity" and len(result.h5_files) > 1:
        shared = [n for n, d in result.datasets.items() if d.category in ("SHARED_AXIS", "ARTIFACT_OR_AXIS")]
        if shared:
            recs.append(f"Shared arrays ({', '.join(shared)}) stored redundantly in every file — consider a single reference file")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def inspect_directory(directory):
    """Run the full 7-step inspection on a data directory.

    Args:
        directory: Path to the root data directory.

    Returns:
        InspectionResult with all findings.
    """
    directory = str(directory)
    result = InspectionResult(source_dir=directory, h5_files=[], file_pattern="*.h5", layout="per_entity")

    # Step 1: Find files
    h5_files, file_pattern = find_h5_files(directory)
    if not h5_files:
        print(f"No HDF5 files found in {directory}")
        return result
    result.h5_files = [str(f) for f in h5_files]
    result.file_pattern = file_pattern
    print(f"Found {len(h5_files)} HDF5 files ({file_pattern})")

    # Step 2: Tree walk (first file)
    first_file = str(h5_files[0])
    datasets, groups = walk_h5_tree(first_file)
    result.datasets = datasets
    result.groups = groups
    print(f"  {len(datasets)} datasets, {len(groups)} groups in {h5_files[0].name}")

    # Check for grouped layout (single file with many groups)
    if len(h5_files) == 1:
        is_grouped, entity_groups = detect_grouped_layout(first_file)
        if is_grouped:
            result.layout = "grouped"
            result.total_entities = len(entity_groups)
            print(f"  Layout: grouped ({result.total_entities} entity groups)")
            with h5py.File(first_file, "r") as f:
                inner_datasets = {}
                group_name = entity_groups[0]
                def visit_inner(name, obj):
                    if isinstance(obj, h5py.Dataset):
                        inner_datasets[f"{group_name}/{name}"] = DatasetInfo(
                            name=f"{group_name}/{name}",
                            shape=obj.shape, dtype=str(obj.dtype),
                            ndim=obj.ndim, size=obj.size,
                        )
                f[group_name].visititems(visit_inner)
            for n, d in inner_datasets.items():
                if n not in datasets:
                    datasets[n] = d
            result.datasets = datasets
            classify_datasets(datasets, groups, "grouped")
            read_sample_values(first_file, datasets, "grouped")
            root_attrs, group_attrs, dataset_attrs = read_attributes(first_file, datasets, groups)
            result.root_attrs = root_attrs
            result.group_attrs = group_attrs
            result.dataset_attrs = dataset_attrs
            return result

    # Step 3: Detect layout and classify
    layout, batch_size = detect_layout(datasets, h5_files)
    result.layout = layout
    result.batch_size = batch_size

    if layout == "batched":
        result.total_entities = batch_size * len(h5_files)
        print(f"  Layout: batched (axis-0 = {batch_size}, total = {result.total_entities:,})")
    else:
        result.total_entities = len(h5_files)
        print(f"  Layout: per_entity ({result.total_entities:,} files)")

    classify_datasets(datasets, groups, layout, batch_size)

    cats = Counter(d.category for d in datasets.values())
    print(f"  Classification: {dict(cats)}")

    # Step 4: Sample values
    read_sample_values(first_file, datasets, layout, batch_size, all_h5_files=h5_files)

    # Step 5: Attributes
    root_attrs, group_attrs, dataset_attrs = read_attributes(first_file, datasets, groups)
    result.root_attrs = root_attrs
    result.group_attrs = group_attrs
    result.dataset_attrs = dataset_attrs
    if root_attrs:
        print(f"  Root attrs: {root_attrs}")

    # Step 6: Consistency
    issues = check_consistency(h5_files, datasets, layout, batch_size)
    result.consistency_issues = issues
    if issues:
        print(f"  Consistency: FAILED ({len(issues)} issues)")
        for issue in issues:
            print(f"    ! {issue}")
    else:
        print(f"  Consistency: PASSED")

    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Inspect HDF5 data directory and generate a draft YAML contract."
    )
    parser.add_argument("directory", help="Path to the data directory")
    parser.add_argument(
        "--output", "-o",
        help="Output path for draft YAML (default: datasets/draft_<dirname>.yml)",
    )
    args = parser.parse_args()

    directory = os.path.abspath(args.directory)
    if not os.path.isdir(directory):
        print(f"Error: {directory} is not a directory", file=sys.stderr)
        sys.exit(1)

    result = inspect_directory(directory)

    if not result.h5_files:
        sys.exit(1)

    output = args.output
    if not output:
        dirname = Path(directory).name.lower().replace(" ", "_").replace("-", "_")
        output = f"datasets/draft_{dirname}.yml"

    yaml_str = emit_draft_yaml(result, output)
    print()
    print(yaml_str)


if __name__ == "__main__":
    main()

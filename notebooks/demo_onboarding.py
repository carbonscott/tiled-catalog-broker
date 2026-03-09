import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Onboarding Pipeline Demo

    **inspect → refine YAML → generate manifest → ingest**

    This notebook walks through the full pipeline for bringing a new HDF5
    dataset into the Tiled catalog broker. We'll show:

    1. **Inspect** — what the auto-inspector discovers from raw HDF5 files
    2. **Refine** — the gap between the draft YAML and a finalized contract
    3. **Generate** — producing Parquet manifests from the finalized YAML
    4. **Ingest** — registering into the catalog and querying the result
    """)
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Setup check

    If you can see the output below without errors, your environment is working.
    """)
    return


@app.cell
def _():
    import sys
    import os
    from pathlib import Path

    # Show Python info
    print(f"Python: {sys.version}")
    print(f"CWD:    {os.getcwd()}")
    return (Path,)


@app.cell
def _():
    # Verify key dependencies are importable
    def _check_deps():
        deps = {}
        for name in ["h5py", "pandas", "pyarrow", "numpy", "ruamel.yaml"]:
            try:
                mod = __import__(name.replace(".", "_") if "." in name else name)
                deps[name] = getattr(mod, "__version__", "ok")
            except ImportError:
                deps[name] = "MISSING"
        return deps

    _check_deps()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 1: Inspect an HDF5 data directory

    The inspector runs a 7-step pipeline on a directory of HDF5 files:

    1. **Find files** — recursively glob for `*.h5` / `*.hdf5` and infer the file pattern
    2. **Tree walk** — open the first file, visit every dataset and group, record shapes and dtypes
    3. **Detect layout & classify** — determine how entities are organized (see below), then label each dataset
    4. **Sample values** — read a small slice of each dataset to compute stats (range, uniqueness, NaN fraction)
    5. **Read attributes** — collect HDF5 attributes from root, groups, and datasets
    6. **Consistency check** — compare structure across up to 10 additional files (missing/extra datasets, shape mismatches)
    7. **Emit draft YAML** — produce a config with TODO markers for fields requiring human judgment

    ### Layout detection

    The inspector decides how entities map to files using this logic:

    | Check | Result |
    |-------|--------|
    | Has scalar datasets **and** many files | **per_entity** — one file = one entity, scalars are parameters |
    | 3+ datasets share the same axis-0 length (> 1) | **batched** — entities stacked along axis-0, `total = batch_size x n_files` |
    | Single file with many top-level groups each containing datasets | **grouped** — one group = one entity |
    | Otherwise | **per_entity** (default) |

    ### Dataset classification (depends on layout)

    **Batched layout:**

    | Condition | Category |
    |-----------|----------|
    | 1-D array with `shape[0] == batch_size`, inside a group where *all* children are 1-D with the same axis-0 | **PARAMETER** |
    | N-D array (ndim > 1) with `shape[0] == batch_size` | **ARTIFACT** |
    | 1-D array with `shape[0] == batch_size`, not in a parameter group | **EXTRA_METADATA** |
    | Array whose axis-0 does *not* match `batch_size` | **SHARED_AXIS** |
    | Scalar (ndim == 0) | **PARAMETER** |

    **Per-entity layout:**

    | Condition | Category |
    |-----------|----------|
    | Scalar (ndim == 0) | **PARAMETER** |
    | Any array (ndim >= 1) | **ARTIFACT_OR_AXIS** (needs human disambiguation) |
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Run the inspector

    We'll inspect the EDRIXS simulation data as a concrete example.
    """)
    return


@app.cell
def _():
    # Point to the EDRIXS data directory as our example
    DATA_DIR = "/sdf/group/mli/samklein/code/sbi_maq/results/edrixs_tsnpe_scaled_data/initial_data_proper"

    # Import the inspection engine
    from data_catalog_service.inspect import inspect_directory, emit_draft_yaml

    result = inspect_directory(DATA_DIR)
    print(f"\nLayout:     {result.layout}")
    print(f"Files:      {len(result.h5_files)}")
    print(f"Entities:   {result.total_entities:,}")
    print(f"Datasets:   {len(result.datasets)}")
    print(f"Consistent: {'YES' if not result.consistency_issues else 'NO'}")
    return emit_draft_yaml, result


@app.cell
def _(mo, result):
    # Show per-dataset classification summary
    from collections import Counter
    _cats = Counter(d.category for d in result.datasets.values())

    def _build_classification_table():
        table_rows = []
        for ds_name, ds in sorted(result.datasets.items()):
            stats_str = ""
            if "min" in ds.stats and ds.stats["min"] is not None:
                stats_str = f"[{ds.stats['min']}, {ds.stats['max']}]"
            elif "range" in ds.stats:
                stats_str = f"[{ds.stats['range'][0]}, {ds.stats['range'][1]}]"
            elif "shape_per_entity" in ds.stats:
                stats_str = f"per-entity shape: {tuple(ds.stats['shape_per_entity'])}"
            table_rows.append(
                f"| `{ds_name}` | {ds.shape} | `{ds.dtype}` | **{ds.category}** | {stats_str} |"
            )
        return "\n    ".join(table_rows)

    _class_table = _build_classification_table()
    mo.md(f"""
    ### Classification results

    Category counts: {dict(_cats)}

    | Dataset | Shape | Dtype | Category | Stats |
    |---------|-------|-------|----------|-------|
    {_class_table}
    """)
    return


@app.cell
def _(emit_draft_yaml, mo, result):
    # Show the auto-generated draft YAML
    draft_yaml = emit_draft_yaml(result)
    mo.md(f"### Draft YAML (auto-generated)\n```yaml\n{draft_yaml}\n```")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 2: Refine the draft YAML

    The draft YAML has TODO markers for fields requiring human judgment.
    Before filling those in, it helps to understand how the YAML is validated
    and what the allowed values are.

    ### Validation: two layers

    When the finalized YAML is passed to `broker-generate-yaml`, it runs
    `validate(cfg)` which enforces two layers:

    **Hard errors** (raises `ValidationError`, blocks manifest generation):

    | Check | Example |
    |-------|---------|
    | Missing `key` or `label` | Left as empty string from draft |
    | Missing `data` section or `data.directory` doesn't exist | Typo in path |
    | Invalid `data.layout` | Must be `per_entity`, `batched`, or `grouped` |
    | No `artifacts` defined, or artifact missing `type`/`dataset` | Deleted the section |
    | Invalid `parameters.location` | Must be `root_scalars`, `group`, `group_scalars`, or `manifest` |

    **Soft warnings** (printed but doesn't block):

    | Check | Example |
    |-------|---------|
    | Vocabulary value not in `catalog_model.yml` | `method: [FOOBAR]` warns but still runs |
    | Cross-field logic | `data_type: experimental` without `facility` |
    | Missing recommended fields | No `material` specified |

    The vocabulary fields (`method`, `data_type`, `material`, `producer`, etc.)
    are **warnings, not errors**. This is intentional — you can ingest data with
    a new material that hasn't been added to `catalog_model.yml` yet. The model
    is a guide, not a gate.
    """)
    return


@app.cell
def _(mo):
    # Load and display the semantic model
    from data_catalog_service.schema import load_catalog_model, get_allowed_values

    catalog_model = load_catalog_model()

    vocab_sections = [
        ("methods", "Scientific methods / observable types"),
        ("data_types", "simulation, experimental, benchmark, optimization"),
        ("materials", "Target materials or systems"),
        ("producers", "Simulation codes that generate data"),
        ("facilities", "Where experimental data was collected"),
        ("projects", "Scientific projects or collaborations"),
    ]

    def _build_vocab_table():
        table_rows = []
        for section_key, description in vocab_sections:
            entries = catalog_model.get(section_key, [])
            ids = [e["id"] for e in entries]
            table_rows.append(f"| `{section_key}` | {description} | {', '.join(f'`{i}`' for i in ids)} |")
        return "\n    ".join(table_rows)

    vocab_table = _build_vocab_table()

    mo.md(f"""
    ### Semantic model: `catalog_model.yml`

    The catalog model defines controlled vocabularies for dataset metadata.
    The inspector shows these as options in the draft YAML TODO comments,
    and the validator checks against them (as warnings).

    | Field | Description | Allowed values |
    |-------|-------------|----------------|
    {vocab_table}

    To add a new vocabulary entry (e.g. a new material), just add it to
    `schema/catalog_model.yml` — no code changes needed.
    """)
    return (catalog_model,)


@app.cell
def _(catalog_model, mo):
    # Show required vs optional dataset metadata fields
    def _build_field_table(fields):
        table_rows = []
        for fld in fields:
            enum_ref = fld.get("enum_ref", "")
            enum_str = f"(from `{enum_ref}`)" if enum_ref else ""
            table_rows.append(f"| `{fld['name']}` | {fld['type']} | {fld.get('description', '')} {enum_str} |")
        return "\n    ".join(table_rows)

    _ds_fields = catalog_model.get("dataset_fields", {})
    req_table = _build_field_table(_ds_fields.get("required", []))
    opt_table = _build_field_table(_ds_fields.get("optional", []))

    mo.md(f"""
    ### Dataset metadata fields

    **Required** (validation warns if missing):

    | Field | Type | Description |
    |-------|------|-------------|
    {req_table}

    **Optional** (recommended for discoverability):

    | Field | Type | Description |
    |-------|------|-------------|
    {opt_table}
    """)
    return


@app.cell
def _(mo):
    # Demonstrate validation on a bad YAML
    from data_catalog_service.schema import validate, ValidationError

    bad_cfg = {
        "key": "",
        "label": "",
        "data": {"directory": "/nonexistent/path", "layout": "invalid"},
        "metadata": {"method": ["FOOBAR"], "data_type": "simulation"},
    }

    error_msg = ""
    warning_msg = ""
    try:
        warnings = validate(bad_cfg)
        warning_msg = "\n".join(f"  - {w}" for w in warnings)
    except ValidationError as e:
        error_msg = "\n".join(f"  - {err}" for err in e.errors)

    mo.md(f"""
    ### Validation example: what happens with a bad YAML

    Given this intentionally broken config:
    ```yaml
    key: ""
    label: ""
    data:
      directory: /nonexistent/path
      layout: invalid
    metadata:
      method: [FOOBAR]
      data_type: simulation
    ```

    **Hard errors** (blocks generation):
    ```
    {error_msg}
    ```

    These must be fixed before the pipeline will run.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Filling in the draft

    With the semantic model and validation rules in mind, refinement is
    straightforward:

    1. **`key` and `label`** — pick a name for the Tiled hierarchy
       (e.g. `EDRIXS_SBI`) and a human-readable label
    2. **`metadata`** — select values from the controlled vocabulary
       tables above; the draft shows allowed options in TODO comments
    3. **`parameters`** — verify the auto-detected `location` and `group`
       (see below); keep the discovered ranges as comments for documentation
    4. **`artifacts`** — rename types if the auto-detected HDF5 dataset
       names aren't meaningful (e.g. `spectra` → `rixs_spectrum`)
    5. **Reclassify** — move any mis-classified datasets between
       `artifacts:`, `shared:`, and `extra_metadata:` sections
    6. **`provenance`** (optional) — add code version, round number, etc.
       The inspector discovers some of these from HDF5 attributes
       (e.g. `round_number`)

    ### Parameter locations

    The `parameters.location` field tells the manifest generator where to
    find per-entity parameter values in the HDF5 files:

    | Location | Meaning | Typical layout |
    |----------|---------|----------------|
    | `root_scalars` | Scalar datasets at the HDF5 file root | per_entity |
    | `group` | Datasets inside a named HDF5 group (e.g. `/params`) | batched |
    | `group_scalars` | Scalar datasets inside each entity's group | grouped |
    | `manifest` | Parameters from an external CSV/Parquet file, not from HDF5 | any |

    For our EDRIXS example, `location: group` with `group: /params` means
    the HDF5 files have a `/params` group containing 12 parameter arrays,
    each with shape `(2000,)` — one value per entity in the batch.

    Below we show the finalized YAML for comparison.
    """)
    return


@app.cell
def _(Path, mo):
    # Show the finalized YAML for comparison
    finalized_path = Path(__file__).parent.parent / "datasets" / "edrixs_sbi.yml"
    finalized_yaml = finalized_path.read_text()
    mo.md(
        "### Finalized YAML (human-refined)\n\n"
        "Compare to the draft above — the user filled in:\n"
        "- `key` and `label`\n"
        "- `metadata` fields (method, data_type, material, producer, project)\n"
        "- Renamed artifact type from `spectra` → `rixs_spectrum`\n"
        "- Removed TODO comments\n"
        "- Added provenance (optional)\n\n"
        f"```yaml\n{finalized_yaml}\n```"
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## What's left after inspection?

    The inspector gets you ~70% of the way. What it **cannot** determine
    automatically:

    | Field | Why it needs human input |
    |-------|-------------------------|
    | `key` | Naming convention for the Tiled hierarchy |
    | `label` | Human-readable name |
    | `method` | Scientific method (EDRIXS, RIXS, VDP, ...) |
    | `data_type` | simulation vs. experimental |
    | `material` | What system was studied |
    | `producer` | What code generated the data |
    | Artifact names | `spectra` → `rixs_spectrum` (domain knowledge) |
    | Provenance | Code version, round number, etc. |

    **Feedback loop**: The "Recommendations" section in the draft YAML
    tells data producers what HDF5 attributes to add so that future
    inspections are more complete.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Next steps (coming soon)

    - **Step 3: Generate manifests** from the finalized YAML
    - **Step 4: Ingest** into the catalog database
    - **Step 5: Query** the registered data via Tiled
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Additional notes

    ### Consistency check

    The inspector compares structure across up to 10 files beyond the first
    one used for the tree walk. It flags four kinds of issues:

    - **Missing datasets** — a file is missing a dataset that the first file had
    - **Extra datasets** — a file has a dataset the first file didn't
    - **Shape mismatches** — same dataset name but different shape across files
    - **Shared axis value differences** — for SHARED_AXIS datasets, the inspector
      checks that the actual array values are identical across files (since they
      represent the same grid/axis everywhere)

    If no issues are found, the draft YAML gets a `# Consistency check: PASSED`
    footer. Otherwise each issue is listed so you know which files are
    structurally inconsistent before attempting manifest generation.

    ### Classification edge cases

    The auto-classifier can be fooled. For example, if a shared axis happens
    to have `shape[0] == batch_size`, it will be labeled **EXTRA_METADATA**
    (if 1-D) or **ARTIFACT** (if N-D) because the logic only checks whether
    axis-0 matches the batch size — it cannot tell that the array is identical
    across all entities. This is exactly the kind of mistake the human
    refinement step exists to catch: you'd move it from `extra_metadata:` or
    `artifacts:` to the `shared:` section in the YAML.

    The convention for batched layouts is that shared axes simply *don't* have
    the batch dimension — which is both storage-efficient and makes
    auto-classification work correctly.
    """)
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

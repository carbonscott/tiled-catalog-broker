import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Querying the Tiled Catalog

    This notebook demonstrates accessing data registered in the Tiled catalog.

    ## Data model

    ```
    BROAD_SIGMA/                                      ← Dataset container (key = slug of label)
    │ metadata:
    │   method: [RIXS]                                   searchable
    │   data_type: simulation                            searchable
    │   material: NiPS3                                  searchable
    │   producer: lajer2025Hamiltonian                   searchable
    │   prior_distribution: uniform                      searchable
    │   round: 0                                         searchable
    │   created_at: 2026-02-13                           provenance
    │   code_version: v1.0.0                             provenance
    │   code_commit: b6b05b7                             provenance
    │   shared_dataset_eloss: /eloss                     shared axis pointer
    │   shared_dataset_omega_bounds: /omega_bounds       shared axis pointer
    │
    ├── eloss         (151,) float64                   ← Shared axis (array child)
    ├── omega_bounds  (2,)   float64                   ← Shared axis (array child)
    │
    ├── BROAD_SIGMA_23e67506d910b/                    ← Entity container
    │   │ metadata:
    │   │   F2_dd: 3.42                                  ┐
    │   │   tenDq: 1.87                                  │ physics parameters
    │   │   sigma: 0.15                                  │ (queryable)
    │   │   ...12 total                                  ┘
    │   │   path_rixs_spectrum: batch_0/simulations.h5   ┐
    │   │   dataset_rixs_spectrum: /spectra              │ provenance locators
    │   │   index_rixs_spectrum: 0                       ┘
    │   │
    │   └── rixs_spectrum  (151, 40) float64           ← Artifact (array)
    │
    ├── BROAD_SIGMA_1a6e32f95230c/
    │   └── rixs_spectrum  (151, 40) float64
    │
    └── ... (10,000 entities total)
    ```

    The dataset key is the slug of the YAML's `label` (written by
    `tcb stamp-key`); entity keys are `{DATASET_KEY}_{uid}`. Shared axis
    arrays (energy grids, etc.) are registered once as direct children of
    the dataset container, alongside the entities.

    ## Reading an array

    | Where it comes from | How | When |
    |------|-----|-------------|
    | **The server** | `entity[artifact][:]` over HTTP | Anywhere the server is reachable. Always works, and the only option for an uploaded dataset |
    | **The original file** | Ask the catalog for the location → open with h5py | A cross-check here. In real use, worth it only for bulk reads on a machine where the files are mounted |
    """)
    return


@app.cell
def _():
    import os

    # ---------------------------------------------------------------
    # Replace with your dataset here.
    #
    # DATASET_KEY   the key `tcb stamp-key` wrote into your YAML
    # ARTIFACT_KEY  the artifact to read -- one of your YAML's
    #               `artifacts:` `type:` values. Leave it None to use
    #               whichever artifact the first entity carries.
    #
    # Everything else the notebook needs -- parameters, shared axes,
    # even where the HDF5 files live -- is read back out of the catalog,
    # so these two names are the whole configuration.
    # ---------------------------------------------------------------
    DATASET_KEY = os.environ.get("TCB_DEMO_DATASET", "BROAD_SIGMA")
    ARTIFACT_KEY = os.environ.get("TCB_DEMO_ARTIFACT") or None
    return ARTIFACT_KEY, DATASET_KEY, os


@app.cell
def _(os):
    from tiled.client import from_uri
    from tiled.queries import Key, Contains

    url = os.environ.get(
        "TILED_URL",
        "https://lcls-data-portal.slac.stanford.edu/tiled-test",
    )
    api_key = os.environ.get("TILED_API_KEY") or os.environ.get("TILED_KEY")

    # This kernel inherits the environment of the shell that launched
    # `marimo edit`, captured at launch. 
    if not api_key:
        raise RuntimeError(
            "TILED_API_KEY is not set in this kernel's environment.\n"
            "  1. In the shell you launch marimo from, run "
            "`env | grep TILED_API_KEY`. Unlike `echo`, that shows only "
            "exported variables -- if it prints nothing, re-run as "
            "`export TILED_API_KEY=<key>`.\n"
            "  2. Restart `marimo edit`. A server that was already running "
            "does not see variables exported afterwards."
        )

    client = from_uri(url, api_key=api_key)
    print(f"Connected to {url} ({len(client)} containers)")
    return Contains, Key, client


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Level 1: Dataset container

    Direct access by key — the slug of the dataset's `label`
    (`"Broad Sigma"` → `BROAD_SIGMA`), stamped into the YAML by
    `tcb stamp-key`.
    """)
    return


@app.cell
def _(DATASET_KEY, client, mo):
    ds = client[DATASET_KEY]
    ds_meta = dict(ds.metadata)

    # Separate user-facing metadata from internal tracking fields
    _public_fields = [
        ("method", "Scientific method"),
        ("data_type", "Simulation or experimental"),
        ("material", "Target material"),
        ("producer", "Code repo that generated the data"),
        ("prior_distribution", "How parameters were sampled"),
        ("round", "SBI iteration round"),
        ("created_at", "When the data was generated"),
        ("code_version", "Producer release tag"),
        ("code_commit", "Producer git commit"),
    ]

    _meta_rows = "\n    ".join(
        f"| {_label} | `{ds_meta.get(_key, '—')}` |"
        for _key, _label in _public_fields
    )

    # Shared axes: pointers in dataset metadata, arrays as direct children
    shared_axes = {
        _k.replace("shared_dataset_", ""): ds_meta[_k]
        for _k in ds_meta
        if _k.startswith("shared_dataset_")
    }
    _shared_rows = "\n    ".join(
        f"| `{_ax}` | `{_src}` |" for _ax, _src in shared_axes.items()
    )

    mo.md(f"""
    **Key:** `{DATASET_KEY}`

    **Children:** {len(ds):,} (entities + {len(shared_axes)} shared axis arrays)

    ### Metadata

    | Field | Value |
    |-------|-------|
    {_meta_rows}

    ### Shared axes

    These HDF5 datasets are the same across all entities (energy grids, etc.)
    and are registered once as array children of the dataset container:

    | Axis | Source HDF5 dataset |
    |------|---------------------|
    {_shared_rows}
    """)
    return ds, shared_axes


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Searching by metadata

    Use `Contains` for list fields (like `method`), `Key` for scalar fields.
    """)
    return


@app.cell
def _(Contains, Key, client, mo):
    results = client.search(Contains("method", "RIXS")).search(
        Key("data_type") == "simulation"
    )
    mo.md(f"""
    ```python
    client.search(Contains("method", "RIXS")).search(Key("data_type") == "simulation")
    ```

    Found **{len(results)}** matching dataset(s): `{list(results)}`
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Level 2: Entity container

    Each entity has physics parameters as queryable metadata, plus
    **locator fields** recording which file and row it came from. Shared
    axis arrays live at the dataset level, so skip them when iterating
    entities.
    """)
    return


@app.cell
def _(ARTIFACT_KEY, ds, mo, shared_axes):
    # First child that is an entity (shared axes are array children, not entities)
    first_key = next(_k for _k in ds if _k not in shared_axes)
    entity = ds[first_key]
    ent_meta = dict(entity.metadata)

    # Resolve the artifact to read. Left unset, take the entity's first
    # child -- the catalog knows its own artifact names, so a notebook
    # pointed at an unfamiliar dataset still has something to show.
    artifact_key = ARTIFACT_KEY or next(iter(entity))
    if artifact_key not in entity:
        raise KeyError(
            f"entity '{first_key}' has no artifact '{artifact_key}'. "
            f"Available: {list(entity)}"
        )

    # Split metadata into categories
    physics_params = {}
    locators = {}
    _internal = {}
    for _k, _v in ent_meta.items():
        if _k.startswith(("path_", "dataset_", "index_")):
            locators[_k] = _v
        elif _k in ("key", "uid", "amsc_public"):
            _internal[_k] = _v
        else:
            physics_params[_k] = _v

    mo.md(f"""
    ### Entity: `{first_key}`

    **Children:** `{list(entity)}`

    **Metadata categories:**
    - Physics parameters: {len(physics_params)} fields
    - Artifact locators: {len(locators)} fields (provenance)
    - Internal: {len(_internal)} fields
    """)
    return artifact_key, entity, locators, physics_params


@app.cell(hide_code=True)
def _(mo, physics_params):
    import pandas as pd

    params_df = pd.DataFrame([physics_params]).T.rename(columns={0: "value"})
    mo.md("### Physics parameters (queryable)")
    return (params_df,)


@app.cell(hide_code=True)
def _(params_df):
    params_df
    return


@app.cell(hide_code=True)
def _(locators, mo):
    _loc_rows = "\n    ".join(
        f"| `{_k}` | `{_v}` |" for _k, _v in sorted(locators.items())
    )
    mo.md(f"""
    ### Artifact locators (provenance, and queryable)

    Recorded so you can *search* on provenance -- which file an entity came
    from, which HDF5 dataset, which row -- and so the trail survives even
    for uploaded datasets whose bytes now live server-side. They are
    relative paths, so they are not what you open a file with; the next
    section gets the absolute location from the catalog instead.

    | Key | Value |
    |-----|-------|
    {_loc_rows}
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Level 3: Artifact (array data)

    **Straight from the file** — open the HDF5 directly. You do not
    assemble the path: Tiled recorded it at registration, so ask the catalog
    where the file is and open what it names.
    """)
    return


@app.cell
def _(artifact_key, entity, mo, os):
    import h5py
    from urllib.parse import urlparse

    # An externally-registered artifact carries a DataSource, and that
    # DataSource is self-describing: its asset `data_uri` is the absolute
    # path, and its `parameters` hold the HDF5 dataset path and (for batched
    # layouts) the row. So there is no base directory to configure per
    # machine and no path to join by hand -- ask, then open.
    #
    # The file can be unreachable for two reasons, and both are detected
    # here rather than assumed:
    #   1. the dataset was registered with `tcb register --upload`, so Tiled
    #      owns the bytes and no external file exists to open;
    #   2. the file exists, but is not mounted where this notebook runs.
    # The read through the server below always works, which is why the
    # plot uses it.
    _external = [
        _s for _s in (entity[artifact_key].data_sources() or [])
        if _s.management == "external"
    ]

    spectrum_a = None
    if not _external:
        _status = (
            "**No file to open** — this dataset has no external data "
            "source. It was registered with `--upload`, so Tiled holds the "
            "array itself. The read through the server below is the way in."
        )
    else:
        _source = _external[0]
        _uri = _source.assets[0].data_uri
        _path = urlparse(_uri).path
        _ds_path = _source.parameters["dataset"]
        _row = _source.parameters.get("slice")

        if not os.path.exists(_path):
            _status = (
                f"**File not reachable here** — the catalog points at "
                f"`{_path}`, which is not mounted on this machine. Run the "
                f"notebook where the data lives, or just use the read "
                f"through the server below, which does not care."
            )
        else:
            with h5py.File(_path, "r") as _f:
                spectrum_a = (
                    _f[_ds_path][int(_row)] if _row is not None
                    else _f[_ds_path][:]
                )
            _status = f"""
    The catalog said where to look — none of this was constructed by hand:

    | From the DataSource | Value |
    |---|---|
    | asset `data_uri` | `{_uri}` |
    | `parameters["dataset"]` | `{_ds_path}` |
    | `parameters["slice"]` | `{_row}` |

    ```python
    src = entity[artifact_key].data_sources()[0]
    row = src.parameters.get("slice")      # batched layouts only; None otherwise
    with h5py.File(urlparse(src.assets[0].data_uri).path) as f:
        ds = f[src.parameters["dataset"]]
        spectrum = ds[int(row)] if row is not None else ds[...]
    ```

    Shape: `{spectrum_a.shape}` (energy_loss x incident_energy) ·
    range [{spectrum_a.min():.2e}, {spectrum_a.max():.2e}]
    """

    mo.md(_status)
    return (spectrum_a,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Through the server** — the same array served over HTTP by Tiled. No
    filesystem access needed; this is also how shared axes are read, and it
    is what the plot below uses.
    """)
    return


@app.cell
def _(artifact_key, ds, entity, mo, shared_axes, spectrum_a):
    import numpy as np

    # This needs nothing but the server, so it is what the plot below
    # uses -- the notebook renders the same figure whether or not the
    # original files are reachable from here.
    spectrum = entity[artifact_key][:]

    _agreement = (
        "_The file was not read here, so there is nothing to compare against._"
        if spectrum_a is None else
        f"Identical to the direct file read: `{np.array_equal(spectrum, spectrum_a)}`"
    )

    # Shared axes are optional in the contract, so read one only if this
    # dataset declared any.
    _axis_note = "_This dataset declares no shared axes._"
    if shared_axes:
        _axis_name = next(iter(shared_axes))
        _axis = ds[_axis_name][:]
        _axis_note = (
            f"Shared axis `{_axis_name}`: {len(_axis)} points, "
            f"range [{_axis.min():.2f}, {_axis.max():.2f}]"
        )

    mo.md(f"""
    **Read through the server:**
    ```python
    spectrum = entity["{artifact_key}"][:]   # served by Tiled over HTTP
    axis = ds["<shared axis>"][:]            # shared axis, dataset-level child
    ```

    Shape: `{spectrum.shape}`. {_agreement}

    {_axis_note}
    """)
    return (spectrum,)


@app.cell
def _(artifact_key, mo, spectrum):
    import matplotlib.pyplot as plt

    # Shape varies by dataset, so pick a rendering from the array itself.
    _fig, _ax = plt.subplots(figsize=(8, 4))
    if spectrum.ndim == 1:
        _ax.plot(spectrum)
        _ax.set_xlabel("Index")
        _ax.set_ylabel("Value")
    elif spectrum.ndim == 2:
        # For BROAD_SIGMA axis 0 is energy loss, axis 1 incident energy.
        _im = _ax.imshow(spectrum, aspect="auto", origin="lower", cmap="viridis")
        _ax.set_xlabel("Axis 1 index")
        _ax.set_ylabel("Axis 0 index")
        _fig.colorbar(_im, ax=_ax, label="Intensity")
    else:
        _ax.text(0.5, 0.5, f"{spectrum.ndim}-D array — slice it to plot",
                 ha="center", va="center")
        _ax.set_axis_off()
    _ax.set_title(artifact_key)
    plt.tight_layout()
    mo.mpl.interactive(_fig)
    return


if __name__ == "__main__":
    app.run()

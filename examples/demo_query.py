import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # Querying the Tiled Catalog

    This notebook demonstrates accessing data registered in the Tiled catalog.

    ## Data model

    ```
    RIXS_SIM_BROAD_SIGMA/                             ← Dataset container
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
    ├── H_626a88ae/                                    ← Entity container
    │   │ metadata:
    │   │   F2_dd: 3.42                                  ┐
    │   │   tenDq: 1.87                                  │ physics parameters
    │   │   sigma: 0.15                                  │ (queryable)
    │   │   ...12 total                                  ┘
    │   │   path_rixs_spectrum: round_0/simulations.h5   ┐
    │   │   dataset_rixs_spectrum: /spectra               │ Mode A locators
    │   │   index_rixs_spectrum: 0                        ┘
    │   │
    │   └── rixs_spectrum  (151, 40) float64           ← Artifact (array)
    │
    ├── H_1c2f046d/
    │   └── rixs_spectrum  (151, 40) float64
    │
    └── ... (10,000 entities total)
    ```

    ## Two access modes

    | Mode | How | When to use |
    |------|-----|-------------|
    | **A (Expert)** | Read locator metadata → open HDF5 with h5py | Fast, local access, custom analysis |
    | **B (Visualizer)** | `entity["rixs_spectrum"].read()` via HTTP | Chunked remote access (needs server filesystem access) |
    """)
    return


@app.cell
def _():
    import os
    from tiled.client import from_uri
    from tiled.queries import Key, Contains

    url = os.environ.get(
        "TILED_URL",
        "https://lcls-data-portal.slac.stanford.edu/tiled-dev",
    )
    api_key = os.environ.get("TILED_API_KEY", os.environ.get("TILED_KEY", ""))

    client = from_uri(url, api_key=api_key)
    print(f"Connected to {url} ({len(client)} containers)")
    return Contains, Key, client, os


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Level 1: Dataset container

    Access via the key convention: `{METHOD}_{SIM|EXP}_{DISTINGUISHING_FEATURE}`
    """)
    return


@app.cell
def _(client, mo):
    # Direct access via key convention
    ds = client["RIXS_SIM_BROAD_SIGMA"]
    ds_meta = dict(ds.metadata)

    # Separate user-facing metadata from internal tracking fields
    public_fields = [
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

    meta_rows = "\n    ".join(
        f"| {label} | `{ds_meta.get(key, '—')}` |"
        for key, label in public_fields
    )

    # Shared axes stored in dataset metadata
    shared_keys = [k for k in ds_meta if k.startswith("shared_dataset_")]
    shared_rows = "\n    ".join(
        f"| `{k.replace('shared_dataset_', '')}` | `{ds_meta[k]}` |"
        for k in shared_keys
    )

    mo.md(f"""
    **Key:** `RIXS_SIM_BROAD_SIGMA`

    **Entities:** {len(ds):,}

    ### Metadata

    | Field | Value |
    |-------|-------|
    {meta_rows}

    ### Shared axes

    These HDF5 datasets are the same across all entities (energy grids, etc.):

    | Axis | HDF5 dataset |
    |------|-------------|
    {shared_rows}
    """)
    return (ds,)


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

    Each entity has physics parameters as queryable metadata,
    plus **locator fields** for Mode A access.
    """)
    return


@app.cell
def _(ds, mo):
    first_key = list(ds)[:1][0]
    entity = ds[first_key]
    ent_meta = dict(entity.metadata)

    # Split metadata into categories
    physics_params = {}
    locators = {}
    internal = {}
    for k, v in ent_meta.items():
        if k.startswith(("path_", "dataset_", "index_")):
            locators[k] = v
        elif k in ("key", "uid"):
            internal[k] = v
        else:
            physics_params[k] = v

    mo.md(f"""
    ### Entity: `{first_key}`

    **Children:** `{list(entity)}`

    **Metadata categories:**
    - Physics parameters: {len(physics_params)} fields
    - Artifact locators: {len(locators)} fields (Mode A)
    - Internal: {len(internal)} fields (key, uid)
    """)
    return ent_meta, locators, physics_params


@app.cell
def _(mo, physics_params):
    import pandas as pd

    params_df = pd.DataFrame([physics_params]).T.rename(columns={0: "value"})
    mo.md("### Physics parameters (queryable)")
    return (params_df,)


@app.cell
def _(params_df):
    params_df
    return


@app.cell
def _(locators, mo):
    loc_rows = "\n    ".join(
        f"| `{k}` | `{v}` |" for k, v in sorted(locators.items())
    )
    mo.md(f"""
    ### Artifact locators (Mode A)

    These metadata fields let you open the HDF5 file directly:

    | Key | Value |
    |-----|-------|
    {loc_rows}
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Level 3: Artifact (array data)

    Load the spectrum using Mode A — read directly from HDF5 using the locator metadata.
    """)
    return


@app.cell
def _(ent_meta, mo, os):
    import h5py

    h5_rel = ent_meta["path_rixs_spectrum"]
    ds_path = ent_meta["dataset_rixs_spectrum"]
    batch_idx = ent_meta.get("index_rixs_spectrum")

    base_dir = "/sdf/group/mli/samklein/code/sbi_maq/results/edrixs_tsnpe_scaled_data/initial_data_proper"
    full_path = os.path.join(base_dir, h5_rel)

    with h5py.File(full_path, "r") as f:
        if batch_idx is not None:
            spectrum = f[ds_path][int(batch_idx)]
        else:
            spectrum = f[ds_path][:]

    mo.md(f"""
    **Mode A read:**
    ```python
    # From entity metadata:
    path  = "{h5_rel}"
    dataset = "{ds_path}"
    index = {batch_idx}

    with h5py.File(base_dir + "/" + path) as f:
        spectrum = f["{ds_path}"][{batch_idx}]
    ```

    Shape: `{spectrum.shape}` (energy_loss x incident_energy)

    Range: [{spectrum.min():.2e}, {spectrum.max():.2e}]
    """)
    return (spectrum,)


@app.cell
def _(mo, spectrum):
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 4))
    im = ax.imshow(
        spectrum,
        aspect="auto",
        origin="lower",
        cmap="viridis",
    )
    ax.set_xlabel("Incident energy index")
    ax.set_ylabel("Energy loss index")
    ax.set_title("RIXS spectrum")
    fig.colorbar(im, ax=ax, label="Intensity")
    plt.tight_layout()
    mo.mpl.interactive(fig)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

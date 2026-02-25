# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "tiled[server]",
#     "pandas",
#     "h5py",
#     "numpy",
#     "ruamel.yaml",
#     "matplotlib",
#     "torch",
# ]
# ///
import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # VDP Dual-Mode M(H) Dataset Demo

    **VDP provides TWO access modes for the SAME data:**

    | Mode | Pattern | Best For |
    |------|---------|----------|
    | **Mode A (Expert)** | `query_manifest()` → direct HDF5 | ML pipelines, bulk loading |
    | **Mode B (Visualizer)** | `h["mh_powder_30T"][:]` | Interactive exploration, remote |

    ## Architecture

    ```
    /                           <- Root
      /H_636ce3e4/              <- Container (Hamiltonian)
          metadata: {Ja_meV, Jb_meV, Jc_meV, Dc_meV, path_mh_powder_30T, ...}
          mh_powder_30T         <- Array (200,) via Tiled adapter
          ins_12meV             <- Array (600x400)
          ...
    ```

    **Prerequisites:** Start the Tiled server:
    ```bash
    cd $PROJ_VDP/tiled_poc
    uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret
    ```
    """)
    return


@app.cell
def _():
    import marimo as mo
    import numpy as np
    import time
    import sys
    from pathlib import Path

    # Add tiled_poc directory to path for broker package imports
    sys.path.insert(0, str(Path(__file__).parent.parent))

    # Use config module for configuration
    from broker.config import get_tiled_url, get_api_key

    TILED_URL = get_tiled_url()
    API_KEY = get_api_key()

    return mo, np, time, TILED_URL, API_KEY, get_tiled_url, get_api_key


@app.cell
def _(TILED_URL, API_KEY, mo):
    # Connect to Tiled server
    from tiled.client import from_uri
    from tiled.queries import Key

    client = from_uri(TILED_URL, api_key=API_KEY)
    mo.md(f"**Connected to VDP server at `{TILED_URL}`.** Catalog contains **{len(client)}** entity containers.")
    return Key, client, from_uri


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Browse Hierarchical Structure

    Each entity is a **container** with:
    - **Metadata**: Physics parameters + artifact paths
    - **Children**: Artifacts accessible via Tiled adapters
    """)
    return


@app.cell
def _(client, mo):
    # Get first entity container
    ent_keys = list(client.keys())[:5]
    ent_key = ent_keys[0] if ent_keys else None

    if ent_key:
        h = client[ent_key]
        children = list(h.keys())

        # Physics parameters
        physics_keys = ["Ja_meV", "Jb_meV", "Jc_meV", "Dc_meV", "spin_s", "g_factor"]
        physics_rows = "\n".join([
            f"| {k} | {h.metadata.get(k, 'N/A'):.4f} |"
            if isinstance(h.metadata.get(k), float) else f"| {k} | {h.metadata.get(k, 'N/A')} |"
            for k in physics_keys
        ])

        # Path keys (for Mode A)
        path_keys = [k for k in h.metadata.keys() if k.startswith("path_")]

        # Children table
        child_rows = "\n".join([
            f"| {ck} | `{h[ck].shape}` | `{h[ck].dtype}` |"
            for ck in children[:6]
        ])

        _output = mo.md(f"""
### Container: `{ent_key}`

**Physics Parameters (Metadata):**

| Parameter | Value |
|-----------|-------|
{physics_rows}

**Artifact Paths (Mode A):** {len(path_keys)} paths available in metadata

**Children (Mode B):**

| Key | Shape | Dtype |
|-----|-------|-------|
{child_rows}
{"| ... | | |" if len(children) > 6 else ""}
        """)
    else:
        h = None
        children = []
        physics_keys = []
        path_keys = []
        _output = mo.md("No entities found. Run `register_catalog.py` first.")

    _output
    return h, ent_key, ent_keys, children, physics_keys, path_keys


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Mode A: Expert Path-Based Access

    **Best for:** ML pipelines, bulk loading, maximum performance

    ```python
    from query_manifest import build_mh_dataset

    # Query Tiled -> get paths -> load directly from HDF5
    X, h_grid, Theta, manifest = build_mh_dataset(client, axis="powder", Hmax_T=30)
    ```

    **Tiled provides:** Queryable manifest with HDF5 paths
    **You do:** Direct HDF5 loading (no HTTP overhead)
    """)
    return


@app.cell
def _(client, mo, time):
    from broker.query_manifest import query_manifest, load_from_manifest, build_mh_dataset

    # Step 1: Query manifest
    _t0 = time.perf_counter()
    manifest = query_manifest(client, axis="powder", Hmax_T=30)
    query_time = (time.perf_counter() - _t0) * 1000

    # Step 2: Load from HDF5
    _t1 = time.perf_counter()
    X_a, Theta_a = load_from_manifest(manifest)
    load_time = (time.perf_counter() - _t1) * 1000

    total_time_a = query_time + load_time

    mo.md(f"""
### Mode A Results

| Step | Time |
|------|------|
| Query manifest | {query_time:.1f} ms |
| Load from HDF5 | {load_time:.1f} ms |
| **Total** | **{total_time_a:.1f} ms** |

**Loaded:** {len(X_a)} curves, shape `{X_a.shape}`
    """)
    return query_manifest, load_from_manifest, build_mh_dataset, manifest, X_a, Theta_a, query_time, load_time, total_time_a


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Mode B: Tiled Adapter Access

    **Best for:** Interactive exploration, visualization, remote access

    ```python
    # Access arrays directly via Tiled
    h = client["H_636ce3e4"]
    data = h["mh_powder_30T"][:]     # Full array
    slice = h["ins_12meV"][100:200, 50:150]  # Partial read
    ```

    **Tiled provides:** Chunked array access via HTTP
    **You get:** Remote access, slicing, no file management
    """)
    return


@app.cell
def _(client, np, mo, time):
    # Mode B: Load via Tiled adapters
    def build_mh_dataset_mode_b(tiled_client, *, axis="powder", Hmax_T=30, clamp_H0=True):
        """Build M(H) dataset using Tiled adapters (Mode B)."""
        artifact_key = f"mh_{axis}_{int(Hmax_T)}T"

        X_list = []
        Theta_list = []

        # Use .items() to iterate all items (not paginated like .keys())
        for ent_key, h in tiled_client.items():
            if artifact_key not in h.keys():
                continue

            params = h.metadata
            spin_s = params.get("spin_s", 2.5)
            g_factor = params.get("g_factor", 2.0)
            Msat = g_factor * spin_s

            # Load via Tiled adapter (HTTP)
            M = h[artifact_key][:]

            if clamp_H0:
                M = M.copy()
                M[0] = 0.0

            X_list.append(M / Msat)
            Theta_list.append([
                params["Ja_meV"], params["Jb_meV"],
                params["Jc_meV"], params["Dc_meV"],
                spin_s, g_factor,
            ])

        if not X_list:
            raise ValueError(f"No curves found for axis={axis}, Hmax_T={Hmax_T}")

        X = np.stack(X_list, dtype=np.float32)
        Theta = np.array(Theta_list, dtype=np.float32)
        h_grid = np.linspace(0, 1, X.shape[1], dtype=np.float32)

        return X, h_grid, Theta

    _t0 = time.perf_counter()
    X_b, h_grid, Theta_b = build_mh_dataset_mode_b(client, axis="powder", Hmax_T=30)
    total_time_b = (time.perf_counter() - _t0) * 1000

    mo.md(f"""
### Mode B Results

| Metric | Value |
|--------|-------|
| **Total time** | **{total_time_b:.1f} ms** |
| Curves loaded | {len(X_b)} |
| Shape | `{X_b.shape}` |
    """)
    return build_mh_dataset_mode_b, X_b, h_grid, Theta_b, total_time_b


@app.cell(hide_code=True)
def _(mo, total_time_a, total_time_b):
    ratio = total_time_a / total_time_b if total_time_b > 0 else 0

    mo.md(f"""
## Performance Comparison

| Mode | Time | Use Case |
|------|------|----------|
| **Mode A (Expert)** | {total_time_a:.1f} ms | ML training, bulk loading |
| **Mode B (Visualizer)** | {total_time_b:.1f} ms | Interactive exploration |

**Ratio:** Mode A / Mode B = {ratio:.2f}x
    """)
    return (ratio,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Query Filtering (Server-Side)

    Both modes benefit from Tiled's server-side filtering:

    ```python
    # Filter BEFORE loading
    ferromagnetic = client.search(Key("Ja_meV") > 0)
    X, h_grid, Theta, manifest = build_mh_dataset(ferromagnetic, axis="powder", Hmax_T=30)
    ```
    """)
    return


@app.cell
def _(Key, client, mo):
    # Query examples
    ferromagnetic = client.search(Key("Ja_meV") > 0)
    antiferromagnetic = client.search(Key("Ja_meV") < 0)
    afm_easy_axis = client.search(Key("Ja_meV") < 0).search(Key("Dc_meV") < 0)

    mo.md(f"""
### Query Results

| Query | Count |
|-------|-------|
| All entities | **{len(client)}** |
| `Key("Ja_meV") > 0` (ferromagnetic) | **{len(ferromagnetic)}** |
| `Key("Ja_meV") < 0` (antiferromagnetic) | **{len(antiferromagnetic)}** |
| `Key("Ja_meV") < 0` AND `Key("Dc_meV") < 0` | **{len(afm_easy_axis)}** |
    """)
    return ferromagnetic, antiferromagnetic, afm_easy_axis


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## PyTorch DataLoader Integration

    Wrap VDP in a PyTorch Dataset for ML training:

    ```python
    dataset = VDPDataset(client, artifact_key="mh_powder_30T")
    dataloader = DataLoader(dataset, batch_size=4, shuffle=True)

    for batch_data, batch_params in dataloader:
        predictions = model(batch_data)
        loss = criterion(predictions, batch_params)
    ```
    """)
    return


@app.cell
def _(np, client):
    import torch
    from torch.utils.data import Dataset, DataLoader

    class VDPDataset(Dataset):
        """PyTorch Dataset using VDP hierarchical catalog."""

        def __init__(self, tiled_client, artifact_key="mh_powder_30T"):
            self.client = tiled_client
            self.artifact_key = artifact_key
            # Cache keys that have the requested artifact
            self.ent_keys = [
                k for k in tiled_client.keys()
                if artifact_key in tiled_client[k].keys()
            ]

        def __len__(self):
            return len(self.ent_keys)

        def __getitem__(self, idx):
            ent_key = self.ent_keys[idx]
            h = self.client[ent_key]

            # Load via Tiled adapter
            data = h[self.artifact_key][:]

            # Physics params from container metadata
            params = h.metadata
            param_tensor = torch.tensor([
                params.get("Ja_meV", 0.0) or 0.0,
                params.get("Jb_meV", 0.0) or 0.0,
                params.get("Jc_meV", 0.0) or 0.0,
                params.get("Dc_meV", 0.0) or 0.0,
            ], dtype=torch.float32)

            data_tensor = torch.from_numpy(data.astype(np.float32))
            return data_tensor, param_tensor

    return torch, Dataset, DataLoader, VDPDataset


@app.cell
def _(DataLoader, VDPDataset, client, mo, time):
    # Create dataset and dataloader
    dataset = VDPDataset(client, artifact_key="mh_powder_30T")
    dataloader = DataLoader(dataset, batch_size=4, shuffle=True)

    # Load first batch
    start = time.perf_counter()
    batch_data, batch_params = next(iter(dataloader))
    load_time_dl = (time.perf_counter() - start) * 1000

    mo.md(f"""
### DataLoader Demo

**Dataset size:** {len(dataset)} entities with `mh_powder_30T`
**Batch size:** 4

**First batch loaded in {load_time_dl:.1f} ms:**
- Data shape: `{tuple(batch_data.shape)}` (batch, 200 field points)
- Params shape: `{tuple(batch_params.shape)}` (batch, 4 params: Ja, Jb, Jc, Dc)

**Sample parameters (Ja, Jb, Jc, Dc):**
- Sample 0: `[{batch_params[0, 0]:.3f}, {batch_params[0, 1]:.3f}, {batch_params[0, 2]:.3f}, {batch_params[0, 3]:.3f}]`
- Sample 1: `[{batch_params[1, 0]:.3f}, {batch_params[1, 1]:.3f}, {batch_params[1, 2]:.3f}, {batch_params[1, 3]:.3f}]`
    """)
    return dataset, dataloader, batch_data, batch_params, load_time_dl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Visualize M(H) Curves

    Plot sample magnetization curves colored by physics parameters.
    """)
    return


@app.cell
def _(X_a, Theta_a, np, mo):
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(12, 4))

    # Plot 1: Sample curves colored by Ja
    ax1 = axes[0]
    n_samples = min(10, len(X_a))
    h_grid_plot = np.linspace(0, 1, X_a.shape[1])
    colors = plt.cm.coolwarm(np.linspace(0, 1, n_samples))

    for i in range(n_samples):
        ax1.plot(h_grid_plot, X_a[i], color=colors[i], alpha=0.8,
                 label=f"Ja={Theta_a[i, 0]:.2f}")
    ax1.set_xlabel("Reduced field h = H/Hmax")
    ax1.set_ylabel("Normalized magnetization m = M/(g*s)")
    ax1.set_title("M(H) Curves (colored by Ja_meV)")
    ax1.legend(fontsize=8, loc="lower right")
    ax1.grid(True, alpha=0.3)

    # Plot 2: Parameter distribution
    ax2 = axes[1]
    sc = ax2.scatter(Theta_a[:, 0], Theta_a[:, 3], c=Theta_a[:, 1], cmap="viridis", alpha=0.7)
    ax2.set_xlabel("Ja_meV")
    ax2.set_ylabel("Dc_meV")
    ax2.set_title("Parameter Space (color = Jb_meV)")
    ax2.grid(True, alpha=0.3)
    plt.colorbar(sc, ax=ax2, label="Jb_meV")

    plt.tight_layout()
    mo.md("### M(H) Visualization")
    return fig, axes, ax1, ax2, plt, n_samples, h_grid_plot, colors, sc


@app.cell
def _(fig):
    fig
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Julia vs Python Comparison

    | Aspect | Julia (direct HDF5) | Python VDP (Tiled) |
    |--------|---------------------|-------------------|
    | **Code** | ~170 lines | ~50 lines |
    | **Manifest parsing** | Manual parquet + join | Built into hierarchy |
    | **File path construction** | Manual | Metadata or adapters |
    | **Server-side filtering** | No | Yes (`client.search()`) |
    | **Remote access** | No | Yes (HTTP) |
    | **Bulk performance** | Direct HDF5 | Mode A matches Julia |
    | **Interactive use** | Manual | Mode B via adapters |

    **Julia:**
    ```julia
    X, h_grid, Theta, meta = build_mh_dataset(axis="powder", Hmax_T=30.0)
    ```

    **Python (Mode A - Expert):**
    ```python
    X, h_grid, Theta, manifest = build_mh_dataset(client, axis="powder", Hmax_T=30)
    ```

    **Python (Mode B - Visualizer):**
    ```python
    h = client["H_636ce3e4"]
    data = h["mh_powder_30T"][:]
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Summary

    **VDP gives users the CHOICE:**

    | Mode | When to Use |
    |------|-------------|
    | **Mode A (Expert)** | ML training, bulk loading, maximum speed |
    | **Mode B (Visualizer)** | Interactive exploration, remote access, slicing |

    **Same catalog, same data, two access patterns.**

    ```
    ┌─────────────────────────────────────────────────────────┐
    │                    VDP Tiled Catalog                    │
    │         (entities as hierarchical containers)             │
    └─────────────────────────────────────────────────────────┘
                  │                           │
                  ▼                           ▼
    ┌─────────────────────────┐   ┌─────────────────────────┐
    │    Mode A (Expert)      │   │  Mode B (Visualizer)    │
    │ query_manifest() → h5py │   │  h["artifact"][:]       │
    │ Maximum performance     │   │  Remote access, slicing │
    └─────────────────────────┘   └─────────────────────────┘
    ```
    """)
    return


if __name__ == "__main__":
    app.run()

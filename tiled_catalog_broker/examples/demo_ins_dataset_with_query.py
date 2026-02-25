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

__generated_with = "0.19.2"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # VDP Query-Based INS Dataset Demo

    ## The Science: Inelastic Neutron Scattering

    **INS** (Inelastic Neutron Scattering) probes the **magnetic excitation spectrum** of materials.
    When neutrons scatter off magnetic ions, they exchange energy and momentum with **magnons** (spin waves).

    The measured quantity **S(Q,ω)** reveals:
    - **Magnon dispersion**: How spin wave energy varies with momentum
    - **Energy gaps**: From single-ion anisotropy (Dc) — gap at Q=0
    - **Bandwidth**: From exchange coupling strength (J)

    This catalog contains **20,000 INS spectra** from [Sunny.jl](https://github.com/SunnySuite/Sunny.jl) simulations:

    | Artifact | Incident Energy | Resolution | Experimental Analog |
    |----------|-----------------|------------|---------------------|
    | `ins_12meV` | Ei = 12 meV | Higher | Low-energy excitations |
    | `ins_25meV` | Ei = 25 meV | Lower | Full bandwidth |

    ## This Demo

    **Query:** Strong ferromagnetic (Ja > 0.5 meV) + easy-axis anisotropy (Dc < -0.5 meV)

    For INS, this selects spectra where:
    - Strong exchange → broad magnon bandwidth
    - Easy-axis anisotropy → visible energy gap at zone center

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

    # Demo configuration constants
    MAX_SPECTRA_DEMO = 20      # Limit for Mode A/B timing comparison
    BATCH_SIZE = 2             # PyTorch DataLoader batch size
    N_SPECTRA_VISUALIZE = 4    # Number of spectra to show in plots
    INCIDENT_ENERGY_MEV = 12   # Default incident energy for demo

    return API_KEY, BATCH_SIZE, INCIDENT_ENERGY_MEV, MAX_SPECTRA_DEMO, N_SPECTRA_VISUALIZE, TILED_URL, mo, np, time


@app.cell
def _(API_KEY, TILED_URL, mo):
    # Connect to Tiled server
    from tiled.client import from_uri
    from tiled.queries import Key

    client = from_uri(TILED_URL, api_key=API_KEY)

    # Apply physics-based query: strong ferromagnetic + easy-axis anisotropy
    subset = client.search(Key("Ja_meV") > 0.5).search(Key("Dc_meV") < -0.5)

    mo.md(f"""**Connected to VDP server at `{TILED_URL}`.**

    - Full catalog: **{len(client)}** entities
    - After query (`Ja > 0.5`, `Dc < -0.5`): **{len(subset)}** entities

    ### What This Query Means for INS Spectra

    **Ja > 0.5 meV** (strong ferromagnetic):
    - Magnon bandwidth scales with exchange coupling
    - Expect well-defined spin wave excitations extending to higher energies

    **Dc < -0.5 meV** (easy-axis anisotropy):
    - Creates an energy gap at the zone center (Q=0)
    - Gap magnitude ~ |Dc| for easy-axis systems
    - Sharper features in the spectrum
    """)
    return client, subset


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Browse INS Artifacts

    Each entity container has two INS spectra:

    | Artifact | Shape | Description |
    |----------|-------|-------------|
    | `ins_12meV` | (600, 400) | Ei=12 meV — better resolution, limited energy range |
    | `ins_25meV` | (600, 400) | Ei=25 meV — full bandwidth, lower resolution |

    **Axes:**
    - **Q** (momentum transfer): 600 points, typically 0–6 Å⁻¹
    - **ω** (energy transfer): 400 points, 0–Ei range
    """)
    return


@app.cell
def _(mo, subset):
    # Get first entity container from the filtered subset
    ent_keys = list(subset.keys())[:5]
    ent_key = ent_keys[0] if ent_keys else None

    if ent_key:
        h = subset[ent_key]

        # Physics parameters
        physics_keys = ["Ja_meV", "Jb_meV", "Jc_meV", "Dc_meV", "spin_s", "g_factor"]
        physics_rows = "\n".join([
            f"| {k} | {h.metadata.get(k, 'N/A'):.4f} |"
            if isinstance(h.metadata.get(k), float) else f"| {k} | {h.metadata.get(k, 'N/A')} |"
            for k in physics_keys
        ])

        # INS artifacts
        ins_keys = [k for k in h.keys() if k.startswith("ins_")]
        ins_rows = "\n".join([
            f"| {ik} | `{h[ik].shape}` | `{h[ik].dtype}` |"
            for ik in ins_keys
        ])

        _output = mo.md(f"""
    ### Container: `{ent_key}`

    **Physics Parameters:**

    | Parameter | Value |
    |-----------|-------|
    {physics_rows}

    **INS Artifacts:**

    | Key | Shape | Dtype |
    |-----|-------|-------|
    {ins_rows}
        """)
    else:
        h = None
        ins_keys = []
        _output = mo.md("No entities found. Run `register_catalog.py` first.")

    _output
    return h, ent_key, ins_keys


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Mode A: Expert Path-Based Access

    **Best for:** ML pipelines, bulk loading of many spectra

    ```python
    # Query Tiled for metadata → get HDF5 paths → load directly
    for ent_key, h in subset.items():
        path = h.metadata["path_ins_12meV"]
        with h5py.File(path, "r") as f:
            spectrum = f["data"][:]
    ```
    """)
    return


@app.cell
def _(INCIDENT_ENERGY_MEV, MAX_SPECTRA_DEMO, mo, np, subset, time):
    import h5py
    import os
    from broker.config import get_base_dir, get_dataset_paths

    def load_ins_mode_a(tiled_client, *, Ei_meV=12, max_spectra=None):
        """Load INS spectra using Mode A (direct HDF5)."""
        base_dir = get_base_dir()
        dataset_path = get_dataset_paths()["ins_powder"]  # "/ins/broadened"
        artifact_key = f"ins_{int(Ei_meV)}meV"
        path_key = f"path_{artifact_key}"

        spectra_list = []
        params_list = []

        for i, (ent_key, h) in enumerate(tiled_client.items()):
            if max_spectra and i >= max_spectra:
                break

            path_rel = h.metadata.get(path_key)
            if not path_rel:
                continue

            path = os.path.join(base_dir, path_rel)
            with h5py.File(path, "r") as f:
                spectrum = f[dataset_path][:]

            spectra_list.append(spectrum)
            params_list.append([
                h.metadata["Ja_meV"], h.metadata["Jb_meV"],
                h.metadata["Jc_meV"], h.metadata["Dc_meV"],
            ])

        if not spectra_list:
            raise ValueError(f"No INS spectra found for Ei={Ei_meV} meV")

        return np.stack(spectra_list), np.array(params_list, dtype=np.float32)

    _t0 = time.perf_counter()
    spectra_a, params_a = load_ins_mode_a(subset, Ei_meV=INCIDENT_ENERGY_MEV, max_spectra=MAX_SPECTRA_DEMO)
    time_a = (time.perf_counter() - _t0) * 1000

    mo.md(f"""
    ### Mode A Results

    | Metric | Value |
    |--------|-------|
    | **Total time** | **{time_a:.1f} ms** |
    | Spectra loaded | {len(spectra_a)} |
    | Shape | `{spectra_a.shape}` |
    """)
    return load_ins_mode_a, params_a, spectra_a, time_a


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Mode B: Tiled Adapter Access

    **Best for:** Interactive exploration, partial reads, remote access

    ```python
    h = subset["H_636ce3e4"]

    # Full spectrum
    spectrum = h["ins_12meV"][:]  # (600, 400)

    # Partial read (slicing)
    roi = h["ins_12meV"][100:300, 50:200]  # Region of interest
    ```
    """)
    return


@app.cell
def _(INCIDENT_ENERGY_MEV, MAX_SPECTRA_DEMO, mo, np, subset, time):
    def load_ins_mode_b(tiled_client, *, Ei_meV=12, max_spectra=None):
        """Load INS spectra using Mode B (Tiled adapters)."""
        artifact_key = f"ins_{int(Ei_meV)}meV"

        spectra_list = []
        params_list = []

        for i, (ent_key, h) in enumerate(tiled_client.items()):
            if max_spectra and i >= max_spectra:
                break

            if artifact_key not in h.keys():
                continue

            # Load via Tiled adapter (HTTP)
            spectrum = h[artifact_key][:]

            spectra_list.append(spectrum)
            params_list.append([
                h.metadata["Ja_meV"], h.metadata["Jb_meV"],
                h.metadata["Jc_meV"], h.metadata["Dc_meV"],
            ])

        if not spectra_list:
            raise ValueError(f"No INS spectra found for Ei={Ei_meV} meV")

        return np.stack(spectra_list), np.array(params_list, dtype=np.float32)

    _t0 = time.perf_counter()
    spectra_b, params_b = load_ins_mode_b(subset, Ei_meV=INCIDENT_ENERGY_MEV, max_spectra=MAX_SPECTRA_DEMO)
    time_b = (time.perf_counter() - _t0) * 1000

    mo.md(f"""
    ### Mode B Results

    | Metric | Value |
    |--------|-------|
    | **Total time** | **{time_b:.1f} ms** |
    | Spectra loaded | {len(spectra_b)} |
    | Shape | `{spectra_b.shape}` |
    """)
    return load_ins_mode_b, params_b, spectra_b, time_b


@app.cell(hide_code=True)
def _(mo, time_a, time_b):
    ratio = time_b / time_a if time_a > 0 else 0

    mo.md(f"""
    ## Performance Comparison

    | Mode | Time | Use Case |
    |------|------|----------|
    | **Mode A (Expert)** | {time_a:.1f} ms | ML training, bulk loading |
    | **Mode B (Visualizer)** | {time_b:.1f} ms | Interactive exploration |

    **Ratio:** Mode B / Mode A = {ratio:.1f}x
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Visualize INS Spectra

    ### What to Look For

    **S(Q,ω) heatmaps:**
    - **Bright bands**: Magnon dispersion (spin wave excitations)
    - **Gap at low Q**: Energy gap from anisotropy (Dc < 0)
    - **Bandwidth**: Width of dispersion relates to exchange strength (J)

    **Comparing spectra:**
    - Different Hamiltonian parameters → different dispersion shapes
    - Strong J → wider bandwidth
    - Large |Dc| → larger gap
    """)
    return


@app.cell
def _(INCIDENT_ENERGY_MEV, N_SPECTRA_VISUALIZE, mo, np, params_a, spectra_a):
    import matplotlib.pyplot as plt

    # Select spectra to visualize
    n_show = min(N_SPECTRA_VISUALIZE, len(spectra_a))

    fig, axes = plt.subplots(1, n_show, figsize=(4 * n_show, 4))
    if n_show == 1:
        axes = [axes]

    for i in range(n_show):
        ax = axes[i]
        # Log scale for better visualization
        spectrum = spectra_a[i]
        vmax = np.percentile(spectrum[spectrum > 0], 99) if np.any(spectrum > 0) else 1

        im = ax.imshow(
            spectrum.T,
            aspect="auto",
            origin="lower",
            cmap="viridis",
            vmin=0,
            vmax=vmax,
        )
        ax.set_xlabel("Q index")
        ax.set_ylabel("ω index")
        ax.set_title(f"Ja={params_a[i, 0]:.2f}, Dc={params_a[i, 3]:.2f}")
        plt.colorbar(im, ax=ax, label="S(Q,ω)")

    plt.tight_layout()
    mo.md(f"### INS Spectra (Ei = {INCIDENT_ENERGY_MEV} meV)")
    return axes, fig, n_show


@app.cell
def _(fig):
    fig
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## PyTorch Dataset for INS

    ### Training the Inverse Solver with INS Data

    INS spectra provide complementary information to M(H) curves for the inverse problem:

    | Modality | Input Shape | Information Content |
    |----------|-------------|---------------------|
    | M(H) curves | (200,) | Bulk magnetic response |
    | INS spectra | (600, 400) | Excitation spectrum, dispersion |

    **Multimodal approach:** Combine both for better parameter inference.
    """)
    return


@app.cell
def _(np):
    import torch
    from torch.utils.data import Dataset, DataLoader

    class INSDataset(Dataset):
        """PyTorch Dataset for INS spectra from VDP catalog."""

        def __init__(self, tiled_client, Ei_meV=12):
            self.client = tiled_client
            self.artifact_key = f"ins_{int(Ei_meV)}meV"
            # Cache keys that have the requested artifact
            self.ent_keys = [
                k for k, h in tiled_client.items()
                if self.artifact_key in h.keys()
            ]

        def __len__(self):
            return len(self.ent_keys)

        def __getitem__(self, idx):
            ent_key = self.ent_keys[idx]
            h = self.client[ent_key]

            # Load via Tiled adapter
            spectrum = h[self.artifact_key][:]

            # Physics params from container metadata
            params = h.metadata
            param_tensor = torch.tensor([
                params.get("Ja_meV", 0.0) or 0.0,
                params.get("Jb_meV", 0.0) or 0.0,
                params.get("Jc_meV", 0.0) or 0.0,
                params.get("Dc_meV", 0.0) or 0.0,
            ], dtype=torch.float32)

            # Spectrum as 2D tensor (can add channel dim for CNN)
            spectrum_tensor = torch.from_numpy(spectrum.astype(np.float32))
            return spectrum_tensor, param_tensor
    return DataLoader, INSDataset


@app.cell
def _(BATCH_SIZE, DataLoader, INCIDENT_ENERGY_MEV, INSDataset, mo, subset, time):
    # Create dataset and dataloader
    ins_dataset = INSDataset(subset, Ei_meV=INCIDENT_ENERGY_MEV)
    ins_dataloader = DataLoader(ins_dataset, batch_size=BATCH_SIZE, shuffle=True)

    # Load first batch
    start = time.perf_counter()
    batch_spectra, batch_params = next(iter(ins_dataloader))
    load_time_dl = (time.perf_counter() - start) * 1000

    mo.md(f"""
    ### DataLoader Demo

    **Dataset size:** {len(ins_dataset)} entities with `ins_12meV`
    **Batch size:** 2

    **First batch loaded in {load_time_dl:.1f} ms:**
    - Spectra shape: `{tuple(batch_spectra.shape)}` (batch, Q, ω)
    - Params shape: `{tuple(batch_params.shape)}` (batch, 4 params)

    **Sample parameters (Ja, Jb, Jc, Dc):**
    - Sample 0: `[{batch_params[0, 0]:.3f}, {batch_params[0, 1]:.3f}, {batch_params[0, 2]:.3f}, {batch_params[0, 3]:.3f}]`
    """)
    return batch_params, batch_spectra, ins_dataloader, ins_dataset, load_time_dl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Summary

    ### Multimodal Data Platform

    The VDP catalog provides **two experimental modalities** through the same query interface:

    | Modality | Artifacts | Shape | Use Case |
    |----------|-----------|-------|----------|
    | **M(H) curves** | 80,000 | (200,) | Bulk magnetic properties |
    | **INS spectra** | 20,000 | (600, 400) | Excitation spectrum |

    ### Same Query, Different Data

    ```python
    # Physics query (same for both modalities)
    subset = client.search(Key("Ja_meV") > 0.5).search(Key("Dc_meV") < -0.5)

    # Access M(H) curves
    mh_curve = subset["H_636ce3e4"]["mh_powder_30T"][:]  # (200,)

    # Access INS spectra
    ins_spectrum = subset["H_636ce3e4"]["ins_12meV"][:]  # (600, 400)
    ```

    ### Toward Multimodal Inverse Solvers

    Combining M(H) and INS data can improve parameter inference:
    - M(H) constrains bulk susceptibility and saturation
    - INS constrains exchange bandwidth and anisotropy gap
    - Together: more unique determination of (Ja, Jb, Jc, Dc)

    For full physics context, see `docs/CONTEXT-SCIENCE.md`.
    """)
    return


if __name__ == "__main__":
    app.run()

import marimo

__generated_with = "0.10.19"
app = marimo.App(width="medium")


@app.cell
def cell_connect():
    """Connect to Tiled and show catalog overview."""
    import marimo as mo
    from tiled.client import from_uri

    client = from_uri("http://localhost:8006", api_key="secret")

    total = len(client)
    mo.md(
        f"""
        # Config-Driven Ingest Demo

        Connected to Tiled at `http://localhost:8006`

        **Total entity containers:** {total}
        """
    )
    return client, mo


@app.cell
def cell_overview(client, mo):
    """Catalog overview - group keys by dataset prefix."""
    keys = list(client)

    vdp_keys = [k for k in keys if k.startswith("H_") and not k.startswith("H_edx") and not k.startswith("H_mm_")]
    edrixs_keys = [k for k in keys if k.startswith("H_edx")]
    mm_keys = [k for k in keys if k.startswith("H_mm_")]

    lines = []
    for label, ks in [("VDP", vdp_keys), ("EDRIXS", edrixs_keys), ("Multimodal", mm_keys)]:
        if ks:
            children_example = list(client[ks[0]])
            lines.append(f"- **{label}**: {len(ks)} entities, children: `{children_example}`")
        else:
            lines.append(f"- **{label}**: *not ingested yet*")

    mo.md(
        "## Catalog Overview\n\n" + "\n".join(lines)
    )
    return vdp_keys, edrixs_keys, mm_keys


@app.cell
def cell_vdp(client, vdp_keys, mo):
    """VDP retrieval - Mode A (locator->h5py) vs Mode B (Tiled adapter)."""
    if not vdp_keys:
        mo.md("## VDP\n\n*Not ingested yet.*")
        return

    import os
    import numpy as np
    import h5py

    ent_key = vdp_keys[0]
    h = client[ent_key]

    # Mode B: read via Tiled adapter
    art_key = "mh_powder_30T"
    mode_b = h[art_key].read()

    # Mode A: read via locator -> h5py
    meta = dict(h.metadata)
    file_path = meta[f"path_{art_key}"]
    dataset_path = meta[f"dataset_{art_key}"]
    base_dir = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/vdp/data/schema_v1"
    full_path = os.path.join(base_dir, file_path)

    with h5py.File(full_path, "r") as f:
        mode_a = f[dataset_path][:]

    match = np.allclose(mode_a, mode_b)

    mo.md(
        f"""
        ## VDP Retrieval

        Entity: `{ent_key}`
        Children: `{list(h)}`

        **`{art_key}`** shape: `{mode_b.shape}`

        | Mode | Source | Match |
        |------|--------|-------|
        | A (h5py) | `{file_path}:{dataset_path}` | - |
        | B (Tiled) | HTTP adapter | - |
        | **allclose** | - | **{match}** |
        """
    )
    return


@app.cell
def cell_edrixs(client, edrixs_keys, mo):
    """EDRIXS retrieval - Mode A (h5py with index) for batched spectra."""
    if not edrixs_keys:
        mo.md("## EDRIXS\n\n*Not ingested yet.*")
        return

    import os
    import h5py

    ent_key = edrixs_keys[0]
    h = client[ent_key]
    meta = dict(h.metadata)

    # Mode A: batched spectra require index-based access via h5py
    file_path = meta["path_rixs"]
    dataset_path = meta["dataset_rixs"]
    index = int(meta["index_rixs"])
    base_dir = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/tlinker/data/EDRIXS"
    full_path = os.path.join(base_dir, file_path)

    with h5py.File(full_path, "r") as f:
        spectrum = f[dataset_path][index]

    param_keys = [k for k in meta if not k.startswith(("path_", "dataset_", "index_", "uid"))]

    mo.md(
        f"""
        ## EDRIXS Retrieval (Mode A)

        Entity: `{ent_key}`
        Children: `{list(h)}`

        **`rixs`** shape: `{spectrum.shape}`, index: `{index}`
        (Batched spectra: Mode A reads `{dataset_path}[{index}]` from `{file_path}`)

        Parameters: `{param_keys}`

        *Mode B is also available: the registration code translates the manifest
        `index` to Tiled's built-in `slice` parameter, so `client[key]["rixs"][:]`
        returns the correct per-entity spectrum.*
        """
    )
    return


@app.cell
def cell_multimodal(client, mm_keys, mo):
    """Multimodal retrieval - verify 6 children, read powder + magnetization."""
    if not mm_keys:
        mo.md("## Multimodal\n\n*Not ingested yet.*")
        return

    ent_key = mm_keys[0]
    h = client[ent_key]
    children = list(h)

    shapes = {}
    for child_key in children:
        arr = h[child_key].read()
        shapes[child_key] = arr.shape

    meta = dict(h.metadata)
    param_keys = [k for k in meta if not k.startswith(("path_", "dataset_", "index_", "uid"))]

    mo.md(
        f"""
        ## Multimodal Retrieval

        Entity: `{ent_key}`
        Children ({len(children)}): `{children}`

        | Artifact | Shape |
        |----------|-------|
        """ + "\n".join(f"| `{k}` | `{v}` |" for k, v in shapes.items()) + f"""

        Parameters: `{param_keys}`
        """
    )
    return


@app.cell
def cell_cross_query(client, vdp_keys, edrixs_keys, mm_keys, mo):
    """Cross-dataset queries - demonstrate dataset-specific metadata keys."""
    from tiled.queries import Key

    lines = []

    if vdp_keys:
        vdp_results = client.search(Key("Ja_meV") >= 0)
        lines.append(f'| `Key("Ja_meV") >= 0` | {len(vdp_results)} | VDP |')
    else:
        lines.append("| `Key(\"Ja_meV\") >= 0` | *n/a* | VDP (not ingested) |")

    if edrixs_keys:
        edrixs_results = client.search(Key("F2_dd") >= 0)
        lines.append(f'| `Key("F2_dd") >= 0` | {len(edrixs_results)} | EDRIXS |')
    else:
        lines.append("| `Key(\"F2_dd\") >= 0` | *n/a* | EDRIXS (not ingested) |")

    if mm_keys:
        mm_results = client.search(Key("J1a") >= -999)
        lines.append(f'| `Key("J1a") >= -999` | {len(mm_results)} | Multimodal |')
    else:
        lines.append("| `Key(\"J1a\") >= -999` | *n/a* | Multimodal (not ingested) |")

    mo.md(
        f"""
        ## Cross-Dataset Queries

        Each dataset has its own parameter names. Searching by a dataset-specific
        key naturally returns only results from that dataset:

        | Query | Matches | Expected Dataset |
        |-------|---------|-----------------|
        """ + "\n".join(lines)
    )
    return


@app.cell
def cell_plots(client, vdp_keys, edrixs_keys, mm_keys, mo):
    """Visualization - side-by-side plots for available datasets."""
    import matplotlib.pyplot as plt
    import numpy as np

    present = []
    if vdp_keys:
        present.append("vdp")
    if edrixs_keys:
        present.append("edrixs")
    if mm_keys:
        present.append("mm")

    if not present:
        mo.md("## Visualization\n\n*No datasets ingested yet.*")
        return

    fig, axes = plt.subplots(1, len(present), figsize=(5 * len(present), 4))
    if len(present) == 1:
        axes = [axes]

    idx = 0

    if "vdp" in present:
        ax = axes[idx]
        h = client[vdp_keys[0]]
        mh = h["mh_powder_30T"].read()
        ax.plot(np.linspace(0, 30, len(mh)), mh)
        ax.set_xlabel("H (T)")
        ax.set_ylabel("M")
        ax.set_title(f"VDP: {vdp_keys[0]}")
        idx += 1

    if "edrixs" in present:
        ax = axes[idx]
        import h5py, os
        h = client[edrixs_keys[0]]
        meta = dict(h.metadata)
        edrixs_path = os.path.join(
            "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/tlinker/data/EDRIXS",
            meta["path_rixs"],
        )
        with h5py.File(edrixs_path, "r") as f:
            spectrum = f[meta["dataset_rixs"]][int(meta["index_rixs"])]
        ax.imshow(spectrum.T, aspect="auto", origin="lower", cmap="viridis")
        ax.set_xlabel("Incident energy")
        ax.set_ylabel("Energy loss")
        ax.set_title(f"EDRIXS: {edrixs_keys[0]}")
        idx += 1

    if "mm" in present:
        ax = axes[idx]
        h = client[mm_keys[0]]
        powder = h["ins_powder"].read()
        ax.imshow(powder.T, aspect="auto", origin="lower", cmap="inferno")
        ax.set_xlabel("|Q|")
        ax.set_ylabel("Energy")
        ax.set_title(f"Multimodal: {mm_keys[0]}")
        idx += 1

    plt.tight_layout()
    mo.md("## Visualization")
    return


if __name__ == "__main__":
    app.run()

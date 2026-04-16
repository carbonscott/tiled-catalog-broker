import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def cell_connect():
    """Connect to Tiled and show catalog overview."""
    import marimo as mo
    from tiled.client import from_uri
    from data_catalog_service.config import get_tiled_url, get_api_key

    _tiled_url = get_tiled_url()
    client = from_uri(_tiled_url, api_key=get_api_key())

    mo.md(
        f"""
        # Config-Driven Ingest Demo

        Connected to Tiled at `{_tiled_url}`

        **Total root containers:** {len(client)}
        """
    )
    return client, mo


@app.cell
def cell_overview(client, mo):
    """Catalog overview - classify root keys by prefix."""
    _all_keys = list(client)

    vdp_keys = [k for k in _all_keys if k.startswith("H_")]
    _other_keys = [k for k in _all_keys if not k.startswith("H_")]

    _lines = []
    if vdp_keys:
        _sample = client[vdp_keys[0]]
        _children = list(_sample)
        _lines.append(
            f"- **VDP**: {len(vdp_keys)} entities, "
            f"children: `{_children}`"
        )
    else:
        _lines.append("- **VDP**: *not ingested yet*")

    if _other_keys:
        _lines.append(f"- **Other**: {len(_other_keys)} items (`{_other_keys}`)")

    mo.md("## Catalog Overview\n\n" + "\n".join(_lines))
    return (vdp_keys,)


@app.cell
def cell_vdp(client, mo, vdp_keys):
    """VDP retrieval - Mode A (metadata locator -> h5py)."""
    import os as _os
    import h5py as _h5py

    mo.stop(not vdp_keys, mo.md("## VDP\n\n*Not ingested yet.*"))

    _ent_key = vdp_keys[0]
    _h = client[_ent_key]
    _meta = dict(_h.metadata)

    # Mode A: use locator metadata to read via h5py
    _art_key = "mh_powder_30T"
    _file_path = _meta[f"path_{_art_key}"]
    _base_dir = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/vdp/data/schema_v1"
    _full_path = _os.path.join(_base_dir, _file_path)

    with _h5py.File(_full_path, "r") as _f:
        _data = _f["curve/M_parallel"][:]

    _param_keys = [k for k in _meta if not k.startswith(("path_", "dataset_", "index_", "uid", "huid"))]

    mo.md(
        f"""
        ## VDP Retrieval (Mode A)

        Entity: `client["{_ent_key}"]`
        Children: `{list(_h)}`

        **`{_art_key}`** shape: `{_data.shape}`, loaded via h5py
        Source: `{_file_path}:curve/M_parallel`

        Physics parameters: `{_param_keys}`
        """
    )
    return


@app.cell
def cell_query(client, mo, vdp_keys):
    """Query VDP entities by physics parameters."""
    from tiled.queries import Key as _Key

    mo.stop(not vdp_keys, mo.md("## Queries\n\n*No VDP entities to query.*"))

    _lines = []

    _results_ja = client.search(_Key("Ja_meV") >= 0)
    _lines.append(f'| `Key("Ja_meV") >= 0` | {len(_results_ja)} |')

    _results_spin = client.search(_Key("spin_s") == 0.5)
    _lines.append(f'| `Key("spin_s") == 0.5` | {len(_results_spin)} |')

    mo.md(
        f"""
        ## VDP Queries

        | Query | Matches |
        |-------|---------|
        """ + "\n".join(_lines)
    )
    return


@app.cell
def cell_plots(client, mo, vdp_keys):
    """Visualization - INS spectrum and magnetization side-by-side."""
    import os as _os
    import h5py as _h5py
    import matplotlib.pyplot as _plt
    import numpy as _np

    mo.stop(not vdp_keys, mo.md("## Visualization\n\n*No VDP entities to plot.*"))

    _ent_key = vdp_keys[0]
    _meta = dict(client[_ent_key].metadata)
    _base_dir = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/vdp/data/schema_v1"

    # Load INS 12meV spectrum
    _ins_path = _os.path.join(_base_dir, _meta["path_ins_12meV"])
    with _h5py.File(_ins_path, "r") as _f:
        _ins = _f["ins/broadened"][:]
        _q = _f["ins/q_Ainv"][:]
        _hw = _f["ins/hw_meV"][:]

    # Load magnetization
    _mh_path = _os.path.join(_base_dir, _meta["path_mh_powder_30T"])
    with _h5py.File(_mh_path, "r") as _f:
        _mh = _f["curve/M_parallel"][:]
        _field = _f["curve/H_T"][:]

    _fig, (_ax1, _ax2) = _plt.subplots(1, 2, figsize=(12, 4))

    # INS heatmap
    _ax1.pcolormesh(_hw, _q, _ins, cmap="inferno", shading="auto")
    _ax1.set_xlabel("Energy (meV)")
    _ax1.set_ylabel("|Q| (1/A)")
    _ax1.set_title(f"INS 12meV: {_ent_key}")

    # Magnetization curve
    _ax2.plot(_field, _mh)
    _ax2.set_xlabel("H (T)")
    _ax2.set_ylabel("M")
    _ax2.set_title(f"M(H) powder 30T: {_ent_key}")

    _plt.tight_layout()

    mo.vstack([mo.md("## Visualization"), _fig])
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

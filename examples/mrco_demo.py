import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def cell_connect():
    """Connect to Tiled and access the MRCO dataset."""
    import marimo as mo
    from tiled.client import from_uri
    from data_catalog_service.config import get_tiled_url, get_api_key

    _tiled_url = get_tiled_url()
    client = from_uri(_tiled_url, api_key=get_api_key())
    mrco = client["TMO101633026_MRCO"]
    _meta = dict(mrco.metadata)

    def _fmt(v):
        if isinstance(v, list):
            return ", ".join(str(x) for x in v)
        return str(v)

    # Curated metadata display in logical groups
    _experiment_fields = [
        ("Experiment", "experiment"),
        ("PI", "PI"),
        ("Facility", "facility"),
        ("Endstation", "endstation"),
        ("Instrument", "instrument"),
        ("Material", "material"),
        ("Method", "method"),
        ("Data type", "data_type"),
        ("Beamtime", None),  # special
    ]

    _prov_fields = [
        ("Producer", "producer"),
        ("Project", "project"),
        ("Repo", "repo"),
        ("Branch", "branch"),
        ("Commit", "commit"),
        ("Created", "created_at"),
    ]

    _exp_rows = []
    for _label, _key in _experiment_fields:
        if _key is None:
            _start = _meta.get("beamtime_start", "")
            _end = _meta.get("beamtime_end", "")
            _exp_rows.append(f"| {_label} | {_start} to {_end} |")
        elif _key in _meta:
            _exp_rows.append(f"| {_label} | {_fmt(_meta[_key])} |")

    _prov_rows = []
    for _label, _key in _prov_fields:
        if _key in _meta:
            _prov_rows.append(f"| {_label} | {_fmt(_meta[_key])} |")

    _exp_table = "\n".join(_exp_rows)
    _prov_table = "\n".join(_prov_rows)

    mo.md(f"""# MSC Commissioning — Data Catalog Demo

Connected to `{_tiled_url}`

**Dataset:** `TMO101633026_MRCO` — **{len(mrco)} runs** registered

### Experiment

| Field | Value |
|-------|-------|
{_exp_table}

### Provenance

| Field | Value |
|-------|-------|
{_prov_table}""")
    return client, mo, mrco


@app.cell
def cell_discover_parameters(mo, mrco):
    """Discover what parameters and values are available in the catalog."""
    import pandas as _pd

    _all_meta = []
    for _k in list(mrco):
        _m = dict(mrco[_k].metadata)
        _all_meta.append(_m)

    meta_df = _pd.DataFrame(_all_meta)

    # Discover categorical parameters and their unique values
    _interesting_cols = [
        ("sample", "Sample gas/target"),
        ("phase", "Experimental phase"),
        ("purpose", "Run purpose"),
        ("scan_variable", "Scan motor"),
        ("has_waveforms", "Raw waveforms stored"),
        ("has_roi", "ROI integrals stored"),
        ("has_fzp", "FZP data stored"),
        ("has_atm", "ATM data stored"),
    ]
    _discovery_rows = []
    for _col, _label in _interesting_cols:
        if _col in meta_df.columns:
            _unique = sorted(meta_df[_col].dropna().unique(), key=str)
            _formatted = ", ".join(f"`{v}`" for v in _unique)
            _discovery_rows.append(f"| {_label} | `{_col}` | {len(_unique)} | {_formatted} |")

    _disc_table = "\n".join(_discovery_rows)

    mo.md(f"""## 1. Cross-Run Discovery

Without a catalog, finding "all N2 runs at 400 eV with pump-probe timing"
means manually checking each HDF5 file or remembering run numbers.
With the catalog, every run's metadata is queryable.

### Discoverable Parameters

| Description | Query key | Unique | Values |
|-------------|-----------|--------|--------|
{_disc_table}""")
    return (meta_df,)


@app.cell
def cell_interactive_query(meta_df, mo):
    """Interactive cross-run query builder."""
    from tiled.queries import Key

    _samples = ["Any"] + sorted(meta_df["sample"].dropna().unique(), key=str)
    _phases = ["Any"] + sorted(meta_df["phase"].dropna().unique(), key=str)
    _scan_vars = ["Any"] + sorted(
        [str(s) for s in meta_df["scan_variable"].dropna().unique() if s], key=str
    )

    sample_dd = mo.ui.dropdown(options=_samples, value="Any", label="Sample")
    phase_dd = mo.ui.dropdown(options=_phases, value="Any", label="Phase")
    scan_dd = mo.ui.dropdown(options=_scan_vars, value="Any", label="Scan variable")
    wfm_cb = mo.ui.checkbox(label="Has waveforms only")

    mo.md(
        f"""
        ### Query Builder

        {mo.hstack([sample_dd, phase_dd, scan_dd, wfm_cb], justify="start", gap=1)}
        """
    )
    return Key, phase_dd, sample_dd, scan_dd, wfm_cb


@app.cell
def cell_query_results(Key, mo, mrco, phase_dd, sample_dd, scan_dd, wfm_cb):
    """Execute the query and show results."""
    import pandas as _pd

    _results = mrco
    _query_parts = []

    if sample_dd.value != "Any":
        _results = _results.search(Key("sample") == sample_dd.value)
        _query_parts.append(f'Key("sample") == "{sample_dd.value}"')

    if phase_dd.value != "Any":
        _results = _results.search(Key("phase") == phase_dd.value)
        _query_parts.append(f'Key("phase") == "{phase_dd.value}"')

    if scan_dd.value != "Any":
        _results = _results.search(Key("scan_variable") == scan_dd.value)
        _query_parts.append(f'Key("scan_variable") == "{scan_dd.value}"')

    if wfm_cb.value:
        _results = _results.search(Key("has_waveforms") == True)
        _query_parts.append('Key("has_waveforms") == True')

    _query_str = ", ".join(_query_parts) if _query_parts else "(no filter)"

    _rows = []
    for _k in list(_results):
        _m = dict(_results[_k].metadata)
        _rows.append({
            "key": _k,
            "run": int(_m.get("run_number", 0)),
            "sample": _m.get("sample"),
            "phase": _m.get("phase"),
            "purpose": _m.get("purpose"),
            "photon_energy_eV": _m.get("photon_energy_eV"),
            "scan_variable": _m.get("scan_variable", ""),
            "n_shots": int(_m.get("n_shots", 0)),
        })

    query_df = _pd.DataFrame(_rows).sort_values("run") if _rows else _pd.DataFrame()

    run_table = mo.ui.table(query_df, selection="single", label="Select a run")

    mo.md(
        f"""
        **Query:** `mrco.search({_query_str})`
        **Result:** {len(query_df)} runs
        """
    )
    return (run_table,)


@app.cell
def cell_query_table(run_table):
    """Display the query results table."""
    run_table
    return


@app.cell
def cell_provenance(mo, mrco, run_table):
    """Show provenance and config for selected run."""
    mo.stop(len(run_table.value) == 0, mo.md(
        """
        ## 2. Reproducibility and Provenance

        *Select a run above to see its full provenance and processing config.*

        The catalog captures metadata that otherwise lives only in the eLog
        and people's heads: which runs were ABORTED/FAILED, what detectors
        were active, what processing config was used. All queryable, all persistent.
        """
    ))

    _selected = run_table.value.iloc[0]
    _ent = mrco[_selected["key"]]
    _meta = dict(_ent.metadata)
    _children = list(_ent)

    # Provenance
    _prov_rows = []
    for _k in ["h5_producer_repo", "h5_producer_script", "h5_producer_commit", "h5_producer_path"]:
        _v = _meta.get(_k, "")
        _prov_rows.append(f"| `{_k}` | `{_v}` |")

    # Config
    _cfg_rows = []
    for _k, _v in sorted(_meta.items()):
        if _k.startswith("cfg_"):
            _cfg_rows.append(f"| `{_k}` | `{_v}` |")

    # Notes (ABORTED, FAILED, etc.)
    _notes = _meta.get("notes", "")

    _prov_table = "\n".join(_prov_rows)
    _cfg_table = "\n".join(_cfg_rows)
    _run_num_display = int(_meta.get('run_number', 0))

    mo.md(f"""## 2. Reproducibility and Provenance

### Run {_run_num_display}: {_meta.get('purpose', '')}

**Notes:** {_notes if _notes else "(none)"}

**Detectors:** `{_meta.get('det_rollcall', '')}`

**Artifacts registered:** {len(_children)}

### Data Product Provenance

| Field | Value |
|-------|-------|
{_prov_table}

### Processing Configuration

| Parameter | Value |
|-----------|-------|
{_cfg_table}""")
    return


@app.cell
def cell_config_comparison(meta_df, mo):
    """Detect configuration changes across runs."""
    import pandas as _pd

    _cfg_cols = [c for c in meta_df.columns if c.startswith("cfg_")]
    _cfg_df = meta_df[["run_number", "sample"] + _cfg_cols].sort_values("run_number")

    # Find where config changed between consecutive runs
    _changes = []
    _prev = None
    for _, _row in _cfg_df.iterrows():
        if _prev is not None:
            for _col in _cfg_cols:
                _old = str(_prev[_col])
                _new = str(_row[_col])
                if _old != _new:
                    _run = _row["run_number"]
                    _changes.append({
                        "run": int(_run) if not _pd.isna(_run) else "?",
                        "parameter": _col,
                        "previous": _old,
                        "new": _new,
                    })
        _prev = _row

    change_df = _pd.DataFrame(_changes)

    _summary = mo.md(
        f"""
        ## 3. Configuration Comparison

        Comparing processing config across all {len(_cfg_df)} runs to find
        where settings changed. This surfaces configuration drift that would
        otherwise require manually diffing opaque config strings in each HDF5 file.

        **{len(_changes)} changes detected across {len(_cfg_cols)} config parameters**
        """
    )

    if len(change_df) > 0:
        _change_table = mo.ui.table(change_df, label="Configuration transitions")
        mo.vstack([_summary, _change_table])
    else:
        mo.vstack([_summary, mo.md("No configuration changes detected.")])
    return


@app.cell
def cell_data_access(mo, mrco, run_table):
    """Load and plot data via Mode A (catalog metadata -> h5py)."""
    import os as _os
    import numpy as _np
    import h5py as _h5py
    import matplotlib.pyplot as _plt

    mo.stop(len(run_table.value) == 0, mo.md(
        """
        ## 4. Data Access (Mode A)

        *Select a run above to load and plot data.*

        The catalog stores locator metadata (file path, HDF5 dataset path) for
        each artifact. Mode A queries this metadata then loads directly via h5py
        — the catalog tells you *where* the data is, you load it yourself.
        """
    ))

    _selected = run_table.value.iloc[0]
    _ent = mrco[_selected["key"]]
    _meta = dict(_ent.metadata)

    _run_num = int(_meta.get("run_number", 0))

    # Resolve base_dir from dataset metadata
    _ds_meta = dict(mrco.metadata)
    _experiment = _ds_meta.get("experiment", "")
    _base_dir = f"/sdf/scratch/lcls/ds/tmo/{_experiment}/scratch/preproc/mrco"

    # Get file path from entity locator metadata
    _path_keys = [k for k in _meta if k.startswith("path_hsd_roi_")]
    if _path_keys:
        _rel_file = _meta[_path_keys[0]]
        _h5_path = _os.path.join(_base_dir, _rel_file)
    else:
        _h5_path = _os.path.join(_base_dir, f"run{_run_num}_mrco.h5")

    mo.stop(not _os.path.exists(_h5_path), mo.md(
        f"## 4. Data Access\n\nFile not found: `{_h5_path}`"
    ))

    # Load ROI data and scan variable via h5py
    _roi_data = {}
    _scan_var = None
    _scan_name = str(_meta.get("scan_variable", ""))

    try:
        with _h5py.File(_h5_path, "r") as _f:
            for _ds_name in sorted(_f.keys()):
                if _ds_name.startswith("hsd_roi_"):
                    _roi_data[_ds_name] = _f[_ds_name][:]
            if "scan_var_0" in _f and _scan_name:
                _scan_var = _f["scan_var_0"][:]
    except OSError as _e:
        mo.stop(True, mo.md(f"## 4. Data Access\n\nCannot open `{_h5_path}`: {_e}"))

    mo.stop(not _roi_data, mo.md(
        f"## 4. Data Access\n\nRun {_run_num} has no `hsd_roi_*` datasets."
    ))

    _n_shots = len(next(iter(_roi_data.values())))

    # Plot: angular distribution + scan or shot-by-shot
    _angles = []
    _means = []
    for _rk in sorted(_roi_data.keys()):
        _angle = int(_rk.replace("hsd_roi_", ""))
        _angles.append(_angle)
        _means.append(_np.nanmean(_roi_data[_rk]))

    _fig, _axes = _plt.subplots(1, 2, figsize=(12, 4))

    _axes[0].bar(_angles, _means, width=15, alpha=0.7)
    _axes[0].set_xlabel("Channel angle (degrees)")
    _axes[0].set_ylabel("Mean integrated ROI signal")
    _axes[0].set_title(f"Run {_run_num}: Angular distribution ({_meta.get('sample')})")

    if _scan_var is not None and len(_np.unique(_scan_var)) > 1:
        _unique_scan = _np.unique(_scan_var)
        _binned = [_np.nanmean(_roi_data["hsd_roi_0"][_scan_var == _sv]) for _sv in _unique_scan]
        _axes[1].plot(_unique_scan, _binned, "o-")
        _axes[1].set_xlabel(_scan_name)
        _axes[1].set_ylabel("Mean hsd_roi_0")
        _axes[1].set_title(f"ROI vs {_scan_name}")
    else:
        _axes[1].plot(_roi_data["hsd_roi_0"], ".", markersize=0.3, alpha=0.3)
        _axes[1].set_xlabel("Shot index")
        _axes[1].set_ylabel("hsd_roi_0")
        _axes[1].set_title("Shot-by-shot signal")

    _plt.tight_layout()

    _summary = mo.md(
        f"""
        ## 4. Data Access (Mode A)

        ### Run {_run_num} — loaded via h5py using catalog locators

        Catalog metadata pointed to `{_os.path.basename(_h5_path)}`.
        {len(_roi_data)} ROI channels loaded, {_n_shots:,} shots each.

        ```python
        ent = mrco["{_selected['key']}"]
        path = ent.metadata["path_hsd_roi_0"]  # file locator from catalog
        with h5py.File(base_dir / path) as f:
            data = f["hsd_roi_0"][:]
        ```
        """
    )
    mo.vstack([_summary, _fig])
    return


@app.cell
def cell_cross_dataset(client, mo):
    """Show what other datasets exist in the catalog for cross-dataset access."""
    _all_keys = list(client)

    # Find dataset containers (non H_ keys that are containers)
    _datasets = []
    for _k in _all_keys:
        if _k.startswith("H_"):
            continue
        try:
            _node = client[_k]
            if hasattr(_node, "keys"):
                _n = len(_node)
                _meta = dict(_node.metadata)
                _datasets.append({
                    "key": _k,
                    "entities": _n,
                    "data_type": _meta.get("data_type", ""),
                    "material": str(_meta.get("material", "")),
                    "method": str(_meta.get("method", "")),
                })
        except Exception:
            pass

    if _datasets:
        import pandas as _pd
        _ds_df = _pd.DataFrame(_datasets)

        mo.md(
            f"""
            ## 5. Cross-Dataset Access for ML/SBI

            The catalog contains **{len(_datasets)} dataset containers**.
            Experimental and simulated data share the same query API:

            ```python
            # Experimental: filter by sample and photon energy
            client["TMO101633026_MRCO"].search(Key("sample") == "N2")

            # Simulated: filter by Hamiltonian parameters
            client["RIXS_SIM_BROAD_SIGMA"].search(Key("tenDq") > 2.0)
            ```

            Same interface, same access pattern — ready for ML pipelines
            that need both experimental and simulated data.
            """
        )
    else:
        mo.md(
            """
            ## 5. Cross-Dataset Access for ML/SBI

            Only MRCO data currently in this catalog. When simulation datasets
            (EDRIXS, NiPS3, etc.) are registered alongside, the same query API
            enables ML pipelines to pull both experimental and simulated data.
            """
        )
    return


@app.cell
def cell_limitations(mo):
    """Document current limitations."""
    mo.md(
        """
        ## Current Limitations

        - **Per-shot querying** — "find all shots where xgmd_energy > 0.5 mJ"
          requires loading the full array. The catalog indexes at the run level,
          not the shot level.

        - **Variable-length artifact serving** — the ragged hit-time arrays
          (`var_hsd_hf_times_*`) can be located via Mode A (h5py) but cannot
          be served as rectangular Tiled arrays via Mode B.

        - **Live/streaming data** — this is post-hoc registration of completed
          runs. Real-time registration during data collection is not yet supported.

        - **Reduced data products** — histogrammed spectra, covariance matrices
          from the outer product pipeline could be registered as additional
          artifacts on the same run entities or as a related dataset. The catalog
          provides the connective tissue to link raw and reduced products.
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

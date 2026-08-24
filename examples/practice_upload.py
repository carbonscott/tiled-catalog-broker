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
    # Practice upload

    A dry run of `tcb register --upload` against **tiled-test**, a shared server kept
    for exactly this: it has no filesystem mount, so pointer registration is not even
    possible there — only the upload transport works. There is nothing of yours to
    accidentally overwrite; the worst outcome is a small container with your surname on
    it, which you can delete at the end.

    This notebook runs the same three commands as publishing a dataset for real —
    `tcb stamp-key`, `tcb generate`, `tcb register --upload` — against synthetic data
    it generates itself, so you rehearse the upload pathway before your own files are
    involved.

    **Before you start:** `tcb --help` should print its four commands, and your `.env`
    should have `TILED_API_KEY` set (see "How to prepare for the workshop"). If
    `TILED_URL` is unset, this notebook defaults to tiled-test.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    last_name_form = mo.ui.text(
        label="Your last name", placeholder="Welborn"
    ).form(submit_button_label="Generate my practice dataset")
    last_name_form
    return (last_name_form,)


@app.cell
def _(last_name_form, mo):
    mo.stop(
        not last_name_form.value,
        mo.md("**Enter your last name above and submit** to continue."),
    )

    import secrets

    last_name = last_name_form.value.strip()
    # The surname alone is what the real workshop convention uses (see
    # workshop-prep.md); a random suffix is added here because this notebook is
    # expected to be re-run, and two identical labels would land in one container.
    suffix = secrets.token_hex(2)
    label = f"{last_name} Practice Upload {suffix}"
    mo.md(f"Dataset label: **{label}**")
    return label, last_name, secrets, suffix


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## 1. Generate synthetic HDF5 files

    Same generator as `examples/make_demo_dataset.py`, inlined so this notebook has
    no import-path dependency on where it is launched from. Produces the `per_entity`
    layout: one file per entity, two artifact arrays, one shared axis, three scalar
    parameters.
    """)
    return


@app.cell
def _():
    import tempfile

    return (tempfile,)


@app.cell
def _(label, mo, tempfile):
    from pathlib import Path

    import h5py
    import numpy as np

    def make_dataset(out_dir, n_entities, seed=0):
        rng = np.random.default_rng(seed)
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        energies = np.linspace(0.0, 8.0, 64)
        written = []

        for i in range(n_entities):
            sigma = float(rng.uniform(0.02, 0.08))
            gamma = float(rng.choice([0.1, 0.2, 0.4]))
            ten_dq = float(rng.uniform(1.0, 2.5))

            peak = np.exp(-((energies - ten_dq) ** 2) / (2 * sigma**2 + 0.05))
            angles = np.linspace(0, np.pi, 16)
            spectrum = np.outer(peak, np.cos(angles) ** 2 + gamma).astype(np.float32)
            curve = (peak * (1.0 - gamma)).astype(np.float32)

            path = out_dir / f"entity_{i + 1:04d}.h5"
            with h5py.File(path, "w") as f:
                f["spectrum"] = spectrum
                f["curve"] = curve
                f["energies"] = energies
                f["sigma"] = sigma
                f["gamma"] = gamma
                f["tenDq"] = ten_dq
            written.append(path)

        return written

    work_dir = Path(tempfile.mkdtemp(prefix="tcb_practice_"))
    data_dir = work_dir / "data"
    written = make_dataset(data_dir, n_entities=6)

    mo.md(
        f"Wrote **{len(written)}** files to `{data_dir}` "
        f"(e.g. `{written[0].name}`: `/spectrum` (64, 16), `/curve` (64,), "
        "params `sigma`, `gamma`, `tenDq`)"
    )
    return Path, data_dir, h5py, make_dataset, np, work_dir, written


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## 2. Write the dataset YAML""")
    return


@app.cell
def _(data_dir, label, mo, work_dir):
    yaml_path = work_dir / "practice.yml"
    yaml_path.write_text(f"""\
label: {label}

metadata:
  method: [Synthetic]
  data_type: benchmark
  material: generic_spin_model
  producer: tcb-practice-upload

data:
  directory: {data_dir}
  file_pattern: "*.h5"
  layout: per_entity

parameters:
  location: root_scalars

artifacts:
  - {{ type: spectrum, dataset: /spectrum }}
  - {{ type: curve,    dataset: /curve }}

shared:
  - {{ type: energies, dataset: /energies }}
""")

    mo.md(f"""Wrote `{yaml_path}`:

```yaml
{yaml_path.read_text()}```

`method: [Synthetic]` is not in the catalog's controlled vocabulary — expect a soft
warning from `tcb generate` below. That is by design for fake data; soft-vocabulary
warnings never block generation or registration.
`data.directory` is a path on **this machine**, which is what the upload transport
reads from; there is no `server_base_dir` to set because the server never opens it.
""")
    return (yaml_path,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## 3. Resolve the server

    Defaults to tiled-test if `TILED_URL` is not set in the shell that launched this
    notebook.
    """)
    return


@app.cell
def _(mo):
    import os

    tiled_url = os.environ.get(
        "TILED_URL", "https://lcls-data-portal.slac.stanford.edu/tiled-test"
    )
    api_key = os.environ.get("TILED_API_KEY") or os.environ.get("TILED_KEY")

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

    mo.md(f"Target server: **{tiled_url}**")
    return api_key, os, tiled_url


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## 4. Run the real pipeline

    The same three commands as publishing a dataset for real, run as subprocesses
    against the YAML above.
    """)
    return


@app.cell
def _(mo):
    import subprocess

    def run_tcb(*args, env=None):
        try:
            result = subprocess.run(
                ["tcb", *args], capture_output=True, text=True, env=env,
            )
        except FileNotFoundError:
            mo.stop(
                True,
                mo.md(
                    "**`tcb` was not found on PATH.** Install it first, then "
                    "restart marimo."
                ),
            )
        output = (result.stdout + result.stderr).strip()
        mo.stop(
            result.returncode != 0,
            mo.md(f"**`tcb {' '.join(args)}` failed:**\n\n```\n{output}\n```"),
        )
        return output

    return run_tcb, subprocess


@app.cell
def _(mo, run_tcb, yaml_path):
    stamp_output = run_tcb("stamp-key", str(yaml_path))

    import re

    m = re.search(r"^key:\s*(\S+)", yaml_path.read_text(), re.M)
    dataset_key = m.group(1)

    mo.md(f"```\n{stamp_output}\n```\n\nDataset key: **{dataset_key}**")
    return dataset_key, m, re, stamp_output


@app.cell
def _(mo, run_tcb, yaml_path):
    generate_output = run_tcb("generate", str(yaml_path))
    mo.md(f"```\n{generate_output}\n```")
    return (generate_output,)


@app.cell
def _(api_key, mo, os, run_tcb, tiled_url, yaml_path):
    register_env = os.environ.copy()
    register_env["TILED_URL"] = tiled_url
    register_env["TILED_API_KEY"] = api_key

    register_output = run_tcb(
        "register", "--upload", str(yaml_path), env=register_env
    )
    mo.md(f"```\n{register_output}\n```")
    return register_env, register_output


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## 5. Read it back

    Proof that the upload landed: connect fresh and look for the dataset by key.
    """)
    return


@app.cell
def _(api_key, dataset_key, mo, tiled_url):
    from tiled.client import from_uri

    client = from_uri(tiled_url, api_key=api_key)
    ds = client[dataset_key]

    shared = {
        k.removeprefix("shared_dataset_")
        for k in ds.metadata
        if k.startswith("shared_dataset_")
    }
    entity_key = next(k for k in ds if k not in shared)
    entity = ds[entity_key]
    spectrum = entity["spectrum"][:]
    params = {
        k: v for k, v in entity.metadata.items()
        if not k.startswith(("path_", "dataset_"))
    }

    mo.md(f"""
    Connected to **{tiled_url}**.

    `{dataset_key}` has **{len(ds)}** children ({len(ds) - len(shared)} entities +
    {len(shared)} shared axis).

    Entity `{entity_key}` parameters: `{params}`

    `spectrum` read back at shape `{spectrum.shape}`, dtype `{spectrum.dtype}` — served
    over HTTP by tiled-test, not read from your disk.
    """)
    return client, ds, entity, entity_key, from_uri, params, shared, spectrum


@app.cell(hide_code=True)
def _(dataset_key, mo, work_dir):
    mo.md(f"""## 6. Clean up

    The arrays now live on tiled-test; `{work_dir}` is no longer needed:

    ```bash
    rm -rf {work_dir}
    ```

    Your surname + random suffix keeps this container out of everyone else's way, so
    there is no rush to delete it from the server. When you are done with it:

    ```bash
    tcb delete {dataset_key} --yes
    ```

    That removes the catalog entries **and** the arrays tiled-test stored, since an
    uploaded dataset's only copy is on the server.

    ---

    Next: publish a dataset with your own files, using the same `--upload` command.
    """)
    return


if __name__ == "__main__":
    app.run()

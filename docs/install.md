# How to install

!!! info "Only want to read a catalog?"

    You do not need this package. The catalog is served over HTTP and Tiled's own
    client does all the work: `pip install 'tiled[client]'`, then see
    [Tiled's documentation](https://blueskyproject.io/tiled/) and
    [How to read a registered catalog](using-the-catalog.md).

Everything below is for **registering** data. It runs the `tcb` command line, so it
wants a clone of the repository and **Python 3.12 or newer**.

The same install serves both transports: pointer registration, where the server opens
your HDF5 files itself, and `tcb register --upload`, where the arrays are read locally
and streamed into the server's storage. Which one you use is chosen later, at
[step 4 of the publishing guide](ONBOARDING.md#4-register-the-dataset) — not here.

## 1. Install

=== "uv"

    ```bash
    git clone https://github.com/carbonscott/tiled-catalog-broker
    cd tiled-catalog-broker
    uv run tcb --help
    ```

    There is no separate install step: `uv run` builds `.venv` from the checked-in
    `uv.lock` the first time it is used. Prefix commands with `uv run`, or
    `source .venv/bin/activate` once and drop the prefix. Tools that are not part of
    the package — pytest, marimo — are pulled in per command with `--with`.

=== "pip"

    ```bash
    git clone https://github.com/carbonscott/tiled-catalog-broker
    cd tiled-catalog-broker
    python3 -m venv .venv && source .venv/bin/activate
    pip install -e ".[test]"
    tcb --help
    ```

    For conda or mamba, swap the `venv` line for
    `conda create -n tcb python=3.12 -y && conda activate tcb`.

=== "pixi"

    ```bash
    git clone https://github.com/carbonscott/tiled-catalog-broker
    cd tiled-catalog-broker
    pixi install
    pixi shell
    tcb --help
    ```

    Pixi reads `pyproject.toml` as its manifest, so there is nothing to initialize.
    No `pixi.lock` is checked in; `pixi install` solves and writes your own. To work
    from another directory, activate it by path:
    `pixi shell --manifest-path /path/to/tiled-catalog-broker`.

    Installing pixi itself: <https://pixi.prefix.dev/latest/installation/>.

`tcb --help` printing its four commands is the check. For a fuller one,
`uv run --with pytest pytest -m "not integration"` (plain `pytest` in an activated
pip or pixi environment) needs no server and no credentials.

The `test` extra installs `pytest`. Nothing else is declared: the notebook in
`examples/` and this documentation are run with their tools pulled in per command —
`uv run --with marimo --with matplotlib marimo edit examples/demo_query.py`
([How to explore a dataset in a notebook](exploring-your-data.md)) and
`uv run --with mkdocs-material mkdocs serve`.

## 2. Point at a server

--8<-- "connect.md"

!!! warning "Uploading reads your files locally"

    `tcb register --upload` opens the HDF5 files on the machine it runs on. If your
    data is on a cluster, either install and run there, or copy a subset down first.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `tcb: command not found` | The environment is not active — `source .venv/bin/activate`, `pixi shell`, or `conda activate`. With uv, prefix commands with `uv run` |
| `requires-python >= 3.12` | The environment is older than the package supports. `uv venv --python 3.12`, or recreate the conda environment |
| `ensurepip is not available` | Your distribution ships `venv` separately (`apt install python3-venv`), or use `uv venv` |
| `ModuleNotFoundError: tiled_catalog_broker` | The package was installed into a different environment than the one you are in, or `pip install -e .` was run outside the checkout |

---

Next: [How to publish a dataset](ONBOARDING.md) — or, if you are coming to the
MAIQMag all-hands, [How to prepare for the workshop](workshop-prep.md) first.

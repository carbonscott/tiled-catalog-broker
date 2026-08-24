# Tiled Catalog Broker

**📖 [Read the documentation](https://carbonscott.github.io/tiled-catalog-broker/)**

Register scientific HDF5 datasets in a
[Tiled](https://blueskyproject.io/tiled/) catalog, find them by their physics,
and read them from anywhere.

One dataset YAML is the contract. The broker does not hardcode parameter names,
artifact types, or scientific metadata.

## Quick start

Python 3.12 or later.

```bash
uv sync --extra test --extra examples   # or: pip install -e ".[test,examples]" / pixi install
uv run tcb --help
```

Author a dataset YAML, then walk it through the pipeline:

```bash
tcb stamp-key datasets/mydata.yml  # derive the catalog key from the label
tcb generate datasets/mydata.yml   # scan HDF5 and write Parquet manifests
tcb register datasets/mydata.yml   # register with the configured server
```

Check the entity and artifact counts printed by `tcb generate` before
registering. Set the server with `TILED_URL` and `TILED_API_KEY`; a `.env` in
the working directory is loaded automatically. Add `--upload` to `tcb register`
when the server cannot open the files itself.

Read a registered dataset from anywhere:

```python
from tiled.client import from_uri
from tiled.queries import Key

client = from_uri("http://localhost:8005", api_key="secret")
hits = client["BROAD_SIGMA"].search(Key("sigma") >= 0.04)
spectrum = hits.values().first()["rixs_spectrum"][:]
```

Reading needs none of the install above — just `pip install 'tiled[client]'`.

## Documentation

The [documentation site](https://carbonscott.github.io/tiled-catalog-broker/)
is the complete reference.

| Goal | Page |
|---|---|
| Install `tcb` and point it at a server | [How to install](https://carbonscott.github.io/tiled-catalog-broker/install/) |
| Register your own dataset, either transport | [How to publish a dataset](https://carbonscott.github.io/tiled-catalog-broker/ONBOARDING/) |
| Read a registered catalog | [How to read a registered catalog](https://carbonscott.github.io/tiled-catalog-broker/using-the-catalog/) |
| Look up a command or YAML field | [`tcb` commands](https://carbonscott.github.io/tiled-catalog-broker/reference/cli/) · [dataset YAML](https://carbonscott.github.io/tiled-catalog-broker/reference/dataset-yaml/) |
| Work out why something failed | [Errors and warnings](https://carbonscott.github.io/tiled-catalog-broker/reference/errors/) |
| Understand the design | [broker vs. Tiled](https://carbonscott.github.io/tiled-catalog-broker/explanation/broker-and-tiled/) · [data model](https://carbonscott.github.io/tiled-catalog-broker/explanation/data-model/) |

Domain terminology and the implementation-versus-contract principle are in
[`CONTEXT.md`](CONTEXT.md).

## Develop

```bash
# Unit tests
uv run pytest -v -m "not integration"

# Integration tests; requires a running server with registered data
uv run pytest -v -m integration

# Local Tiled server for the integration tests
uv run tiled serve config config.yml --api-key secret

# Documentation preview at http://127.0.0.1:8000
uv run --with 'mkdocs-material>=9,<10' mkdocs serve
```

The test modules state their scope in their names and markers; use
`pytest --collect-only` to inspect the current suite.

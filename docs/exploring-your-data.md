# How to explore a dataset in a notebook

`examples/demo_query.py` is a marimo notebook that walks a dataset from its container down
to a plotted array. Point it at something you just registered and it confirms the data
landed, the metadata is queryable, and the arrays read back at the shape you expect.

Being the read side, it needs no `tcb`, no HDF5 files, and no shared filesystem — just the
server URL, an API key, and your stamped dataset key. It works the same for a collaborator
who was never involved in registering, which is the point.

## 1. Start the notebook

marimo is not part of the package; `uv run --with` pulls it in for the one command:

```bash
set -a; source .env; set +a
uv run --with marimo --with matplotlib marimo run  examples/demo_query.py   # read it
uv run --with marimo --with matplotlib marimo edit examples/demo_query.py   # read it and change it
```

Without uv, `pip install 'tiled[client]' marimo pandas h5py numpy matplotlib` into any
Python ≥ 3.10 environment, then `marimo edit examples/demo_query.py`.

- `run` renders it as a document — prose, tables, and the plot, code folded away.
- `edit` shows the eight working cells and lets you change them, which is what
  [pointing it at your own dataset](#2-point-it-at-your-dataset) needs.

Either way marimo prints a `http://127.0.0.1:2718/...` URL and tries to open a browser.
Over SSH, add `--headless` and forward the port (`ssh -L 2718:127.0.0.1:2718 …`). Keep the
token in the URL.

## 2. Point it at your dataset

Two names configure the whole notebook. Export them in the shell you start marimo
from:

```bash
export TCB_DEMO_DATASET=<YOUR_DATASET_KEY>    # the key `tcb stamp-key` wrote
export TCB_DEMO_ARTIFACT=<AN_ARTIFACT_TYPE>   # optional
```

`<YOUR_DATASET_KEY>` is the `key:` field in your YAML — the slug of your `label:`, so
`label: "My Dataset"` becomes `MY_DATASET`. `TCB_DEMO_ARTIFACT` is one of your `artifacts:`
`type:` values; unset, the notebook reads whichever artifact the first entity carries.

Both are also editable in the notebook's first code cell:

```python
DATASET_KEY = os.environ.get("TCB_DEMO_DATASET", "BROAD_SIGMA")
ARTIFACT_KEY = os.environ.get("TCB_DEMO_ARTIFACT") or None
```

Those two names are the entire configuration — parameters, shared axes, shapes, dtypes,
even the path to the original file all come back out of the catalog. That is the property
worth checking: if a reader needs to be told anything beyond these two keys, the
registration did not capture enough.

## 3. What the notebook walks through

| Level | What to confirm |
|---|---|
| **Dataset container** | Your provenance metadata (`method`, `material`, `producer`, …) and any shared axes came through |
| **Metadata search** | `client.search(...)` finds your dataset by its metadata, not by its name |
| **Entity container** | Your physics parameters are present as individual queryable fields, one row per entity |
| **Artifact** | The array reads back at the shape and dtype you registered, and plots |

Every level reads over HTTP through the Tiled client, as
[How to read a registered catalog](using-the-catalog.md) describes; the notebook imports
none of this package.

It shows both read paths and compares them byte for byte. The direct HDF5 read is skipped,
with a printed reason, when there is no file to open — the dataset was uploaded, or the
files are not mounted here. The plot reads through the server either way.

## 4. Troubleshooting

| Symptom | Cause |
|---|---|
| `TILED_API_KEY is not set in this kernel's environment` | The key reached your shell but not marimo. `echo $VAR` prints variables that were merely assigned; only *exported* ones reach child processes. Check with `env \| grep TILED_API_KEY`. A marimo server that was already running will not see a variable exported afterwards — restart it |
| `KeyError: '<YOUR_KEY>'` | `TCB_DEMO_DATASET` is not the stamped key. `list(client)` shows what the server has |
| Parameters missing from entity metadata | `parameters:` in the YAML did not match the file layout. Check the `tcb generate` output, then delete and re-register |
| Array shape not what you expect | For `batched` layouts the manifest records the *per-entity* shape, leading axis dropped |

Registration problems belong to
[the publishing guide's troubleshooting table](ONBOARDING.md#troubleshooting).

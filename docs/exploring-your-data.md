# Exploring your data — reading back what you registered

**Audience:** you have just run `tcb register --upload`
(`docs/remote-onboarding.md`) and want to see your data in the catalog:
confirm it landed, check the metadata is queryable, and look at an array.

This is the read side, so it needs much less than registration did. No
`tcb`, no HDF5 files, no shared filesystem — just the server URL, an API
key, and the dataset key you stamped. Anything on this page also works for a
collaborator who was not involved in registering, which is the point.

---

## 1. Install

The notebook talks to the server over HTTP and imports none of this
package, so it needs neither an editable install nor Python ≥ 3.12 —
anything ≥ 3.10 will do.

**With [uv](https://docs.astral.sh/uv/):**

```bash
uv run --with 'tiled[client]' --with marimo --with pandas \
       --with h5py --with numpy --with matplotlib \
  marimo edit examples/demo_query.py
```

**With `venv` + `pip`:**

```bash
python3 -m venv .venv-demo && source .venv-demo/bin/activate
pip install 'tiled[client]' marimo pandas h5py numpy matplotlib
marimo edit examples/demo_query.py
```

If `python3 -m venv` reports that `ensurepip` is unavailable, your
distribution ships `venv` separately (`apt install python3-venv` on
Debian/Ubuntu); `pip install virtualenv && virtualenv .venv-demo` needs no
root.

marimo prints a `http://127.0.0.1:2718/...` URL and tries to open a browser.
Over SSH or where no browser is available, add `--headless` and forward the
port (`ssh -L 2718:127.0.0.1:2718 …`). Keep the token in the URL.

## 2. Point it at your dataset

```bash
export TILED_URL=<URL>
export TILED_API_KEY=<API_KEY>
export TCB_DEMO_DATASET=<YOUR_DATASET_KEY>    # the key `tcb stamp-key` wrote
```

`<YOUR_DATASET_KEY>` is the `key:` field in your YAML — the slug of your
`label:`, e.g. `label: "My Dataset"` → `MY_DATASET`.

The same two names are also editable in the notebook's first code cell,
under the `# Replace with your dataset here` comment, if you would rather
not use the environment:

```python
DATASET_KEY = os.environ.get("TCB_DEMO_DATASET", "BROAD_SIGMA")
ARTIFACT_KEY = os.environ.get("TCB_DEMO_ARTIFACT") or None
```

`ARTIFACT_KEY` is one of your YAML's `artifacts:` `type:` values. Left
unset, the notebook reads whichever artifact the first entity carries, so
it has something to show even on a dataset it has never seen.

Those two names are the entire configuration. Everything else — your
parameters, your shared axes, the shape and dtype of each array, even the
path to the original HDF5 file — the notebook reads back out of the
catalog. That is the property worth checking: if you have to tell a reader
something that is not one of these two keys, the registration did not
capture enough.

## 3. What the notebook walks through

| Level | What to confirm |
|---|---|
| **Dataset container** | Your provenance metadata (`method`, `material`, `producer`, …) and any shared axes came through |
| **Metadata search** | `client.search(...)` finds your dataset by its metadata, not by its name |
| **Entity container** | Your physics parameters are present as individual queryable fields, one row per entity |
| **Artifact** | The array reads back at the shape and dtype you registered, and plots |

## 4. Mode A and Mode B, and which one you get

Two ways to reach an array. The notebook tries both and reports what it
found.

**Mode B** — Tiled serves the array over HTTP:

```python
spectrum = entity["<artifact>"][:]
```

This always works. It needs nothing but the URL and the key, and it is the
only mode available for an uploaded dataset.

**Mode A** — open the original HDF5 file directly, for bulk or custom
analysis. You do not construct the path: Tiled recorded it at registration,
so the notebook asks the catalog and opens what it names.

```python
src = entity["<artifact>"].data_sources()[0]
path = urlparse(src.assets[0].data_uri).path
with h5py.File(path) as f:
    spectrum = f[src.parameters["dataset"]][int(src.parameters["slice"])]
```

**If you registered with `--upload`, expect the notebook to say Mode A is
unavailable.** That is correct, not a failure. Uploading moved the bytes
into Tiled's own storage, so there is no external file to open — the
absence of an external data source *is* the marker. The notebook reports
why and continues with Mode B, which is what the plot reads.

You will also see Mode A skipped if the dataset was registered as pointers
but you are running somewhere the files are not mounted — a laptop reading
a catalog whose data lives on a cluster. Same handling, different reason,
and the message says which.

Your `path_*` / `dataset_*` / `index_*` entity metadata is still recorded
either way. Those are relative-path provenance, kept so you can *search* on
where an entity came from; they are not how you open a file.

## 5. If something looks wrong

| Symptom | Cause |
|---|---|
| `TILED_API_KEY is not set in this kernel's environment` | The key reached your shell but not marimo. `echo $VAR` prints variables that were merely assigned; only *exported* ones reach child processes — check with `env \| grep TILED_API_KEY`, and re-run as `export TILED_API_KEY=<key>`. A marimo server that was already running will not see a variable exported afterwards; restart it. |
| `KeyError: '<YOUR_KEY>'` | `TCB_DEMO_DATASET` is not the stamped key. `list(client)` shows what the server has. |
| Entity count lower than expected | An interrupted upload. Re-run the same `tcb register --upload`; it skips what is already there. |
| Parameters missing from entity metadata | `parameters:` in the YAML did not match the file layout. Check `tcb generate` output and re-register. |
| Array shape not what you expect | For `batched` layouts the manifest records the *per-entity* shape, leading axis dropped. |
| `Mode A unavailable — no external data source` | Expected for `--upload`. See above. |

To start over: `tcb delete <YOUR_DATASET_KEY>`. For an uploaded dataset
that removes the arrays the server stored as well as the catalog entries;
your local files are untouched. Then register again.

---

Full read-side reference, independent of this notebook:
`docs/using-the-catalog.md`. Registration: `docs/remote-onboarding.md`.

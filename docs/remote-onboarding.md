# Remote onboarding — registering data the server cannot see

**Audience:** you have HDF5 data on your own machine (laptop, university
cluster), and someone has given you the `<URL>` and `<API_KEY>` of a Tiled
server run elsewhere. You want your data in that catalog, queryable and
readable by your collaborators, without moving files onto the server's
filesystem yourself.

The standard registration path (`docs/ONBOARDING.md`) registers *pointers*:
the server opens the HDF5 files from its own disk at read time. That only
works when the server can see your filesystem. This path is the other one —
`tcb register --upload` reads the arrays from your local files and writes
them **through** the server into its own storage. After that the server owns
the bytes: your local files are no longer involved, and the data persists on
the server until it is deleted from the catalog.

Everything before the final step is identical to normal onboarding — same
YAML contract, same manifests. Only the transport differs.

---

## 1. Install

You need Python ≥ 3.12. Either toolchain below leaves you with an
activated environment and a working `tcb`.

**With [uv](https://docs.astral.sh/uv/):**

```bash
git clone https://github.com/carbonscott/tiled-catalog-broker
cd tiled-catalog-broker
uv venv && source .venv/bin/activate
uv pip install -e .
tcb --help
```

**With the standard library (`venv` + `pip`):**

```bash
git clone https://github.com/carbonscott/tiled-catalog-broker
cd tiled-catalog-broker
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e .
tcb --help
```

Name whichever interpreter is ≥ 3.12 on your machine — `python3.12`,
`python3.13`, or a `module load`ed one on a cluster. If `python3 -m venv`
reports that `ensurepip` is unavailable, your distribution ships `venv`
separately (`apt install python3.12-venv` on Debian/Ubuntu); `pip install
virtualenv && virtualenv -p python3.12 .venv` works without root.

Re-activate with `source .venv/bin/activate` in each new shell.

Reading the data back is a separate, lighter setup — see
[docs/exploring-your-data.md](exploring-your-data.md), the companion to this
page.

## 2. Author the dataset YAML

Follow `docs/ONBOARDING.md` — the contract is the same. The one field to
note: `data.directory` must be the path to your data **on your machine**;
that is where `--upload` reads the arrays from. Leave
`data.server_base_dir` out — the server never reads your files, so there is
no server-side mount to describe.

```yaml
# datasets/mydata.yml (abridged — see ONBOARDING.md for the full contract)
label: My Dataset
metadata:
  method: [INS]
  data_type: experimental
  material: NiPS3
data:
  directory: /home/me/experiments/run42   # local to YOU
  layout: per_entity
  file_pattern: "*.h5"
parameters:
  location: root_scalars
artifacts:
  - type: spectrum
    dataset: /spectrum
```

## 3. Stamp the key and generate manifests

```bash
tcb stamp-key datasets/mydata.yml
tcb generate datasets/mydata.yml
```

`tcb stamp-key` writes the catalog key (derived from `label`) into the
YAML; `tcb generate` validates the contract — it refuses to run before the
key is stamped — then opens your HDF5 files locally and records each
artifact's path, shape, and dtype in Parquet manifests. Nothing has
touched the network yet.

## 4. Point at the server and upload

```bash
export TILED_URL=<URL>
export TILED_API_KEY=<API_KEY>

tcb register --upload datasets/mydata.yml
```

Each entity is created with its parameters as queryable metadata, and each
artifact's array is read from your local file and streamed to the server.
The dataset container is stamped `storage: uploaded` so tooling knows the
server holds the bytes.

Registration is incremental: if the connection drops partway, re-run the
same command and already-registered entities are skipped.

Two things to expect, compared to pointer registration:

- **It moves the actual data.** Wall-clock scales with dataset size and
  your uplink, not with entity count. Try a subset first:
  `tcb register --upload -n 5 datasets/mydata.yml` (delete the subset
  before re-registering the full set, or let the re-run skip those 5 and
  continue).
- **A dataset is one transport or the other.** You cannot add uploaded
  entities to a dataset that was registered as pointers, or vice versa —
  `tcb register` refuses with an error rather than mixing them.

## 5. Verify

From any machine that can reach the server:

```python
from tiled.client import from_uri

c = from_uri("<URL>", api_key="<API_KEY>")
ds = c["MY_DATASET"]                 # the stamped key
ent = ds[list(ds)[0]]
ent["spectrum"][:]                   # served from the server's storage
```

This is ordinary Mode B access (`docs/using-the-catalog.md`) — your
collaborators need nothing beyond the Tiled client. Mode A locators
(`path_*` metadata) are still recorded as provenance, and still work for
anyone with access to the original files — i.e. you.

## 6. Undo

```bash
tcb delete MY_DATASET
```

For an uploaded dataset this removes the catalog entries **and** the arrays
the server stored — the catalog is their only server-side home, so deleting
the pointers alone would leave orphaned bytes. Your local HDF5 files are,
as always, untouched.

---

## Running the server side (demo hosts)

The server needs exactly one thing beyond a stock Tiled catalog config:
`writable_storage`. `config.demo.yml` at the repo root is a complete
minimal example:

```bash
uv run --with 'tiled[server]' tiled serve config config.demo.yml --api-key <API_KEY>
```

Tiled requires the API key to be strictly alphanumeric (generate one with
`openssl rand -hex 32`) — a key with hyphens or other punctuation makes
the server exit at startup.

No `readable_storage` and no admin path-allowlisting — the server never
reads registrants' files, so there is nothing to allowlist. Hand
participants the URL and the API key, and step 4 above is all they do.

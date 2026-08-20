# Workshop prep — before the session

**Audience:** you are attending a hands-on session where you will register your
own HDF5 data into a Tiled catalog. This page is what to do *beforehand*, so
the session is spent on your data rather than on setup.

Budget about 15–20 minutes. If something does not work, tell the organizer
before the session rather than on the day.

The session itself follows [docs/remote-onboarding.md](remote-onboarding.md)
(getting data in) and [docs/exploring-your-data.md](exploring-your-data.md)
(getting it back out). You do not need to read those in advance.

---

## 1. Install the tool

You need Python 3.12 or newer.

```bash
git clone https://github.com/carbonscott/tiled-catalog-broker
cd tiled-catalog-broker
uv venv && source .venv/bin/activate
uv pip install -e ".[test]"
tcb --help
```

If you do not have [uv](https://docs.astral.sh/uv/), the standard library
route works too:

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e ".[test]"
```

Any other environment manager is fine — conda, pixi, whatever you already
use — as long as it installs this package into a Python 3.12+ environment.
The `[test]` extra pulls in `pytest` for the next step.

`uv` fetches its own Python, so it is the easier route when the system
interpreter is older than 3.12. Re-activate with `source .venv/bin/activate`
in each new shell.

## 2. Check the install

```bash
pytest tests/test_config.py tests/test_utils.py tests/test_generic_registration.py -q
```

You should see `56 passed`. This needs no server and no credentials — it only
confirms the install is sound. If it fails, send the output to the organizer.

## 3. Optionally, install a coding agent

If you use [Claude Code](https://claude.ai/code) or a similar coding agent,
this repo ships an `/onboarding` skill that inspects your HDF5 files and
drafts the dataset configuration for you. It is the easier path and it is
usually what gets demonstrated. Everything is doable by hand if you prefer —
both routes are documented.

## 4. Get your data ready

This is the part most worth doing carefully in advance.

### Where it has to be

**On the machine you will run the tool from.** The session uses an upload
path that reads your files locally and streams them to the server, so your
data must be on the same laptop or workstation where you ran step 1.

If your data lives on a cluster, either install and run there, or copy a
subset down first. This is the single most common thing that catches people
out, and it cannot be fixed during the session.

### What format

HDF5 (`.h5` / `.hdf5`). Nothing else will work.

### How it needs to be organised

Your files should hold **many comparable entities** — simulation runs,
samples, measurements — that share a structure, each with its own parameters
and one or more arrays. One of these three shapes:

| Shape | What it looks like |
|---|---|
| **One file per entity** | `entity_0001.h5`, `entity_0002.h5`, … each holding that entity's arrays, with its parameters as scalars in the file. |
| **Entities stacked in one file** | a `(10000, 151, 40)` dataset where entity *i* is row *i*, with parameters in a parallel array (often a `/params` group). |
| **One group per entity** | `/samples/sample_000/`, `/samples/sample_001/`, … each self-contained. |

You do **not** need to rename parameters, adopt a shared schema, convert file
format, or match anyone else's columns. Your parameters stay yours. What is
required is only that the data already sits in one of the three arrangements
above.

If yours does not resemble any of them, say so before the session — it is
usually possible to map, but better sorted in advance.

### How big

Bring something **small — under about 1 GB**. The upload moves the actual
bytes over the network, and if everyone uploads at once a large dataset will
not finish inside the session. A subset of a bigger dataset is ideal; there
is a flag to register only the first few entities, and it will be used.

### What you should know about it

You will be asked to record what produced the data (instrument, or the code
and its version), the material or system studied, and what each per-entity
parameter means.

A coding agent can read shapes and dtypes off your files, but it cannot know
your provenance. Bring that knowledge, or a README or paper that has it.

## 5. Set up your access key

Your organizer will send you an API key separately — typically through a
one-time link that expires, so open it when it arrives rather than leaving it
until the day.

Create a file called `.env` in the repo root (next to `pyproject.toml`):

```
TILED_URL=<the server URL your organizer gives you>
TILED_API_KEY=<your key>
```

Load it in each shell you work in:

```bash
set -a; source .env; set +a
```

`.env` is gitignored. Treat it like a password.

### Check it worked

```bash
python -c "
from tiled.client import from_uri
from tiled_catalog_broker.config import get_tiled_url, get_api_key
print(list(from_uri(get_tiled_url(), api_key=get_api_key())))
"
```

<!-- Keep this snippet free of quoted strings: `pixi run` strips inner quotes. -->

A list of dataset names means you are ready. `Connection refused` means the
`.env` was not loaded in this shell; `401` means the key is wrong — ask the
organizer to reissue it.

## 6. Choose a name for your dataset

Everyone registers into the same shared catalog, and the container key is
derived from the name you choose. Prefix yours with your surname or initials
— `Okafor NiPS3 Powder` rather than `My Dataset`. Two people picking the same
name land in the same container; not destructive, but confusing to untangle
mid-session.

---

## Checklist

- [ ] `tcb --help` runs
- [ ] `56 passed` from the test command
- [ ] Data is HDF5, on this machine, under ~1 GB, in one of the three shapes
- [ ] I know what produced it and what its parameters mean
- [ ] `.env` written and loaded, and the check command lists dataset names
- [ ] I have picked a dataset name with my surname in it

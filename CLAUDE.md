## Environment Setup

Use `uv` to run python programs (`uv run tcb ...`, `uv run --with pytest pytest ...`).
Nothing in this repo assumes a particular machine or facility: it runs from any checkout
with `uv` installed, and the local server config (`config.yml`) serves data placed under
`./data/` with no edits.

**Only if you are on SLAC's SDF**, you may point uv at the shared cache to avoid repeated
package downloads:

```bash
export UV_CACHE_DIR=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cwang31/.UV_CACHE
```

Off SDF that path does not exist and setting it makes every `uv` command fail — leave
`UV_CACHE_DIR` unset and use uv's default cache.

## Project Overview

**Tiled Catalog Broker** — a config-driven system for registering
multi-modal scientific HDF5 datasets into a
[Tiled](https://blueskyproject.io/tiled/) catalog. Data model inspired by
[ArrayLake](https://docs.earthmover.io/concepts/data-model) (Organization →
Repo → Group → Array), adapted for many-entity scientific data with queryable
metadata.

**Hierarchy:** Dataset → Entity → Artifact
- **Datasets** are top-level containers (BROAD_SIGMA, LCLS_RIXS_STATIC, etc.)
  with provenance metadata (method, material, producer, facility)
- **Entities** are containers with physics parameters as queryable metadata
- **Artifacts** are array children of their parent entity
- **Keys are human-readable**: `client["BROAD_SIGMA"][entity_key]["rixs_spectrum"]`

**Two read paths**, both documented in `docs/using-the-catalog.md`:
- **Through the server** — arrays over HTTP via the Tiled client, sliced server-side.
  The default, and the only option for an uploaded dataset.
- **Straight from the files** — entities carry `path_/dataset_/index_` locators, so a
  reader on the same filesystem can open the HDF5 with h5py and skip the server. For
  bulk reads; `clients/query_manifest.py` is the helper.

The terms **"Mode A" and "Mode B" are retired from the docs site** — they were a
taxonomy readers had to memorize before either name meant anything. The concept stays;
call the paths by what they do. `src/`, `tests/`, and `docs/adr/` still use the old
names internally.

The broker is **dataset-agnostic**. The Parquet manifest is the contract: no
parameter names, artifact types, or file layouts are hardcoded.

## Directory Structure

```
tiled-catalog-broker/
├── CLAUDE.md                  # This file
├── pyproject.toml             # Package definition (tiled-catalog-broker)
├── config.yml                 # Tiled server configuration
├── src/
│   └── tiled_catalog_broker/  # Installable Python package
│       ├── cli.py             # CLI: tcb {generate,stamp-key,register,delete}
│       ├── config.py          # Server connection settings from the environment
│       ├── http_register.py   # HTTP registration via Tiled client (the single route)
│       ├── utils.py           # Shared helpers
│       ├── adapters/          # Tiled array adapters
│       ├── tools/             # Data-prep tools
│       │   ├── _models.py     # Pydantic dataset YAML contract (the contract surface)
│       │   ├── generate.py    # Generate Parquet manifests from YAML
│       │   └── schema.py      # YAML contract validation + soft vocab checks
│       └── clients/           # Client-side utilities
│           ├── tiled_cache.py # Disk-backed cache + PyTorch Dataset
│           └── query_manifest.py  # bulk direct-HDF5 loader (the read-from-files path)
├── examples/                  # demo_query.py — marimo notebook of the read path
├── tests/                     # Test suite
├── includes/                  # Prose fragments shared between docs pages (`--8<--`)
└── docs/                      # Published documentation (mkdocs)
```

## How to Run

```bash
# Run from the checkout: uv builds .venv from the checked-in uv.lock on first use
# (pixi.lock deliberately is not checked in). Extra tools come in per command
# with --with (pytest, marimo, mkdocs-material) — nothing beyond `test` is a declared extra.
uv run tcb --help

# Pipeline: author YAML → stamp-key → generate → register
tcb stamp-key datasets/my_dataset.yml
tcb generate datasets/my_dataset.yml
tcb register datasets/my_dataset.yml     # needs a running server (TILED_URL, TILED_API_KEY)
tcb register --upload datasets/my_dataset.yml  # stream arrays into server storage (server can't see the files)

# Serve (from the repo root: config.yml's readable_storage lists `data`, resolved
# relative to the server's CWD, so ./data/<DATASET> is readable with no config change)
uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret
```

## Running Tests

```bash
# Unit tests (no server required)
uv run --with pytest pytest tests/test_config.py tests/test_utils.py tests/test_generic_registration.py -v

# Integration tests (requires running server with data)
uv run --with pytest pytest tests/ -v
```

## Architecture

Entity keys are `{dataset_key}_{uid[:13]}`, derived at registration from the dataset key and
the manifest uid. Artifact keys are the manifest's `type` verbatim.

```
/ (root)
├── BROAD_SIGMA/                     ← dataset container
│   metadata: {method, data_type, material, producer, ...}
│   ├── eloss                        ← shared axis (151,), registered once
│   ├── BROAD_SIGMA_1a2b3c4d5e6f7/   ← entity container
│   │   metadata: {sigma, gamma, ...} + path_/dataset_/index_ locators
│   │   └── rixs_spectrum            ← array artifact (151, 40)
│   └── ...
├── CONCATENATED_MULTIMODAL/         ← dataset container
│   ├── CONCATENATED_MULTIMODAL_.../
│   │   metadata: {J1a, J1b, ...}
│   │   ├── hisym                    ← array artifact (384, 384)
│   │   ├── powder                   ← array artifact (512, 256)
│   │   └── ...
│   └── ...
├── LCLS_RIXS_STATIC/                ← experimental dataset
└── ...
```

The dataset YAMLs this repo has onboarded are in `datasets/`; each one's `key:` is the
container key it registers into. For what is actually registered on a given server, ask the
server — `list(from_uri(url, api_key=key))`.

## Related Documentation

The published site is <https://carbonscott.github.io/tiled-catalog-broker/>, built from
`docs/` by `mkdocs.yml` (`uv run --with mkdocs-material mkdocs serve` to preview). Pages
are grouped by the four kinds of documentation (diataxis.fr) — keep new pages in the kind
they belong to rather than blending instruction, reference, and discussion on one page.
How-to pages are titled "How to …". Explanation pages take a bare noun phrase — the
"about" is implicit, not written ("Sliced reads", not "About sliced reads"), and
cross-references to them read "see [sliced reads](…)". Explanation lives
under `docs/explanation/`.

Two conventions keep the guides from re-explaining each other:

- **Nav order is the reading order.** Material's footer "next" button follows `nav:`, and
  Guides are ordered the way a dataset moves through them: install, prepare, publish,
  read, explore. Moving a page in `nav:` changes where a reader is sent next.
- **Prose shared by more than one page lives in `includes/`** (outside `docs_dir`, wired
  up through `pymdownx.snippets`' `base_path`) and is pulled in with `--8<--`. Today that
  is `includes/connect.md`, the `.env` setup and connection check, used by both install
  pages and by the workshop guide. Anything else that would be repeated should either
  move there or become a link.

| Document | Description |
|----------|-------------|
| `CONTEXT.md` | Domain language + the implementation-vs-contract principle |
| `docs/install.md` | How-to: install `tcb` (clone, Python 3.12+, uv / pip / pixi), point at a server, troubleshoot. Reading a catalog needs only `tiled[client]`, so that case is one admonition at the top rather than a page of its own |
| `docs/workshop-prep.md` | How-to: what all-hands participants do before the session |
| `docs/ONBOARDING.md` | How-to: publish a dataset (titled "How to publish a dataset"; filename kept so the published URL and the `/onboarding` skill's references still resolve). **Both transports** — pointer and `--upload` — as linked content tabs at steps 1, 3, and 4 |
| `docs/using-the-catalog.md` | How-to: read a registered dataset — through the server, and straight from the files |
| `docs/exploring-your-data.md` | How-to: browse a registered dataset in the marimo notebook |
| `docs/reference/cli.md` | Reference: the four `tcb` subcommands, flags, exit codes, env vars |
| `docs/reference/dataset-yaml.md` | Reference: every dataset-YAML field |
| `docs/reference/manifest.md` | Reference: the two Parquet manifests and their columns |
| `docs/reference/errors.md` | Reference: every `tcb`/server message with a specific cause, by pipeline stage. The full symptom→cause→fix table; `docs/ONBOARDING.md`'s Troubleshooting section carries only the common rows and links here |
| `docs/explanation/broker-and-tiled.md` | Explanation: what the broker adds to stock Tiled, and where the line falls |
| `docs/explanation/data-model.md` | Explanation: why Dataset → Entity → Artifact |
| `docs/explanation/layouts.md` | Explanation: the three layouts and why the set is frozen |
| `docs/explanation/sliced-reads.md` | Explanation: why an entity is a slice, and why the broker ships its own adapter |
| `docs/explanation/vocabulary.md` | Explanation: why the vocabulary is soft |
| `docs/explanation/nexus.md` | Explanation: how a NeXus file maps onto the (generic) contract, and what is deliberately not modeled |
| `docs/adr/` | Architecture Decision Records (frozen layouts, single register route, soft vocab, hierarchical containers). **Internal — not published** to the site, and not cited from published pages: they carry issue numbers, names, and dataset specifics. Put the reasoning in `docs/explanation/` in its own words instead |

# The `tcb` command line

`tcb` dispatches four subcommands. Run bare or with `-h` / `--help` for the list; an
unrecognized subcommand exits `1`.

```
tcb <command> [args]

  generate    Generate Parquet manifests from a finalized YAML contract
  stamp-key   Write the derived catalog key into a YAML
  register    HTTP registration against a running Tiled server
  delete      Delete registered data from a running Tiled server
```

Pipeline order is `stamp-key` → `generate` → `register`; the walkthrough is
[How to publish a dataset](../ONBOARDING.md). Source:
`src/tiled_catalog_broker/cli.py`.

---

## `tcb stamp-key`

Writes the derived catalog key into a dataset YAML's `key:` field.

```
tcb stamp-key CONFIG [CONFIG ...]
```

| | |
|---|---|
| Reads | each `CONFIG`'s `label` |
| Writes | `key:` in each `CONFIG`, in place |
| Server | not required |

The key is `slugify_key(label)` — `"Broad Sigma"` becomes `BROAD_SIGMA`. The YAML
round-trips through `ruamel.yaml` with `preserve_quotes`, so comments and quoting survive.

Exit status:

| Condition | Status |
|---|---|
| Key written, or already equal to `slug(label)` (no change) | `0` |
| `CONFIG` does not exist | `1` |
| `label` missing | `1` |
| Existing `key` differs from `slug(label)` | `1` |

A drifted key is never silently rewritten. Recovery is in
[errors and warnings](errors.md#tcb-stamp-key).

---

## `tcb generate`

Scans the HDF5 files and writes the two Parquet manifests that registration reads.

```
tcb generate YAML_PATH [-o OUTPUT_DIR] [--append]
```

| Flag | Effect |
|---|---|
| `-o`, `--output-dir DIR` | Write manifests to `DIR` instead of the default |
| `--append` | Keep existing manifests and add only entities whose `uid` is not already present |

| | |
|---|---|
| Reads | the YAML, and every HDF5 file matched by `data.directory` + `data.file_pattern` |
| Writes | `entities.parquet` and `artifacts.parquet` in `<yaml_dir>/manifests/<label>/` |
| Server | not required |

Takes exactly one YAML, unlike `stamp-key` and `register`. Validates against the contract
first — including that `key` is stamped — and prints non-fatal controlled-vocabulary
warnings.

Each artifact's `shape` and `dtype` are captured here, which is what lets registration run
without opening a single HDF5 file, and why the manifest must be regenerated when the data
changes shape or dtype. See [The Parquet manifest](manifest.md).

---

## `tcb register`

Registers manifests into a running Tiled server over HTTP. This is the only registration
route.

```
tcb register CONFIG [CONFIG ...] [-n NUM] [--max-workers N] [--upload]
```

| Flag | Effect |
|---|---|
| `-n`, `--max-entities NUM` | Register at most `NUM` entities per dataset (default: all) |
| `--max-workers N` | Concurrent registration workers (default: `TCB_MAX_WORKERS`, else `8`) |
| `--upload` | Read the arrays from local HDF5 and write them through the server into its writable storage, instead of registering pointers to files the server reads itself |

| | |
|---|---|
| Reads | each `CONFIG`, its manifests, and with `--upload` the HDF5 files themselves |
| Writes | catalog entries on the server; with `--upload`, array data in the server's storage |
| Server | **required** |

- **Incremental.** An entity whose key already exists is skipped, so a re-run resumes
  rather than duplicates.
- **Shared axes go first** — the manifest's `uid`-less rows, one array child of the dataset
  container each, over the same transport as entity artifacts. Independent of `-n`; an
  existing axis is skipped.
- **Without `--upload`** the server opens the files itself, so `data.directory`,
  `data.server_base_dir`, and the server's `readable_storage` all have to agree. Getting
  them wrong registers cleanly and fails on every read — see
  [How to publish a dataset](../ONBOARDING.md#paths-where-your-view-and-the-servers-view-differ).
- **With `--upload`**, `data.server_base_dir` is ignored (announced, not an error) and
  `data.directory` must exist locally. A dataset cannot mix uploaded and pointer entities.

Manifests are searched for in this order, taking the first directory holding both files:

```
<yaml_dir>/manifests/<label>/
<cwd>/manifests/<label>/
```

A `label` containing spaces is matched both verbatim and underscore-normalized; if neither
directory has them, the searched paths are printed with the `tcb generate` command to run.

Exit status is `1` for an unreachable server, a missing config or manifest, an unstamped or
drifted `key`, or a `ValueError` during registration.

---

## `tcb delete`

Removes registered data from a running server. Granularity is inferred from the number of
positional arguments.

```
tcb delete DATASET [ENTITY [ARTIFACT]] [-y] [--dry-run]
tcb delete all [--confirm URL] [--dry-run]
```

| Form | Deletes |
|---|---|
| `tcb delete DATASET` | the dataset container and everything under it |
| `tcb delete DATASET ENTITY` | one entity and its artifacts |
| `tcb delete DATASET ENTITY ARTIFACT` | one artifact array |
| `tcb delete all` | every top-level container |

| Flag | Effect |
|---|---|
| `-y`, `--yes` | Skip the `y`/`yes` prompt (granular forms only) |
| `--confirm URL` | Bypass the URL-retype prompt for `all`; must match `TILED_URL` after normalization |
| `--dry-run` | Print the preview block and exit without deleting |

`all` is a reserved sentinel and takes no further arguments, so a dataset whose key is
literally `all` cannot be deleted with the single-argument form.

Confirmation differs by form:

- **Granular** — prompts for `y` or `yes`.
- **`all`** — requires retyping the server URL. Comparison lowercases scheme and host and
  strips trailing slashes, so `https://Tiled.example.com/` matches
  `https://tiled.example.com`; path, query, and fragment case are preserved.
- **Non-interactive shell** — no prompt is shown; the command exits `2` telling you to pass
  `--yes` or `--confirm`.

External HDF5 files are never removed. For an `--upload` dataset the stored arrays go with
the catalog entries, since the catalog is their only home; the preview block says which
case applies before it asks.

Exit status is `0` on success or `--dry-run`, `1` on an unreachable server, an unresolvable
target, a declined confirmation, or any failure during `all`, and `2` on an argument or
non-interactive-confirmation error.

---

## Environment

Server settings come from the environment, not a CLI flag. Source:
`src/tiled_catalog_broker/config.py`.

| Variable | Used by | Default |
|---|---|---|
| `TILED_URL` | `register`, `delete` | `http://localhost:8005` |
| `TILED_API_KEY` | `register`, `delete` | falls back to `TILED_KEY`, then empty |
| `TCB_MAX_WORKERS` | `register` | `8` |

A `.env` in the working directory is loaded automatically on import via `setdefault`, so a
variable already exported in the shell wins over the file. Switching servers comes down to
which `.env` you are next to.

!!! note

    Child processes do not inherit that load. A separate program such as `marimo` needs the
    variables exported into its own environment (`set -a; source .env; set +a`) — which is
    why the [read-back guide](../exploring-your-data.md) insists on it.

`config.yml` is the **Tiled server's** configuration, consumed by
`tiled serve config config.yml`. The broker never reads it.

# Errors and warnings

Every message `tcb` and the server produce that has a specific cause, in the order of the
pipeline that raises it. [How to publish a dataset](../ONBOARDING.md) has a short table of
the common ones; this page is the full set.

Generation errors name the file and the YAML key involved. Registration and read errors
come from the server and are terser — a read failure in particular is ambiguous from the
client by construction, so the last section decodes it by elimination.

---

## `tcb stamp-key`

| Message | Cause | Fix |
|---|---|---|
| `label` missing | The YAML has no `label` | Add one; the key derives from it |
| stored `key` differs from `slug(label)` | `label` changed after the key was stamped | Restore the old `label`, or delete the `key` line and stamp again. A drifted key is never silently rewritten, because the old key may already hold registered data |

---

## `tcb generate`

| Message | Cause | Fix |
|---|---|---|
| `Validation failed: ... 'key' is required` | The YAML is not stamped | `tcb stamp-key` |
| `Validation failed: ... Extra inputs are not permitted` | An unknown or typo'd key in a closed section — `data`, `artifacts`, `parameters`, `shared` | Fix the key. [The YAML reference](dataset-yaml.md#strictness) has the field list; `metadata` is the only open section |
| `OSError: Unable to open file ... file signature not found` | `file_pattern` matched a non-HDF5 sibling — a NetCDF twin, a Parquet sidecar, a `.gz` | Tighten `file_pattern`. `ls` the directory first |
| `<file>: artifact type=... dataset '...' not found` | Wrong HDF5 path — or, in a `grouped` layout, an absolute path where the path is resolved relative to the entity group | Dump one file (`h5py.File(p).visititems(print)`) and correct `dataset` |
| `shared axis type=...: dataset ... not found in any of the N files` | Wrong path for a `shared` entry | Correct the path |
| `shared axis type=... differs between a.h5 and b.h5` | The array is not actually shared — it varies per entity | It is an artifact, not an axis. Move it to `artifacts` |
| `pyarrow.lib.ArrowInvalid` while writing the manifest | A parameter changes type across files — scalar in one, string or array in another | Dump two files and diff their parameters; fix the inconsistent file |
| `WARNING: /path@attr is not carried as array metadata` | A dataset attribute is named like a manifest column: `type`, `shape`, `dtype`, `file`, `dataset`, `index`, `uid` | Harmless — the attribute is dropped, not the array. Rename it in the file if you need it carried |
| Controlled-vocabulary warnings | A `metadata` value is not in the semantic model | Non-fatal by design (see [the soft vocabulary](../explanation/vocabulary.md)). Prefer a canonical id so the dataset lands on the same facets as everyone else's |
| Entity or artifact counts differ from what you expected | The glob matched extra or fewer files; a `batched` leading axis is not what you assumed (it is read from `artifacts[0]`); `entity_group` is wrong, or a non-entity subgroup was counted | Count the files (`ls <directory>/<file_pattern> | wc -l`), dump one file, fix, regenerate |

Expected counts: `per_entity`, the files `file_pattern` matches; `batched`, the leading axis
of `artifacts[0]` summed over files; `grouped`, the entity groups. Artifacts are entities ×
`artifacts` entries, plus one per `shared` axis.

---

## `tcb register`

| Message | Cause | Fix |
|---|---|---|
| `ERROR: manifests not found for '<label>'` | `tcb generate` was not run, or was run on a YAML with a different `label` | Run `tcb generate` on this YAML. The message prints the directories it searched |
| `dataset '<KEY>' exists with storage='external' but this run would register storage='uploaded'` (or the reverse) | The key was registered with the other transport, and a dataset is one or the other | `tcb delete <KEY>` and re-register, or choose a new `label` |
| `415 ... mimetype application/x-hdf5-broker is not one that the Tiled server knows how to read` | The server's config has no `adapters_by_mimetype` entry for the broker adapter, or the broker package is not importable in the server's environment | Server side: add the entry ([step 3 of the publishing guide](../ONBOARDING.md#3-make-local-and-server-paths-agree) has it). On a server you do not run, ask its operator |
| `WARNING ent=... half-registered: k of n artifacts on the server; registering the rest` | A previous run died partway through that entity | Nothing — this run attaches the missing artifacts. Check that `Artifact errors` is `0` |
| `httpx.ConnectError` / connection refused | The server is not running, the URL is wrong, or it is not reachable from this host | Check `TILED_URL` and reachability |
| `401` naming the scopes it wanted | The API key lacks a scope — write for `register`, delete for `tcb delete` | The message names both required and held scopes; use a key that has them |

`Artifact errors: 0` in the summary block says nothing about whether the server can read a
byte. See the read-back below.

---

## Reads through the server

| Symptom | Cause | Fix |
|---|---|---|
| Registration clean, **every** read returns HTTP 500 | One of: the server is missing `adapters_by_mimetype`; `data.server_base_dir` is unset or wrong; `data.directory` is not under the server's `readable_storage`; `directory` was written through a symlink; or the file is unreadable by the server process. Nothing opens the pointer until a read, so `generate` and `register` both pass on all of these | [Step 3 of the publishing guide](../ONBOARDING.md#3-make-local-and-server-paths-agree). On a server you run, the log line `Refusing to serve file://... because it is outside the readable storage area` names the offending path. Then regenerate, `tcb delete` the dataset, and register again — an existing artifact is never rewritten |
| Read returns 500 for **some** entities only | Stale manifest: those files changed shape or dtype since `tcb generate`, and the adapter re-checks both against the file | Regenerate, delete, re-register |
| `A manifest without shape/dtype` on read | The manifest predates shape/dtype capture | Regenerate; it is cheap and idempotent |
| `KeyError: '<KEY>'` right after a clean register | You are reading a different server than you registered into | Use the same `TILED_URL` and `TILED_API_KEY` for both |
| Array shape is not what you expect | For a `batched` layout the manifest records the *per-entity* shape, leading axis dropped | Expected — see [sliced reads](../explanation/sliced-reads.md) |

The symlink case is worth restating because it is silent and expensive: the server's
containment check compares the path **as written** against `readable_storage` and never
resolves links, so a logical path that merely symlinks into an allowed root is refused.
Write `data.directory` as a physical path (`pwd -P`, `readlink -f`).

# How to do a practice upload

`examples/practice_upload.py` is a marimo notebook that rehearses `tcb register
--upload` before your own data is involved. It generates a small synthetic HDF5
dataset, walks it through the real three-command pipeline — `tcb stamp-key`,
`tcb generate`, `tcb register --upload` — against **tiled-test**, and reads the
result back to prove it landed.

Do this if you have never run the upload transport before, or just want to see the
whole pipeline succeed once with data that cannot go wrong.

## Why tiled-test, and why it is safe

tiled-test is a shared server kept for exactly this. It has no filesystem mount, so
the pointer-registration transport (`tcb register`, no `--upload`) is not even
possible there — only upload works, which makes it a good match for practicing that
one path deliberately.

Nothing you register there can collide with anyone else's data:

- The notebook asks for your last name and appends a random suffix to it (e.g.
  `WELBORN_PRACTICE_UPLOAD_E821`), so re-running it, or someone else with the same
  surname, never lands in the same container.
- It is a dataset you generated yourself, seconds ago, with metadata that says so
  (`producer: tcb-practice-upload`).
- Deleting it removes only that one container. `tcb delete <YOUR_KEY> --yes` is the
  last cell.

## 1. Start the notebook

marimo comes from the `examples` extra, which [How to install](install.md) includes
by default:

```bash
set -a; source .env; set +a
marimo edit examples/practice_upload.py
```

`TILED_API_KEY` must be exported into the shell marimo starts from (the notebook
checks and tells you if it is missing). `TILED_URL` is optional — leave it unset and
the notebook targets tiled-test itself.

## 2. Enter your last name and run the cells

The first interactive cell is a form. Type your last name and submit it; every cell
below regenerates from there — synthetic files, a dataset YAML, the three `tcb`
commands as subprocesses, then a read-back through the Tiled client.

Each `tcb` step's real output is printed in the notebook, including this warning
from `tcb generate`, which is expected:

```
Warning: metadata.method 'Synthetic' not in catalog model — allowed: [...]
```

The dataset's `method` is intentionally not a real scientific technique. Soft-vocab
warnings never block generation or registration — see
[soft vocabulary](explanation/vocabulary.md) — and this is a convenient place to see
one fire without wondering whether it means something is wrong.

## 3. Confirm, then clean up

The last two cells read the uploaded dataset back — container key, entity count, one
entity's parameters, one array's shape read over HTTP — and then show the exact
cleanup commands: removing the local temp directory (no longer needed once the
bytes are on the server) and, whenever you are ready, `tcb delete <YOUR_KEY> --yes`.
Neither runs automatically; there is no rush to delete it, and leaving it does not
affect anyone else's data.

## Next

[How to publish a dataset](ONBOARDING.md) — the same `--upload` command, on your own
files. [How to prepare for the workshop](workshop-prep.md) if you have not been
through that yet.

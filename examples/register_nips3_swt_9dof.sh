#!/usr/bin/env bash
# Register the NiPS3 SWT 9DoF dataset against the dev Tiled server.
#
# Three steps: stamp-key → generate → register. Sources .env, which
# pins TILED_URL / TILED_API_KEY to the deployed dev server.
#
# To target the test server instead, source .env.test before invoking
# this script — that file overrides TILED_URL / TILED_API_KEY with the
# test-server credentials. Example:
#     ( source .env.test && ./examples/register_nips3_swt_9dof.sh -n 100 )
#
# Extra args are forwarded to `tcb register` — pass `-n 100` to limit
# entities for a smoke test.
#
# Usage:
#     ./examples/register_nips3_swt_9dof.sh         # full registration on dev
#     ./examples/register_nips3_swt_9dof.sh -n 100  # first 100 entities only

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Only auto-source .env when nothing else has set TILED_URL — otherwise
# we'd clobber e.g. test-server creds the caller sourced from .env.test.
if [[ -z "${TILED_URL:-}" ]]; then
  if [[ ! -f .env ]]; then
    echo "ERROR: TILED_URL not set and .env not found in $REPO_ROOT." >&2
    echo "       Copy .env.example to .env and fill in, or source .env.test first." >&2
    exit 1
  fi
  # `set -a` exports every variable sourced from .env (the file uses bare
  # KEY=VALUE syntax, no `export`).
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

YAML="datasets/nips3_swt_9dof.yml"
if [[ ! -f $YAML ]]; then
  echo "ERROR: $YAML not found." >&2
  exit 1
fi

echo "=== Step 1: stamp-key — write derived 'key:' into $YAML ==="
uv run tcb stamp-key "$YAML"

echo
echo "=== Step 2: generate — produce entities.parquet + artifacts.parquet ==="
uv run tcb generate "$YAML"

echo
echo "=== Step 3: register against $TILED_URL ==="
uv run tcb register "$YAML" "$@"

echo
echo "=== Done ==="
echo "Probe with:"
cat <<'PROBE'
  uv run --with tiled python -c '
import os
from tiled.client import from_uri
c = from_uri(os.environ["TILED_URL"], api_key=os.environ["TILED_API_KEY"])
d = c["NIPS3_SWT_9DOF"]
print(f"entities: {len(d)}")
sample = d[list(d)[0]]
print(f"first entity artifacts: {list(sample)}")
print(f"shared axes: {[k for k in d.metadata if k.startswith(\"shared_dataset_\")]}")
'
PROBE

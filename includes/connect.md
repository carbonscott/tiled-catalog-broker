Both `tcb` and the Python client take the server URL and your API key from the
environment. Put them in a `.env` file in the directory you work from — the root of
your checkout, if you have one:

```
TILED_URL=<the server URL>
TILED_API_KEY=<your key>
```

In a checkout, `cp .env.example .env` gives you the file to fill in. `.env` is
gitignored; treat it like a password.

`tcb` reads `.env` from the working directory by itself. Nothing else does. A
notebook, a script, or a `python -c` needs the variables **exported** into its own
environment first:

```bash
set -a; source .env; set +a
```

Check that both halves worked:

```bash
python -c "
import os
from tiled.client import from_uri
print(list(from_uri(os.environ['TILED_URL'], api_key=os.environ['TILED_API_KEY'])))
"
```

A list of dataset keys means you are connected. `KeyError` means the variables are
in the file but not in this shell. `Connection refused` means the URL is wrong or
the server is down. `401` means the key is wrong or expired.

!!! tip "Pixi users"

    If that snippet fails with `SyntaxError: invalid syntax`, the quotes were
    stripped before Python saw them. Run `pixi shell` once, then run the
    `python -c ...` normally.

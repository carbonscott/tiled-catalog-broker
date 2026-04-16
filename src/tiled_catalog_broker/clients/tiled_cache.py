"""
Mode B disk-backed cache + PyTorch-compatible Dataset.

Arrays fetched via Tiled HTTP in epoch 1 are saved as .npy files.
Subsequent epochs load from disk — no repeated HTTP requests.

Cache layout:
    cache_dir/{dataset_key}/{ent_key}/{artifact_key}.npy

Usage (library):
    from tiled.client import from_uri
    from tiled_catalog_broker.clients.tiled_cache import TiledCatalogDataset

    client = from_uri("http://localhost:8005", api_key="secret")
    ds = TiledCatalogDataset(
        client=client["NiPS3_Multimodal"],
        dataset_key="NiPS3_Multimodal",
        artifact_keys=["Ax", "Az", "J1a", "J1b", "J2a", "J2b", "J3a", "J3b", "J4"],
        cache_dir="./tiled_cache",
    )
    sample = ds[0]  # {"Ax": np.ndarray, "J1a": np.ndarray, "metadata": {...}}

Usage (CLI):
    python -m tiled_catalog_broker.tiled_cache \\
        --dataset VDP \\
        --artifacts mh_powder_30T ins_12meV \\
        --epochs 3 \\
        --cache-dir ./tiled_cache

    python -m tiled_catalog_broker.tiled_cache --clear-cache --cache-dir ./tiled_cache
"""

import argparse
import shutil
import time
from pathlib import Path

import numpy as np


# ---------------------------------------------------------------------------
# Cache layer
# ---------------------------------------------------------------------------

class TiledArrayCache:
    """Disk-backed cache for arrays fetched via Tiled HTTP.

    On a cache miss the provided ``fetch_fn`` is called, the result is saved
    as a ``.npy`` file, and the array is returned.  On a cache hit the file
    is loaded from disk directly.

    Args:
        cache_dir: Root directory for ``.npy`` cache files.
    """

    def __init__(self, cache_dir):
        self.cache_dir = Path(cache_dir)
        self._hits = 0
        self._misses = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, dataset_key: str, ent_key: str, artifact_key: str, fetch_fn):
        """Return cached array or fetch, cache, and return it.

        Args:
            dataset_key: Dataset container key (e.g. ``"VDP"``).
            ent_key: Entity container key (e.g. ``"H_636ce3e4"``).
            artifact_key: Artifact key (e.g. ``"mh_powder_30T"``).
            fetch_fn: Zero-argument callable that returns a numpy-compatible
                array.  Only called on a cache miss.

        Returns:
            np.ndarray
        """
        path = self._path(dataset_key, ent_key, artifact_key)
        if path.exists():
            self._hits += 1
            return np.load(path)

        arr = np.asarray(fetch_fn(), dtype=np.float32)
        path.parent.mkdir(parents=True, exist_ok=True)
        np.save(path, arr)
        self._misses += 1
        return arr

    def clear(self):
        """Remove all cached ``.npy`` files and reset counters."""
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
        self._hits = 0
        self._misses = 0

    def reset_counters(self):
        """Reset hit/miss counters without touching files on disk."""
        self._hits = 0
        self._misses = 0

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    @property
    def hits(self) -> int:
        return self._hits

    @property
    def misses(self) -> int:
        return self._misses

    @property
    def total_requests(self) -> int:
        return self._hits + self._misses

    @property
    def hit_rate(self) -> float:
        total = self.total_requests
        return self._hits / total if total > 0 else 0.0

    def hit_rate_report(self, *, label: str = "") -> None:
        """Print a formatted cache hit-rate summary to stdout."""
        header = f"Cache Hit Rate Report{' — ' + label if label else ''}"
        sep = "=" * max(40, len(header) + 4)
        print(f"\n{sep}")
        print(f"  {header}")
        print(sep)
        print(f"  Hits:      {self._hits:>8,}")
        print(f"  Misses:    {self._misses:>8,}")
        print(f"  Total:     {self.total_requests:>8,}")
        print(f"  Hit rate:  {self.hit_rate:>8.1%}")
        if self.cache_dir.exists():
            npy_files = list(self.cache_dir.rglob("*.npy"))
            total_bytes = sum(f.stat().st_size for f in npy_files)
            print(f"  Cached files: {len(npy_files):,}")
            print(f"  Cache size:   {_fmt_bytes(total_bytes)}")
        print(sep)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _path(self, dataset_key: str, ent_key: str, artifact_key: str) -> Path:
        return self.cache_dir / dataset_key / ent_key / f"{artifact_key}.npy"


def _fmt_bytes(n: int) -> str:
    """Format byte count as a human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ---------------------------------------------------------------------------
# PyTorch-compatible Dataset
# ---------------------------------------------------------------------------

class TiledCatalogDataset:
    """PyTorch-compatible Dataset for Mode B (Tiled HTTP) access with caching.

    Fetches arrays via Tiled HTTP on first access and stores them as ``.npy``
    files under ``cache_dir``.  Subsequent ``__getitem__`` calls (e.g. in
    epoch 2+) load from disk — no HTTP round-trips.

    Implements the PyTorch ``Dataset`` interface (``__len__``, ``__getitem__``)
    without requiring ``torch`` as a hard dependency.  Items are returned as
    plain ``numpy`` arrays; wrap with a ``transform`` or a custom
    ``collate_fn`` to produce tensors.

    Args:
        client: Tiled client node for the dataset container
            (e.g. ``from_uri(url, api_key=k)["VDP"]``).
        dataset_key: Name of the dataset container — used as the top-level
            cache directory segment (e.g. ``"VDP"``).
        artifact_keys: Ordered list of artifact keys to fetch per entity
            (e.g. ``["mh_powder_30T", "ins_12meV"]``).  Artifacts missing
            from an entity are silently skipped.
        cache_dir: Root directory for ``.npy`` cache files.
            Default: ``"./tiled_cache"``.
        transform: Optional callable applied to each sample dict before it is
            returned.  Receives and must return a dict.

    Returns (per item):
        dict with:
        - one key per artifact_key whose value is a ``np.ndarray``
        - ``"metadata"``: the entity container's Tiled metadata dict
        - ``"ent_key"``: the entity key string

    Example::

        from tiled.client import from_uri
        from tiled_catalog_broker.clients.tiled_cache import TiledCatalogDataset

        client = from_uri("http://localhost:8005", api_key="secret")
        ds = TiledCatalogDataset(
            client=client["NiPS3_Multimodal"],
            dataset_key="NiPS3_Multimodal",
            artifact_keys=["Ax"],
            cache_dir="./tiled_cache",
        )

        # PyTorch DataLoader compatible
        from torch.utils.data import DataLoader
        loader = DataLoader(ds, batch_size=32, num_workers=0)
        for batch in loader:
            ...
    """

    def __init__(
        self,
        client,
        dataset_key: str,
        artifact_keys: list,
        cache_dir: str = "./tiled_cache",
        transform=None,
    ):
        self._client = client
        self.dataset_key = dataset_key
        self.artifact_keys = list(artifact_keys)
        self.cache = TiledArrayCache(cache_dir)
        self.transform = transform

        # Fetch all entity keys once.
        # Tiled's meta["count"] is incorrectly reported (~101), causing links["next"]
        # to go None after 200 items. Paginate manually using len() which is correct.
        total = len(client)
        batch = 100
        self._ent_keys = []
        offset = 0
        while offset < total:
            resp = client.context.http_client.get(
                client.item["links"]["search"],
                params={"fields": "", "page[limit]": batch, "page[offset]": offset},
            )
            page = resp.json()["data"]
            self._ent_keys.extend(item["id"] for item in page)
            offset += len(page)
            if not page:
                break

    # ------------------------------------------------------------------
    # Dataset interface
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._ent_keys)

    def __getitem__(self, idx: int) -> dict:
        ent_key = self._ent_keys[idx]
        ent_node = self._client[ent_key]
        available = set(ent_node.keys())

        sample = {
            "ent_key": ent_key,
            "metadata": dict(ent_node.metadata),
        }

        for art_key in self.artifact_keys:
            if art_key not in available:
                continue
            sample[art_key] = self.cache.get(
                self.dataset_key,
                ent_key,
                art_key,
                # Default-arg capture avoids late-binding closure bug
                fetch_fn=lambda node=ent_node, k=art_key: node[k][:],
            )

        if self.transform is not None:
            sample = self.transform(sample)

        return sample

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    def hit_rate_report(self, **kwargs) -> None:
        """Delegate to the underlying cache's hit-rate report."""
        self.cache.hit_rate_report(**kwargs)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m broker.tiled_dataset",
        description=(
            "Iterate a Tiled catalog dataset with disk-backed caching "
            "and report the cache hit rate."
        ),
    )
    p.add_argument(
        "--tiled-url",
        default=None,
        help="Tiled server URL (default: from config.yml)",
    )
    p.add_argument(
        "--api-key",
        default=None,
        help="Tiled API key (default: from config.yml)",
    )
    p.add_argument(
        "--dataset",
        default="VDP",
        metavar="KEY",
        help="Dataset container key to iterate (default: VDP)",
    )
    p.add_argument(
        "--artifacts",
        nargs="+",
        default=["mh_powder_30T"],
        metavar="KEY",
        help="Artifact keys to fetch per entity (default: mh_powder_30T)",
    )
    p.add_argument(
        "--cache-dir",
        default="./tiled_cache",
        metavar="DIR",
        help="Root directory for .npy cache files (default: ./tiled_cache)",
    )
    p.add_argument(
        "--clear-cache",
        action="store_true",
        help="Delete existing cache before running (forces fresh HTTP fetches)",
    )
    p.add_argument(
        "--epochs",
        type=int,
        default=2,
        help="Number of full passes over the dataset (default: 2)",
    )
    p.add_argument(
        "--max-entities",
        type=int,
        default=None,
        metavar="N",
        help="Limit to first N entities (useful for quick tests)",
    )
    return p


def main():
    args = _build_parser().parse_args()

    # Resolve server config
    from ..config import get_tiled_url, get_api_key
    tiled_url = args.tiled_url or get_tiled_url()
    api_key = args.api_key or get_api_key()

    # Handle --clear-cache before connecting (allows clearing without a server)
    if args.clear_cache:
        cache_path = Path(args.cache_dir)
        if cache_path.exists():
            shutil.rmtree(cache_path)
            print(f"Cache cleared: {cache_path}")
        else:
            print(f"Cache directory does not exist, nothing to clear: {cache_path}")

    # Connect to Tiled
    from tiled.client import from_uri
    print(f"\nConnecting to {tiled_url} ...")
    client = from_uri(tiled_url, api_key=api_key)
    dataset_client = client[args.dataset]
    print(f"Dataset '{args.dataset}' — {len(dataset_client)} entities")

    # Build dataset
    ds = TiledCatalogDataset(
        client=dataset_client,
        dataset_key=args.dataset,
        artifact_keys=args.artifacts,
        cache_dir=args.cache_dir,
    )

    n = len(ds) if args.max_entities is None else min(args.max_entities, len(ds))
    print(f"Artifacts: {args.artifacts}")
    print(f"Cache dir: {args.cache_dir}")
    print(f"Entities per epoch: {n}")
    print(f"Epochs: {args.epochs}")

    epoch_times = []

    for epoch in range(1, args.epochs + 1):
        ds.cache.reset_counters()
        t0 = time.perf_counter()

        for idx in range(n):
            _ = ds[idx]

        elapsed = time.perf_counter() - t0
        epoch_times.append(elapsed)
        rate = n / elapsed if elapsed > 0 else float("inf")
        print(
            f"\nEpoch {epoch}/{args.epochs} — "
            f"{n} items in {elapsed:.2f}s ({rate:.1f} items/s)"
        )
        ds.hit_rate_report(label=f"Epoch {epoch}")

    # Overall summary
    print("\n" + "=" * 50)
    print("Run Summary")
    print("=" * 50)
    for i, t in enumerate(epoch_times, 1):
        print(f"  Epoch {i}: {t:.2f}s")
    if len(epoch_times) >= 2:
        speedup = epoch_times[0] / epoch_times[-1] if epoch_times[-1] > 0 else float("inf")
        print(f"  Speedup (epoch 1 → last): {speedup:.1f}x")
    print("=" * 50)


if __name__ == "__main__":
    main()

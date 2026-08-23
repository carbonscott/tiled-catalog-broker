import argparse
from tiled_catalog_broker.clients.tiled_cache import TiledCatalogDataset
from tiled.client import from_uri
from pathlib import Path
import shutil
import time

import pytest

TILED_URL = "http://localhost:8005"
API_KEY = "secret"
DATASET_KEY = "NiPS3_Multimodal"
# Artifact types present in NiPS3_Multimodal: ma, mb, mcs, hisym, powder, powder_mask, qs
ARTIFACT_KEYS = ["ma", "mb", "powder"]
CACHE_DIR = "./test_cache"
N_ENTITIES = 200   # number of entities to test with
N_EPOCHS = 4     # number of epochs to run


@pytest.mark.integration
def test_tiled_cache(n_entities=N_ENTITIES, n_epochs=N_EPOCHS):
    """Test disk-backed cache: epoch 1 should miss, all subsequent epochs should hit 100%."""
    cache_path = Path(CACHE_DIR)
    if cache_path.exists():
        shutil.rmtree(cache_path)

    try:
        client = from_uri(TILED_URL, api_key=API_KEY)
    except Exception as e:
        pytest.skip(f"Tiled server not available at {TILED_URL}: {e}")
    dataset_client = client[DATASET_KEY]

    ds = TiledCatalogDataset(
        client=dataset_client,
        dataset_key=DATASET_KEY,
        artifact_keys=ARTIFACT_KEYS,
        cache_dir=CACHE_DIR,
    )

    n = min(n_entities, len(ds))
    print(f"\nDataset '{DATASET_KEY}' — {len(ds)} total entities, testing {n}")
    print(f"Artifacts: {ARTIFACT_KEYS}")
    print(f"Epochs: {n_epochs}")

    epoch_times = []

    for epoch in range(1, n_epochs + 1):
        ds.cache.reset_counters()
        t0 = time.perf_counter()
        for idx in range(n):
            sample = ds[idx]
            if epoch == 1:
                # Sanity check: expected artifact keys present
                for key in ARTIFACT_KEYS:
                    assert key in sample, f"Missing artifact '{key}' in sample {idx}"
        elapsed = time.perf_counter() - t0
        epoch_times.append(elapsed)
        print(f"\nEpoch {epoch}: {n} items in {elapsed:.2f}s")
        ds.hit_rate_report(label=f"Epoch {epoch}")

        if epoch == 1:
            assert ds.cache.misses == n * len(ARTIFACT_KEYS), (
                f"Expected {n * len(ARTIFACT_KEYS)} misses on epoch 1, got {ds.cache.misses}"
            )
            assert ds.cache.hits == 0, f"Expected 0 hits on epoch 1, got {ds.cache.hits}"
        else:
            assert ds.cache.hits == n * len(ARTIFACT_KEYS), (
                f"Expected {n * len(ARTIFACT_KEYS)} hits on epoch {epoch}, got {ds.cache.hits}"
            )
            assert ds.cache.misses == 0, f"Expected 0 misses on epoch {epoch}, got {ds.cache.misses}"

    if n_epochs >= 2:
        speedup = epoch_times[0] / epoch_times[1] if epoch_times[1] > 0 else float("inf")
        print(f"\nSpeedup: {speedup:.1f}x (epoch 1 → epoch 2)")
    print("All assertions passed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Integration test for TiledArrayCache.")
    parser.add_argument("--n-entities", type=int, default=N_ENTITIES, help=f"Number of entities to test (default: {N_ENTITIES})")
    parser.add_argument("--n-epochs", type=int, default=N_EPOCHS, help=f"Number of epochs to run (default: {N_EPOCHS})")
    args = parser.parse_args()
    test_tiled_cache(n_entities=args.n_entities, n_epochs=args.n_epochs)


def test_entity_keys_exclude_shared_axes():
    """Shared axes sit beside the entities as array children of the dataset container
    (announced as shared_dataset_<type> in its metadata); they are not samples."""
    from unittest.mock import MagicMock

    client = MagicMock()
    client.metadata = {"label": "DS", "shared_dataset_eloss": "/eloss"}
    client.__len__.return_value = 3
    client.item = {"links": {"search": "http://x/search"}}
    client.context.http_client.get.return_value.json.return_value = {
        "data": [{"id": "eloss"}, {"id": "DS_aaaa"}, {"id": "DS_bbbb"}]}

    ds = TiledCatalogDataset(client=client, dataset_key="DS", artifact_keys=["spectrum"],
                             cache_dir=str(Path(CACHE_DIR) / "unit"))
    assert ds._ent_keys == ["DS_aaaa", "DS_bbbb"]
    assert len(ds) == 2

#!/usr/bin/env python3
"""
Simple registration of NiPS3 test data.

Each run creates a fresh dataset with a timestamp-based key,
so you get new ChildCreated events every time.

After registration, a sample of entity nodes are metadata-updated to exercise
the container_child_metadata_updated event path.
"""

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from tiled.client import from_uri
from tiled_catalog_broker.http_register import register_dataset_http

N_METADATA_UPDATES = int(os.environ.get("N_METADATA_UPDATES", "3"))


def main():
    url = os.environ.get("TILED_URL", "http://localhost:8000")
    api_key = os.environ.get("TILED_SINGLE_USER_API_KEY", "secret")

    # Generate unique dataset key with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_key = f"NiPS3_{timestamp}"

    print(f"Connecting to {url}...")
    client = from_uri(url, api_key=api_key)
    print("✓ Connected")

    # Load manifests
    testdata_dir = Path(__file__).parent.parent.parent / "tests" / "testdata" / "nips3"
    print(f"Loading manifests from {testdata_dir}...")
    ent_df = pd.read_parquet(testdata_dir / "nips3_entities.parquet")
    art_df = pd.read_parquet(testdata_dir / "nips3_artifacts.parquet")

    # The NiPS3 test manifest reuses the same key for multiple rows.
    # Make keys unique per uid so each entity is created and emits events.
    if ent_df["key"].nunique() < len(ent_df):
        ent_df["key"] = ent_df.apply(lambda row: f"{row['key']}_{row['uid']}", axis=1)

    print(f"✓ Loaded {len(ent_df)} entities, {len(art_df)} artifacts")

    # Register to fresh dataset key
    print(f"Registering to {dataset_key}...")
    dataset_metadata = {
        "description": "NiPS3 Multimodal test data",
        "created_at": datetime.now().isoformat(),
    }

    register_dataset_http(
        client=client,
        ent_df=ent_df,
        art_df=art_df,
        base_dir=str(testdata_dir),
        label="NiPS3_Multimodal",
        dataset_key=dataset_key,
        dataset_metadata=dataset_metadata,
    )

    print(f"\n✓ Done! Registered to: {dataset_key}")

    # Trigger metadata_updated events on a sample of entity nodes
    n = min(N_METADATA_UPDATES, len(ent_df))
    if n == 0:
        return
    print(f"\nUpdating metadata on {n} entity node(s)...")
    sample_keys = ent_df["key"].iloc[:n].tolist()
    dataset_node = client[dataset_key]
    for key in sample_keys:
        entity_node = dataset_node[key]
        entity_node.update_metadata({"updated_at": datetime.now().isoformat()})
        print(f"  Updated metadata: {dataset_key}/{key}")
    print(f"✓ Triggered {n} metadata_updated event(s)")


if __name__ == "__main__":
    main()

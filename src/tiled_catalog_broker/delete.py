"""HTTP deletion of registered data via Tiled Client.

Four granularities mirroring the Dataset -> Entity -> Artifact hierarchy:

  - delete_target on a dataset node:   drops the dataset container and all entities/artifacts under it
  - delete_target on an entity node:   drops one entity container and its artifacts
  - delete_target on an artifact node: drops one artifact array
  - delete_all:                        wipes every top-level container on the server

All operations use ``external_only=True`` so only catalog pointers are
removed -- the underlying HDF5 files on disk are untouched.
"""

from __future__ import annotations

from tiled.client.utils import ClientError


def resolve_target(client, dataset, entity=None, artifact=None):
    """Walk the catalog tree and return the node the user is targeting.

    Args:
        client:   Tiled root client.
        dataset:  Top-level dataset key (required).
        entity:   Entity key within the dataset (optional).
        artifact: Artifact key within the entity (optional).

    Returns:
        (node, path_str, granularity) where granularity is one of
        "dataset", "entity", or "artifact".

    Raises:
        KeyError: if any segment is missing on the server.
    """
    if dataset not in client:
        raise KeyError(f"No such dataset: '{dataset}'")
    node = client[dataset]
    path = dataset
    granularity = "dataset"

    if entity is not None:
        if entity not in node:
            raise KeyError(f"No such entity: '{entity}' in dataset '{dataset}'")
        node = node[entity]
        path = f"{path}/{entity}"
        granularity = "entity"

    if artifact is not None:
        if artifact not in node:
            raise KeyError(
                f"No such artifact: '{artifact}' in entity '{dataset}/{entity}'"
            )
        node = node[artifact]
        path = f"{path}/{artifact}"
        granularity = "artifact"

    return node, path, granularity


def preview_counts(node, granularity):
    """Return a small dict of counts/samples for the preview block.

    Args:
        node:        The target node (from resolve_target).
        granularity: "dataset", "entity", "artifact", or "all".

    Returns:
        dict with "n_children" for container granularities; "sample_keys"
        for the "all" granularity (first 10 top-level keys).
    """
    if granularity == "artifact":
        return {"n_children": 0}
    if granularity == "all":
        keys = list(node)
        return {"n_children": len(keys), "sample_keys": keys[:10]}
    return {"n_children": len(node)}


def delete_target(node, *, external_only=True):
    """Delete a single node (dataset / entity / artifact) and its descendants."""
    node.delete(recursive=True, external_only=external_only)


def delete_all(client, *, external_only=True):
    """Iterate every top-level container and delete each in turn.

    Partial failures are collected rather than raised so the caller can
    report per-key status. Tiled HTTP has no transactional semantics, so
    there is no rollback to attempt.

    Returns:
        (successful_keys, failures) where failures is a list of
        (key, error_message) tuples.
    """
    successes = []
    failures = []
    for k in list(client):
        try:
            client[k].delete(recursive=True, external_only=external_only)
            successes.append(k)
        except ClientError as e:
            failures.append((k, str(e)))
    return successes, failures

"""Unit tests for registration helper functions.

Covers:
- cli._build_dataset_metadata: tolerates an empty/None provenance block.
- http_register.verify_registration_http: walks the full three-level
  hierarchy (root -> dataset -> entity -> artifact) without crashing.

Run with: uv run --with pytest pytest tests/test_registration_helpers.py -v
"""

from unittest.mock import MagicMock

from tiled_catalog_broker.cli import _build_dataset_metadata
from tiled_catalog_broker.http_register import verify_registration_http


# ---------- _build_dataset_metadata: provenance None guard ----------

def test_build_dataset_metadata_provenance_none():
    """An all-commented `provenance:` block parses as None — must not crash."""
    config = {"metadata": {"label": "X"}, "provenance": None}
    result = _build_dataset_metadata(config, "X")
    assert result == {"label": "X"}


def test_build_dataset_metadata_provenance_missing():
    """Missing `provenance:` key is also fine."""
    config = {"metadata": {"label": "X"}}
    result = _build_dataset_metadata(config, "X")
    assert result == {"label": "X"}


def test_build_dataset_metadata_provenance_populated():
    """Populated provenance is merged into the metadata dict."""
    config = {
        "metadata": {"label": "X"},
        "provenance": {"created_at": "2026-04-28", "code_version": "1.2.3"},
    }
    result = _build_dataset_metadata(config, "X")
    assert result == {
        "label": "X",
        "created_at": "2026-04-28",
        "code_version": "1.2.3",
    }


# ---------- verify_registration_http: walks the full hierarchy ----------

def _make_artifact(shape=(10, 20), dtype="float32"):
    art = MagicMock()
    art.shape = shape
    art.dtype = dtype
    return art


def _make_entity(art_keys=("array_0",), path_locators=("path_array_0",)):
    ent = MagicMock()
    ent.metadata = {k: f"/data/{k}.h5" for k in path_locators}
    ent.keys.return_value = list(art_keys)
    # The walk only touches index 0, so per-key dispatch isn't exercised;
    # any subscript yields the same artifact mock.
    ent.__getitem__.side_effect = lambda k: _make_artifact()
    return ent


def _make_dataset(ent_keys=("E_0",), with_entity=True):
    ds = MagicMock()
    ds.metadata = {"label": "DS", "material": "GaAs"}
    ds.keys.return_value = list(ent_keys)
    ds.__getitem__.side_effect = lambda k: _make_entity() if with_entity else None
    return ds


def _make_root(ds_keys=("DS",)):
    root = MagicMock()
    root.keys.return_value = list(ds_keys)
    root.__getitem__.side_effect = lambda k: _make_dataset()
    return root


def test_verify_walks_full_hierarchy(capsys):
    """Root -> dataset -> entity -> artifact. All four levels reached."""
    verify_registration_http(_make_root())
    out = capsys.readouterr().out
    assert "Dataset containers at root: 1" in out
    assert "Dataset 'DS'" in out
    assert "entity containers: 1" in out
    assert "Entity 'E_0'" in out
    assert "artifact children: 1" in out
    assert "Artifact 'array_0'" in out
    assert "shape: (10, 20)" in out


def test_verify_empty_root(capsys):
    """No datasets registered: clean early return, no crash."""
    root = MagicMock()
    root.keys.return_value = []
    verify_registration_http(root)
    out = capsys.readouterr().out
    assert "No datasets registered yet." in out


def test_verify_entity_with_no_artifacts(capsys):
    """Entity has no array children: warns about Mode B failure."""
    ent = MagicMock()
    ent.metadata = {"path_x": "/data/x.h5"}
    ent.keys.return_value = []
    ds = MagicMock()
    ds.metadata = {}
    ds.keys.return_value = ["E_0"]
    ds.__getitem__.side_effect = lambda k: ent
    root = MagicMock()
    root.keys.return_value = ["DS"]
    root.__getitem__.side_effect = lambda k: ds
    verify_registration_http(root)
    out = capsys.readouterr().out
    assert "WARNING: no array children" in out

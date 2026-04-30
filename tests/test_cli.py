"""Unit tests for CLI helpers — _require_key and stamp_key_main.

Run with: uv run --with pytest pytest tests/test_cli.py -v
"""

import sys
from unittest.mock import patch

import pytest

from tiled_catalog_broker.cli import _require_key, stamp_key_main


class TestRequireKey:
    """Read-only validator: errors and exits if key is missing or drifted."""

    def test_valid_key(self):
        config = {"label": "Broad Sigma", "key": "BROAD_SIGMA"}
        assert _require_key(config, "test.yml") == "BROAD_SIGMA"

    def test_missing_key_exits_with_hint(self, capsys):
        config = {"label": "Broad Sigma"}
        with pytest.raises(SystemExit) as exc:
            _require_key(config, "test.yml")
        assert exc.value.code == 1
        err = capsys.readouterr().err
        assert "missing 'key'" in err
        assert "tcb stamp-key" in err

    def test_drifted_key_exits(self, capsys):
        config = {"label": "Broad Sigma", "key": "OLD_NAME"}
        with pytest.raises(SystemExit) as exc:
            _require_key(config, "test.yml")
        assert exc.value.code == 1
        assert "does not match" in capsys.readouterr().err

    def test_missing_label_exits(self):
        with pytest.raises(SystemExit) as exc:
            _require_key({}, "test.yml")
        assert exc.value.code == 1


class TestStampKeyMain:
    """End-to-end tests for `tcb stamp-key`."""

    def _run(self, *args):
        with patch.object(sys, "argv", ["tcb stamp-key", *args]):
            stamp_key_main()

    def test_stamps_fresh_yaml(self, tmp_path):
        yaml_path = tmp_path / "fresh.yml"
        yaml_path.write_text('label: "Broad Sigma"\n')
        self._run(str(yaml_path))
        content = yaml_path.read_text()
        assert "key: BROAD_SIGMA" in content
        assert "Broad Sigma" in content  # label preserved

    def test_idempotent_on_correct_key(self, tmp_path, capsys):
        yaml_path = tmp_path / "stamped.yml"
        yaml_path.write_text('key: BROAD_SIGMA\nlabel: "Broad Sigma"\n')
        before = yaml_path.read_text()
        self._run(str(yaml_path))
        assert "already correct" in capsys.readouterr().out
        assert yaml_path.read_text() == before

    def test_drifted_key_exits(self, tmp_path, capsys):
        yaml_path = tmp_path / "drift.yml"
        yaml_path.write_text('key: OLD_NAME\nlabel: "Broad Sigma"\n')
        with pytest.raises(SystemExit) as exc:
            self._run(str(yaml_path))
        assert exc.value.code == 1
        assert "does not match" in capsys.readouterr().err

    def test_missing_file_exits(self, tmp_path):
        with pytest.raises(SystemExit) as exc:
            self._run(str(tmp_path / "nope.yml"))
        assert exc.value.code == 1

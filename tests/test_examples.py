"""The annotated example dataset YAMLs are part of the contract surface.

Each `datasets/examples/<layout>.yml` must parse and validate cleanly against the
contract (`tcb generate` runs the same `validate()`), and must use canonical vocabulary
so it produces zero warnings — they are the templates producers copy.
"""

from pathlib import Path

import pytest
from ruamel.yaml import YAML

from tiled_catalog_broker.tools.schema import validate

EXAMPLES_DIR = Path(__file__).parent.parent / "datasets" / "examples"
EXAMPLE_FILES = sorted(EXAMPLES_DIR.glob("*.yml"))


def test_examples_exist():
    """One example per layout (per_entity, batched, grouped)."""
    names = {p.stem for p in EXAMPLE_FILES}
    assert {"per_entity", "batched", "grouped"} <= names


@pytest.mark.parametrize("path", EXAMPLE_FILES, ids=lambda p: p.stem)
def test_example_validates_with_no_warnings(path):
    """Each example validates against the contract and uses canonical vocab."""
    yaml = YAML()
    with open(path) as f:
        cfg = yaml.load(f)

    warnings = validate(cfg)  # raises pydantic ValidationError on a contract violation
    assert warnings == [], f"{path.name} produced vocab warnings: {warnings}"


@pytest.mark.parametrize("path", EXAMPLE_FILES, ids=lambda p: p.stem)
def test_example_layout_matches_filename(path):
    """The example's `data.layout` matches its filename (per_entity.yml -> per_entity)."""
    yaml = YAML()
    with open(path) as f:
        cfg = yaml.load(f)
    assert cfg["data"]["layout"] == path.stem

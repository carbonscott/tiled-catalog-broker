"""
Onboarding pipeline for registering new HDF5 datasets.

Provides three capabilities:
  - inspect: Auto-scan HDF5 directories, detect layout, emit draft YAML config
  - generate: Produce Parquet manifests from finalized YAML configs
  - schema: Validate YAML configs against a controlled vocabulary

Typical workflow:
    from broker.onboarding.inspect import inspect_directory, emit_draft_yaml
    from broker.onboarding.generate import generate_manifests
    from broker.onboarding.schema import validate

    result = inspect_directory("/path/to/hdf5/data")
    emit_draft_yaml(result, "datasets/draft.yml")
    # ... user edits draft.yml ...
    warnings = validate(config)
    ent_path, art_path = generate_manifests("datasets/mydata.yml")
"""

from .inspect import inspect_directory, emit_draft_yaml
from .generate import generate_manifests
from .schema import validate, ValidationError

__all__ = [
    "inspect_directory",
    "emit_draft_yaml",
    "generate_manifests",
    "validate",
    "ValidationError",
]

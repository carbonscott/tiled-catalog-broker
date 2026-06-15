"""YAML contract schema validation for dataset configs.

Validates dataset YAML configs against both structural requirements
and the semantic model (schema/catalog_model.yml).
"""

import os
from pathlib import Path

from pydantic import ValidationError as PydanticValidationError
from ruamel.yaml import YAML

from ._models import DatasetConfig


class ValidationError(Exception):
    """Raised when a dataset YAML fails validation."""

    def __init__(self, errors):
        self.errors = errors
        super().__init__(
            f"{len(errors)} validation error(s):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )


def load_catalog_model(model_path=None):
    """Load the semantic model YAML.

    Args:
        model_path: Path to catalog_model.yml.
            Defaults to schema/catalog_model.yml relative to the package.

    Returns:
        dict: The parsed catalog model, or None if not found.
    """
    if model_path is None:
        model_path = (
            Path(__file__).parent / "schema" / "catalog_model.yml"
        )
    if not Path(model_path).exists():
        return None

    yaml = YAML()
    with open(model_path) as f:
        return yaml.load(f)


def get_allowed_values(model, field_name):
    """Extract allowed IDs for a vocabulary field from the catalog model.

    Args:
        model: Parsed catalog model dict.
        field_name: Key in the model (e.g., "methods", "materials").

    Returns:
        list[str]: Allowed ID values, or empty list if not found.
    """
    if model is None or field_name not in model:
        return []
    return [entry["id"] for entry in model[field_name]]


def get_alias_map(model, field_name):
    """Build a mapping from alias IDs to (canonical_id, implies_dict).

    Scans the vocabulary entries for 'aliases' fields and returns a dict
    that maps each alias to the canonical ID and any implied field values.

    Args:
        model: Parsed catalog model dict.
        field_name: Key in the model (e.g., "methods", "materials").

    Returns:
        dict: {alias_id: {"canonical": canonical_id, "implies": {...}}}
    """
    if model is None or field_name not in model:
        return {}
    alias_map = {}
    for entry in model[field_name]:
        for alias in entry.get("aliases", []):
            if isinstance(alias, dict):
                alias_map[alias["id"]] = {
                    "canonical": entry["id"],
                    "implies": alias.get("implies", {}),
                }
            else:
                # Simple string alias (e.g., materials aliases: [NIPS, nips3])
                alias_map[alias] = {
                    "canonical": entry["id"],
                    "implies": {},
                }
    return alias_map


def resolve_aliases(cfg, model):
    """Resolve any alias values in metadata to their canonical IDs.

    Modifies cfg["metadata"] in place. Returns a list of resolution
    messages (informational, not warnings).

    Args:
        cfg: Parsed dataset config dict.
        model: Parsed catalog model dict.

    Returns:
        list[str]: Messages about resolved aliases.
    """
    if model is None:
        return []
    messages = []
    metadata = cfg.get("metadata", {})

    # Resolve method aliases
    method_aliases = get_alias_map(model, "methods")
    methods = metadata.get("method", [])
    if isinstance(methods, list):
        resolved = []
        for m in methods:
            if m in method_aliases:
                info = method_aliases[m]
                resolved.append(info["canonical"])
                messages.append(
                    f"Resolved alias '{m}' → '{info['canonical']}'"
                )
                # Apply implied fields (e.g., data_type: simulation)
                for k, v in info.get("implies", {}).items():
                    if not metadata.get(k):
                        metadata[k] = v
                        messages.append(
                            f"  implied {k}={v} from alias '{m}'"
                        )
            else:
                resolved.append(m)
        metadata["method"] = resolved

    # Resolve material aliases
    mat_aliases = get_alias_map(model, "materials")
    mat = metadata.get("material")
    if mat and mat in mat_aliases:
        info = mat_aliases[mat]
        metadata["material"] = info["canonical"]
        messages.append(f"Resolved material alias '{mat}' → '{info['canonical']}'")

    return messages


def _format_model_errors(exc):
    """Translate a pydantic ValidationError into the broker's flat error strings.

    Each pydantic error becomes a "<dotted.location>: <message>" line so the existing
    ``ValidationError(errors)`` message format (and substring-based tests) keep working.
    """
    messages = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"])
        msg = err["msg"].removeprefix("Value error, ")
        messages.append(f"{loc}: {msg}" if loc else msg)
    return messages


def validate(cfg, model_path=None):
    """Validate a parsed dataset YAML config.

    Args:
        cfg: dict loaded from YAML.
        model_path: Optional path to catalog_model.yml.

    Returns:
        list of warning strings (non-fatal).

    Raises:
        ValidationError: if required fields are missing or invalid.
    """
    warnings = []
    model = load_catalog_model(model_path)

    # Resolve aliases before validation (mutates cfg in place).
    warnings.extend(resolve_aliases(cfg, model))

    # Structural validation via the pydantic contract model. Raise immediately —
    # the filesystem/vocab checks below operate on the validated object.
    # try/except is required here: pydantic raises a single ValidationError for all
    # structural problems, which we translate into the broker's flat-list format.
    try:
        config = DatasetConfig.model_validate(cfg)
    except PydanticValidationError as e:
        raise ValidationError(_format_model_errors(e))

    # Filesystem check (kept out of the pure model so a config can be validated
    # without its data present — but enforced here when the directory is given).
    if not os.path.isdir(config.data.directory):
        raise ValidationError(
            [f"'data.directory' does not exist: {config.data.directory}"]
        )
    if not config.data.file_pattern:
        warnings.append("'data.file_pattern' not set — will default to '**/*.h5'")

    metadata = config.metadata
    if model:
        _validate_vocab(metadata, "method", "methods", model, warnings, is_list=True)
        _validate_vocab(metadata, "data_type", "data_types", model, warnings)
        _validate_vocab(metadata, "material", "materials", model, warnings)
        _validate_vocab(metadata, "producer", "producers", model, warnings)
        _validate_vocab(metadata, "facility", "facilities", model, warnings)
        _validate_vocab(metadata, "project", "projects", model, warnings)

    # Cross-field advisory checks (producer↔simulation, facility↔experimental).
    dt = metadata.data_type
    if dt == "experimental" and not metadata.facility:
        warnings.append("data_type is 'experimental' but no 'facility' specified")
    if dt == "simulation" and not metadata.producer:
        warnings.append("data_type is 'simulation' but no 'producer' specified")
    if dt == "experimental" and metadata.producer:
        warnings.append(
            "data_type is 'experimental' but 'producer' is set"
            " — producer is typically for simulations"
        )
    if dt == "simulation" and metadata.facility:
        warnings.append(
            "data_type is 'simulation' but 'facility' is set"
            " — facility is typically for experiments"
        )

    return warnings


def _validate_vocab(metadata, field, model_key, model, warnings, is_list=False):
    """Check a metadata field against the catalog model vocabulary.

    `metadata` is a validated DatasetMetadata model. Accepts both canonical IDs
    and known aliases.
    """
    value = getattr(metadata, field, None)
    if value is None:
        return
    allowed = get_allowed_values(model, model_key)
    aliases = get_alias_map(model, model_key)
    if not allowed:
        return
    all_accepted = set(allowed) | set(aliases.keys())
    values = value if is_list and isinstance(value, list) else [value]
    for v in values:
        if v not in all_accepted:
            warnings.append(
                f"metadata.{field} '{v}' not in catalog model — allowed: {allowed}"
            )

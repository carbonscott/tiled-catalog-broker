"""YAML contract schema validation for dataset configs.

Validates dataset YAML configs against both structural requirements
and the semantic model (schema/catalog_model.yml).
"""

import os
from pathlib import Path

from ruamel.yaml import YAML

VALID_LAYOUTS = {"per_entity", "batched", "grouped"}
VALID_PARAM_LOCATIONS = {"root_scalars", "group", "group_scalars", "manifest"}


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
            Path(__file__).parent.parent / "schema" / "catalog_model.yml"
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
    errors = []
    warnings = []
    model = load_catalog_model(model_path)

    # --- Resolve aliases before validation ---
    alias_messages = resolve_aliases(cfg, model)
    for msg in alias_messages:
        warnings.append(msg)

    # --- Required identity fields ---
    if not cfg.get("label"):
        errors.append("'label' is required (e.g., edrixs_sbi)")
    if not cfg.get("key"):
        if not cfg.get("key_prefix"):
            errors.append("'key' is required (dataset container key in Tiled)")

    # --- Data section ---
    data = cfg.get("data")
    if not data:
        errors.append("'data' section is required")
    else:
        if not data.get("directory"):
            errors.append("'data.directory' is required")
        elif not os.path.isdir(data["directory"]):
            errors.append(f"'data.directory' does not exist: {data['directory']}")

        layout = data.get("layout")
        if not layout:
            errors.append("'data.layout' is required (per_entity | batched | grouped)")
        elif layout not in VALID_LAYOUTS:
            errors.append(f"'data.layout' must be one of {VALID_LAYOUTS}, got '{layout}'")

        if not data.get("file_pattern"):
            warnings.append("'data.file_pattern' not set — will default to '**/*.h5'")

    # --- Artifacts ---
    artifacts = cfg.get("artifacts", [])
    if not artifacts:
        errors.append("'artifacts' list is required (at least one artifact)")
    else:
        for i, art in enumerate(artifacts):
            if not art.get("type"):
                errors.append(f"artifacts[{i}].type is required")
            if not art.get("dataset"):
                errors.append(f"artifacts[{i}].dataset is required")

    # --- Parameters (optional but validated if present) ---
    params = cfg.get("parameters")
    if params:
        loc = params.get("location")
        if loc and loc not in VALID_PARAM_LOCATIONS:
            errors.append(
                f"'parameters.location' must be one of {VALID_PARAM_LOCATIONS}, got '{loc}'"
            )
        if loc == "group" and not params.get("group"):
            errors.append("'parameters.group' is required when location is 'group'")
        if loc == "manifest" and not params.get("manifest"):
            errors.append("'parameters.manifest' is required when location is 'manifest'")

    # --- Shared axes (optional, validated if present) ---
    for i, ax in enumerate(cfg.get("shared", [])):
        if not ax.get("type"):
            errors.append(f"shared[{i}].type is required")
        if not ax.get("dataset"):
            errors.append(f"shared[{i}].dataset is required")

    # --- Provenance (optional, no special validation) ---

    # --- Dataset container metadata: validate against semantic model ---
    metadata = cfg.get("metadata", {})
    if model:
        _validate_vocab(metadata, "method", "methods", model, warnings, is_list=True)
        _validate_vocab(metadata, "data_type", "data_types", model, warnings)
        _validate_vocab(metadata, "material", "materials", model, warnings)
        _validate_vocab(metadata, "producer", "producers", model, warnings)
        _validate_vocab(metadata, "facility", "facilities", model, warnings)
        _validate_vocab(metadata, "project", "projects", model, warnings)

    # --- Cross-field validation ---
    dt = metadata.get("data_type")
    if dt == "experimental" and not metadata.get("facility"):
        warnings.append("data_type is 'experimental' but no 'facility' specified")
    if dt == "simulation" and not metadata.get("producer"):
        warnings.append("data_type is 'simulation' but no 'producer' specified")
    if dt == "experimental" and metadata.get("producer"):
        warnings.append(
            "data_type is 'experimental' but 'producer' is set"
            " — producer is typically for simulations"
        )
    if dt == "simulation" and metadata.get("facility"):
        warnings.append(
            "data_type is 'simulation' but 'facility' is set"
            " — facility is typically for experiments"
        )
    if not metadata.get("material"):
        warnings.append("'material' not specified — recommended for discoverability")

    if errors:
        raise ValidationError(errors)

    return warnings


def _validate_vocab(metadata, field, model_key, model, warnings, is_list=False):
    """Check a metadata field against the catalog model vocabulary.

    Accepts both canonical IDs and known aliases.
    """
    value = metadata.get(field)
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

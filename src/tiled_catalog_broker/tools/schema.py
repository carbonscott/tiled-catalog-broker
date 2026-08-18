"""YAML contract schema validation for dataset configs.

Validates dataset YAML configs against both structural requirements
and the semantic model (schema/catalog_model.yml).
"""

from pathlib import Path

from ruamel.yaml import YAML

from ._models import DatasetConfig


def load_catalog_model():
    """Load and parse the bundled semantic model (schema/catalog_model.yml)."""
    path = Path(__file__).parent / "schema" / "catalog_model.yml"
    yaml = YAML()
    with open(path) as f:
        return yaml.load(f)


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
    if field_name not in model:
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


def validate(cfg):
    """Validate a parsed dataset YAML config.

    Returns a list of non-fatal warning strings. Raises pydantic ``ValidationError``
    if the config violates the structural contract.
    """
    warnings = []
    model = load_catalog_model()

    # Resolve aliases before validation (mutates cfg in place).
    warnings.extend(resolve_aliases(cfg, model))

    # Structural validation — pydantic raises ValidationError on any violation.
    config = DatasetConfig.model_validate(cfg)

    metadata = config.metadata
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
    allowed = [entry["id"] for entry in model.get(model_key, [])]
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

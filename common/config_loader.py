import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_INFRA_CONFIG_PATH = Path(__file__).parent / "config" / "infrastructure.json"


def _resolve_ref(ref: str, infra: dict) -> str:
    """Resolve a $-prefixed dot-notation reference against the infrastructure config.

    Example: "$buckets.legacy_raw.name" -> "gilead-pdm-{env}-us-west-2-raw"
    """
    keys = ref[1:].split(".")
    node = infra
    for key in keys:
        if not isinstance(node, dict) or key not in node:
            raise KeyError(f"Config reference '{ref}' could not be resolved in infrastructure config")
        node = node[key]
    if not isinstance(node, str):
        raise TypeError(f"Config reference '{ref}' resolved to a non-string value: {node!r}")
    return node


def load_config(project_config_path: str) -> dict:
    """Load a project config file and resolve $-references against the shared infrastructure config.

    Any string value starting with '$' is treated as a dot-notation reference into
    common/config/infrastructure.json.  All other values are passed through unchanged.

    Args:
        project_config_path: Path to the project-specific JSON config file.

    Returns:
        Fully resolved config dict ready for use in notebooks or modules.

    Example:
        config = load_config("/path/to/project/config/raw.json")
        bucket = config["src_bkt"]  # "gilead-pdm-dev-us-west-2-raw"
    """
    with open(_INFRA_CONFIG_PATH) as f:
        infra = json.load(f)
    logger.debug(f"Loaded infrastructure config from {_INFRA_CONFIG_PATH}")

    with open(project_config_path) as f:
        config = json.load(f)
    logger.debug(f"Loaded project config from {project_config_path}")

    resolved = {}
    for key, value in config.items():
        if isinstance(value, str) and value.startswith("$"):
            resolved[key] = _resolve_ref(value, infra)
            logger.debug(f"Resolved '{key}': '{value}' -> '{resolved[key]}'")
        else:
            resolved[key] = value

    return resolved

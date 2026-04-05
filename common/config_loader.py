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

    Merging is applied in three layers, with each layer overriding the previous:
      1. common/config/infrastructure.json  — repo-level bucket definitions (used for $ resolution)
      2. config/base.json                   — project-level defaults (if present alongside the config file)
      3. config/<notebook>.json             — notebook-specific values and overrides

    Any string value starting with '$' is treated as a dot-notation reference into
    infrastructure.json and is resolved after merging.

    Args:
        project_config_path: Path to the notebook-specific JSON config file.

    Returns:
        Fully resolved config dict ready for use in notebooks or modules.

    Example:
        config = load_config("/path/to/project/config/raw.json")
        mount  = config["src_bkt_mount_point"]  # "/mnt/gilead-pdm-raw"
    """
    with open(_INFRA_CONFIG_PATH) as f:
        infra = json.load(f)
    logger.debug(f"Loaded infrastructure config from {_INFRA_CONFIG_PATH}")

    # Layer 2: project base config (optional)
    config_dir = Path(project_config_path).parent
    base_path = config_dir / "base.json"
    merged = {}
    if base_path.exists():
        with open(base_path) as f:
            merged = json.load(f)
        logger.debug(f"Loaded base config from {base_path}")

    # Layer 3: notebook-specific config (overrides base)
    with open(project_config_path) as f:
        specific = json.load(f)
    logger.debug(f"Loaded project config from {project_config_path}")
    merged.update(specific)

    # Resolve $-references in the merged result
    resolved = {}
    for key, value in merged.items():
        if isinstance(value, str) and value.startswith("$"):
            resolved[key] = _resolve_ref(value, infra)
            logger.debug(f"Resolved '{key}': '{value}' -> '{resolved[key]}'")
        else:
            resolved[key] = value

    return resolved

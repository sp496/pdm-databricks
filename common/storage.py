import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_INFRA_CONFIG_PATH = Path(__file__).parent / "config" / "infrastructure.json"


def ensure_mount(dbutils, mount_point: str, bucket_name: str) -> None:
    """Mount an S3 bucket at the given mount point if not already mounted.

    Args:
        dbutils: Databricks dbutils object (passed from the calling notebook).
        mount_point: DBFS mount point, e.g. "/mnt/pdm-gsc-bi".
        bucket_name: S3 bucket name, e.g. "gilead-edp-pdm-dev-us-west-2-pdm-gsc-bi".
    """
    if not any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
        dbutils.fs.mount(source=f"s3a://{bucket_name}", mount_point=mount_point)
        logger.info(f"Mounted s3a://{bucket_name} at {mount_point}")
    else:
        logger.info(f"Mount already exists: {mount_point}")


def mount_all(dbutils, env: str) -> None:
    """Mount all S3 buckets defined in common/config/infrastructure.json.

    Reads the shared infrastructure config, resolves the {env} placeholder in
    each bucket name, and calls ensure_mount() for every bucket. Safe to call
    multiple times — already-mounted buckets are skipped.

    Intended to be called once at the start of the pipeline from a setup notebook,
    so individual pipeline notebooks do not need to manage mounts themselves.

    Args:
        dbutils: Databricks dbutils object (passed from the calling notebook).
        env: Environment name, e.g. "dev", "staging", "prd".
    """
    with open(_INFRA_CONFIG_PATH) as f:
        infra = json.load(f)

    for bucket_key, bucket_cfg in infra["buckets"].items():
        bucket_name = bucket_cfg["name"].format(env=env)
        mount_point = bucket_cfg["mount"]
        ensure_mount(dbutils, mount_point, bucket_name)
        logger.info(f"Bucket '{bucket_key}' ready at {mount_point}")


def unmount(dbutils, mount_point: str) -> None:
    """Unmount an S3 bucket if currently mounted.

    Args:
        dbutils: Databricks dbutils object (passed from the calling notebook).
        mount_point: DBFS mount point to unmount, e.g. "/mnt/pdm-gsc-bi".
    """
    if any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
        dbutils.fs.unmount(mount_point)
        logger.info(f"Unmounted {mount_point}")
    else:
        logger.info(f"Mount point not found, nothing to unmount: {mount_point}")

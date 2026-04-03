import logging

logger = logging.getLogger(__name__)


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

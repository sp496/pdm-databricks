"""DBFS path utilities for Databricks notebooks."""


def dbfs_path(path):
    """Convert a dbfs: or mount path to its FUSE-accessible /dbfs equivalent for pandas I/O.

    Examples:
        "dbfs:/mnt/bucket/file.csv"  → "/dbfs/mnt/bucket/file.csv"
        "/mnt/bucket/file.csv"       → "/dbfs/mnt/bucket/file.csv"
        "/dbfs/mnt/bucket/file.csv"  → "/dbfs/mnt/bucket/file.csv"
    """
    if path.startswith("dbfs:"):
        return path.replace("dbfs:", "/dbfs", 1)
    return path if path.startswith("/dbfs") else f"/dbfs{path}"

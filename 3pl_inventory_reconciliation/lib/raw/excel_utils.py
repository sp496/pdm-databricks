"""Excel cleaning utilities.

NOTE: These functions need to be ported from the legacy `cmo_inventory_utils`
notebook in the prior project. They are declared here as the contract used by
`excel_processor.py`. Replace the stub bodies with the real implementations.
"""
import pandas as pd


def find_table_boundaries(df, cmo_id, cmo_column_dict):
    """Locate the header row and data block within a sparsely populated sheet.

    Returns dict {"header": list_of_columns, "data": DataFrame} or None if not found.
    """
    raise NotImplementedError("Port from legacy cmo_inventory_utils.find_table_boundaries")


def remove_rows_with_n_values(df, n=2):
    """Drop rows whose count of non-null values is below `n`."""
    raise NotImplementedError("Port from legacy cmo_inventory_utils.remove_rows_with_n_values")


def extract_first_dataframe(df):
    """When a sheet contains multiple disjoint tables, keep only the first."""
    raise NotImplementedError("Port from legacy cmo_inventory_utils.extract_first_dataframe")


def trim_rows_and_cols(df):
    """Trim leading/trailing fully-empty rows and columns."""
    raise NotImplementedError("Port from legacy cmo_inventory_utils.trim_rows_and_cols")


def remove_aggregate_rows(df):
    """Drop summary/total rows (e.g. rows with 'Total' in a key column)."""
    raise NotImplementedError("Port from legacy cmo_inventory_utils.remove_aggregate_rows")


def remove_special_characters(df):
    """Strip control characters, non-breaking spaces, etc. from string cells."""
    raise NotImplementedError("Port from legacy cmo_inventory_utils.remove_special_characters")

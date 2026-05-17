"""Transformation functions for the 3PL curated processing layer.

Column name conventions (post data_cache _COL_CLEAN_PATTERN cleaning):
    3PL                  — site/plant ID column from the mapping Excel
    3PL_Column_Header    — 3PL column name column from the mapping Excel
    Sheet_Name           — sheet name column from the mapping Excel
    Gilead_Column_Header — Gilead target column name (trailing space cleaned to _)
"""
import re
import os
import numpy as np
import pandas as pd
from pathlib import Path

from .data_cache import MappingDataCache

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def get_site_id(file_path: str) -> str:
    """Return the site/3PL ID from the file's parent directory name."""
    return Path(file_path).parts[-2]


# ---------------------------------------------------------------------------
# Metadata enrichment
# ---------------------------------------------------------------------------

def add_3pl_details(df: pd.DataFrame, site_id: str, plant_name_mapping_df: pd.DataFrame, pl_type_mapping_df: pd.DataFrame) -> pd.DataFrame:
    df["3PL"] = site_id

    pl_type_dict = pl_type_mapping_df.set_index("3PL")["3PL_Type"].to_dict()
    df["3PL_Type"] = pl_type_dict.get(site_id)

    plant_name_dict = plant_name_mapping_df.set_index("plant_number")["plant_name"].to_dict()
    df["3PL_Name"] = plant_name_dict.get(site_id)

    return df


def add_metadata(df: pd.DataFrame, file_path: str) -> pd.DataFrame:
    path_obj = Path(file_path)
    df["File_Name"] = os.path.basename(file_path)
    df["Date_Processed"] = pd.Timestamp.today().strftime("%Y-%m-%d")
    df["Year"] = path_obj.parts[-5]
    df["Quarter"] = path_obj.parts[-4]
    return df


# ---------------------------------------------------------------------------
# Header mapping
# ---------------------------------------------------------------------------

def postprocess_mapped_df(df: pd.DataFrame) -> pd.DataFrame:
    df["3PL_Material_Code"] = df["3PL_Material_Code"].str.replace(r"\.0$", "", regex=True)
    if not pd.api.types.is_numeric_dtype(df["3PL_Quantity"]):
        df["3PL_Quantity"] = pd.to_numeric(
            df["3PL_Quantity"].str.replace(r"[^\d\.\-]", "", regex=True), errors="coerce"
        )
    df["3PL_Quantity"] = df["3PL_Quantity"].astype(float)
    materials_with_batch = df[df["3PL_Batch_Number"].notna()]["3PL_Material_Code"].unique()
    df = df[df["3PL_Batch_Number"].notna() | ~df["3PL_Material_Code"].isin(materials_with_batch)]
    return df


def map_3pl_df(df: pd.DataFrame, site_id: str, file_stem: str, header_mapping: dict) -> pd.DataFrame:
    # Try sheet-aware key first (e.g. "1205_gilead_api_inventory_by_weight"),
    # then fall back to the plain site key (e.g. "1205") for single-sheet files.
    sheet_key = f"{site_id}_{file_stem}"
    column_mapping = header_mapping.get(sheet_key) or header_mapping.get(site_id)
    if column_mapping is None:
        raise KeyError(
            f"No header mapping found for site '{site_id}' "
            f"(tried '{sheet_key}' and '{site_id}' — file stem: '{file_stem}')"
        )

    complex_mappings = {}
    simple_mapping = {}
    for source_col, target_col in column_mapping.items():
        if "," in source_col:
            complex_mappings[target_col] = [c.strip() for c in source_col.split(",")]
        elif "+" in source_col:
            complex_mappings[target_col] = [c.strip() for c in source_col.split("+")]
        else:
            simple_mapping[source_col] = target_col

    df = df.rename(columns=simple_mapping)

    for target_col, source_cols in complex_mappings.items():
        if target_col == "3PL_Quantity":
            for col in source_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(
                        df[col].str.replace(r"[^\d\.\-]", "", regex=True), errors="coerce"
                    )
            valid_cols = [c for c in source_cols if c in df.columns]
            df[target_col] = df[valid_cols].sum(axis=1, min_count=1) if valid_cols else np.nan

        elif target_col == "3PL_Batch_Number":
            df[target_col] = np.nan
            for src in source_cols:
                if src in df.columns:
                    mask = df[target_col].isna() & df[src].notna()
                    df.loc[mask, target_col] = df.loc[mask, src]

        elif target_col == "3PL_Material_Code":
            df[target_col] = np.nan
            for src in source_cols:
                if src in df.columns:
                    df.loc[~df[src].astype(str).str.contains(r"\d", na=False), src] = np.nan
                    mask = df[target_col].isna() & df[src].notna()
                    df.loc[mask, target_col] = df.loc[mask, src]

    required_columns = ["3PL_Material_Code", "3PL_Batch_Number", "3PL_Quantity"]
    missing = [c for c in required_columns if c not in df.columns or df[c].isna().all()]
    if missing:
        raise ValueError(f"Validation failed: missing or empty required columns: {', '.join(missing)}")

    if "3PL_UOM" not in df.columns:
        df["3PL_UOM"] = None

    df = df[["3PL_Material_Code", "3PL_Batch_Number", "3PL_Quantity", "3PL_UOM"]]
    return df


# ---------------------------------------------------------------------------
# Quantity aggregation
# ---------------------------------------------------------------------------

def aggregate_quantities(df: pd.DataFrame) -> pd.DataFrame:
    main_group_cols = ["3PL", "3PL_Material_Code", "3PL_Batch_Number"]
    group_cols = [c for c in df.columns if c != "3PL_Quantity"]

    valid = df[df[main_group_cols].notna().all(axis=1)]
    invalid = df[~df[main_group_cols].notna().all(axis=1)]

    grouped = valid.groupby(group_cols, dropna=False, as_index=False)["3PL_Quantity"].sum()
    return pd.concat([grouped, invalid], ignore_index=True)


# ---------------------------------------------------------------------------
# Material code mapping
# ---------------------------------------------------------------------------

def map_material_code(df: pd.DataFrame, item_mapping_df: pd.DataFrame, material_master_df: pd.DataFrame) -> pd.DataFrame:
    df = df.merge(
        item_mapping_df[["Plant_Number", "3PL_Part", "Material_Number"]].drop_duplicates(subset=["Plant_Number", "3PL_Part"], keep="first"),
        how="left",
        left_on=["3PL", "3PL_Material_Code"],
        right_on=["Plant_Number", "3PL_Part"],
    )

    mask = df["Material_Number"].notna()
    df.loc[mask, "Gilead_Material_Code"] = df.loc[mask, "Material_Number"]
    df = df.drop(columns=["Plant_Number", "3PL_Part", "Material_Number"])

    valid_material_codes = set(material_master_df["matnr"].dropna().unique())
    df = df.merge(material_master_df.drop_duplicates(), how="left", left_on="3PL_Material_Code", right_on="matnr")

    mask = df["matnr"].notna() & df["Gilead_Material_Code"].isna()
    df.loc[mask, "Gilead_Material_Code"] = df.loc[mask, "matnr"]
    df = df.drop(columns=["matnr"])

    mask_not_in_master = ~df["Gilead_Material_Code"].isin(valid_material_codes) & df["Gilead_Material_Code"].notna()
    df.loc[mask_not_in_master, "Validation_Remark"] = "Mapped Material Code Not In Material master"
    df.loc[mask_not_in_master, "Has_Error"] = True

    mask_invalid = df["Gilead_Material_Code"].isna()
    df.loc[mask_invalid, "Validation_Remark"] = "Invalid 3PL Material Code"
    df.loc[mask_invalid, "Has_Error"] = True

    mask_null = df["3PL_Material_Code"].isna()
    df.loc[mask_null, "Validation_Remark"] = "Material Code is NULL in 3PL File"
    df.loc[mask_null, "Has_Error"] = True

    return df


# ---------------------------------------------------------------------------
# Lot number mapping
# ---------------------------------------------------------------------------

def map_lot_no_wildcard(df: pd.DataFrame, lot_number_master_df: pd.DataFrame, lot_no_mapping_lookup: dict) -> pd.DataFrame:
    # ------------------------------------------------------------------
    # Pass 1 — exact match against lot number master
    # ------------------------------------------------------------------
    df = df.merge(
        lot_number_master_df.drop_duplicates(),
        how="left",
        left_on=["Gilead_Material_Code", "3PL_Batch_Number"],
        right_on=["matnr", "charg"],
    )
    mask = df["charg"].notna()
    df.loc[mask, "Gilead_Batch_Number"] = df.loc[mask, "charg"]
    df = df.drop(columns=["matnr", "charg"])

    # ------------------------------------------------------------------
    # Pass 2 — wildcard/substring match using pre-built lookup
    # ------------------------------------------------------------------
    for idx, row in df[df["Gilead_Batch_Number"].isna()].iterrows():
        batch = str(row["3PL_Batch_Number"])
        for atwrt, charg in lot_no_mapping_lookup.get(row["Gilead_Material_Code"], []):
            if batch in atwrt:
                df.at[idx, "Gilead_Batch_Number"] = charg
                break

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    mask_invalid_batch = (
        df["Gilead_Material_Code"].notna()
        & df["Gilead_Batch_Number"].isna()
    )
    df.loc[mask_invalid_batch, "Validation_Remark"] = "Invalid 3PL Batch Number"
    df.loc[mask_invalid_batch, "Has_Error"] = True

    mask_invalid_material = df["Validation_Remark"] == "Invalid 3PL Material Code"
    mask_null_batch_q_0 = (~mask_invalid_material) & df["3PL_Batch_Number"].isna() & (df["3PL_Quantity"] == 0)
    df.loc[mask_null_batch_q_0, "Validation_Remark"] = "Batch Number NULL In 3PL File"
    df.loc[mask_null_batch_q_0, "Has_Error"] = False

    return df


# ---------------------------------------------------------------------------
# UOM mapping and conversion
# ---------------------------------------------------------------------------

def _map_uom_master(df: pd.DataFrame, uom_master_df: pd.DataFrame) -> pd.DataFrame:
    if df["3PL_UOM"].isna().all():
        print("\t\t3PL_UOM column is empty — skipping UOM master conversion")
        return df
    if not df["Conversion_Factor"].isna().any():
        print("\t\tAll rows already have a Conversion Factor — skipping UOM master conversion")
        return df

    df["3PL_UOM_upper"] = df["3PL_UOM"].str.upper()
    uom_standardization = {
        "GM": ["GRAM", "GRAMS", "GR", "GRM", "G"],
        "KG": ["KILOGRAM", "KILOGRAMS", "KILO", "KGS"],
    }
    for standard, variations in uom_standardization.items():
        df.loc[df["3PL_UOM_upper"].isin(variations + [standard]), "3PL_UOM_upper"] = standard

    uom_master_df["conversion_factor"] = uom_master_df["conversion_factor"].astype(float)
    df = df.merge(
        uom_master_df[["matnr", "alternate_uom", "gilead_uom", "conversion_factor"]].drop_duplicates(),
        how="left",
        left_on=["Gilead_Material_Code", "3PL_UOM_upper"],
        right_on=["matnr", "alternate_uom"],
    )
    mask = df["conversion_factor"].notna() & df["Conversion_Factor"].isna()
    df.loc[mask, "Conversion_Factor"] = df.loc[mask, "conversion_factor"]
    df = df.drop(columns=["3PL_UOM_upper", "matnr", "alternate_uom", "gilead_uom", "conversion_factor"])
    df["Conversion_Factor"] = df["Conversion_Factor"].astype(float)
    return df


def map_uom_and_convert(df: pd.DataFrame, uom_mapping_df: pd.DataFrame, uom_master_df: pd.DataFrame) -> pd.DataFrame:
    df = df.merge(
        uom_mapping_df[["Plant_Number", "3PL_Part", "Gilead_UOM", "Conversion_Factor"]].drop_duplicates(subset=["Plant_Number", "3PL_Part"], keep="first"),
        how="left",
        left_on=["3PL", "3PL_Material_Code"],
        right_on=["Plant_Number", "3PL_Part"],
    )
    df["Conversion_Factor"] = df["Conversion_Factor"].astype(float)

    df = _map_uom_master(df, uom_master_df)

    mask = df["Conversion_Factor"].notna()
    df["3PL_Converted_Quantity"] = df["3PL_Quantity"]
    df.loc[mask, "3PL_Converted_Quantity"] = df.loc[mask, "3PL_Quantity"] * df.loc[mask, "Conversion_Factor"]
    df = df.drop(columns=["3PL_Part", "Plant_Number"])
    return df


# ---------------------------------------------------------------------------
# Cost and material type
# ---------------------------------------------------------------------------

def get_unit_cost(df: pd.DataFrame, unit_cost_df: pd.DataFrame) -> pd.DataFrame:
    unit_cost_df = unit_cost_df.rename(columns={"standard_cost_usd": "Cost"})
    df = df.merge(
        unit_cost_df,
        how="left",
        left_on=["3PL", "Gilead_Material_Code"],
        right_on=["plant_code", "material_number"],
    )
    df = df.drop(columns=["material_number", "plant_code", "base_uom", "standard_cost", "currency_code", "exchange_rate", "exchange_rate_date"])
    return df


def get_material_type(df: pd.DataFrame, material_type_df: pd.DataFrame) -> pd.DataFrame:
    material_type_df = material_type_df.rename(columns={"extwg": "3PL_Material_Type"})
    df = df.merge(material_type_df, how="left", left_on="Gilead_Material_Code", right_on="matnr")
    df = df.drop(columns=["matnr"])
    return df


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

_OUTPUT_COLUMNS = [
    # Business columns
    "Segment",
    "3PL",
    "3PL_Name",
    "3PL_Material_Code",
    "Gilead_Material_Code",
    "3PL_Batch_Number",
    "Gilead_Batch_Number",
    "3PL_UOM",
    "Gilead_UOM",
    "Conversion_Factor",
    "3PL_Quantity",
    "3PL_Converted_Quantity",
    "Cost",
    "3PL_Material_Type",
    "3PL_Type",
    # Validation
    "Has_Error",
    "Validation_Remark",
    # Metadata
    "File_Name",
    "Year",
    "Quarter",
    "Date_Processed",
]


def curated_processing(raw_df: pd.DataFrame, raw_file_path: str, mapping_cache: MappingDataCache, segment: str = "") -> pd.DataFrame:
    """Apply the full curation pipeline to a single raw 3PL CSV.

    Args:
        mapping_cache: fully loaded cache including pre-built header_mapping dict.
        segment: business segment (e.g. 'commercial', 'clinical'). Written to
            the Segment column and used as a partition key in the Delta table.

    Returns a DataFrame with the columns defined in _OUTPUT_COLUMNS.
    """
    df = raw_df.copy()
    header_mapping = mapping_cache.header_mapping

    df.columns = df.columns.str.lower().str.strip().str.replace(r"\s+", " ", regex=True)
    site_id = get_site_id(raw_file_path)

    # Filter to only the source columns this site's mapping references,
    # before add_3pl_details or any other step sees the data.
    file_stem = os.path.splitext(os.path.basename(raw_file_path))[0].lower().replace(" ", "_")
    column_mapping = header_mapping.get(f"{site_id}_{file_stem}") or header_mapping.get(site_id)
    if column_mapping is not None:
        source_cols = {
            part.strip()
            for source_col in column_mapping
            for part in re.split(r"[,+]", source_col)
        }
        df = df[[c for c in df.columns if c in source_cols]]

    print(f"\n  Site: {site_id}  |  input shape: {df.shape}")

    print("    Applying header mapping")
    df = map_3pl_df(df, site_id, file_stem, header_mapping)

    print("    Adding 3PL details")
    df = add_3pl_details(df, site_id, mapping_cache.plant_name_mapping_df, mapping_cache.pl_type_mapping_df)

    print("    Post-processing mapped columns")
    df = postprocess_mapped_df(df)

    print("    Adding metadata")
    df = add_metadata(df, raw_file_path)

    print("    Aggregating quantities")
    df = aggregate_quantities(df)

    df["Has_Error"] = False
    df["Validation_Remark"] = None

    print("    Mapping material codes")
    df = map_material_code(df, mapping_cache.item_mapping_df, mapping_cache.material_master_df)

    print("    Mapping lot numbers")
    df = map_lot_no_wildcard(df, mapping_cache.lot_no_master_df, mapping_cache.lot_no_mapping_lookup)

    print("    Processing UOM mapping and conversion")
    df = map_uom_and_convert(df, mapping_cache.uom_mapping_df, mapping_cache.uom_master_df)

    print("    Getting unit costs")
    df = get_unit_cost(df, mapping_cache.unit_cost_df)

    print("    Getting material type")
    df = get_material_type(df, mapping_cache.material_type_df)

    df["Segment"] = segment
    df = df[_OUTPUT_COLUMNS]
    print(f"    Done — output shape: {df.shape}")
    return df


# ---------------------------------------------------------------------------
# SAP enrichment (used at curated write time)
# ---------------------------------------------------------------------------

def match_gilead_receipts(sap_df: pd.DataFrame, gil_receipts_df: pd.DataFrame) -> pd.DataFrame:
    """Left-join Gilead receipt quantities onto the SAP report rows.

    The SAP report is expected to have Delta-table column names (underscores).
    gil_receipts_df columns: plant, material, batch, qty
    """
    gil_receipts_df = (
        gil_receipts_df
        .rename(columns={"qty": "Gilead_Receipts", "plant": "Plant_Receipts"})
        .dropna(subset=["Plant_Receipts", "material", "batch"])
        .astype(str)
    )
    sap_df = sap_df.merge(
        gil_receipts_df[["Plant_Receipts", "material", "batch", "Gilead_Receipts"]],
        how="left",
        left_on=["Plant", "Material_Number", "Batch_Number"],
        right_on=["Plant_Receipts", "material", "batch"],
    )
    return sap_df.drop(columns=["Plant_Receipts", "material", "batch"])

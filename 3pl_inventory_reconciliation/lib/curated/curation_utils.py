"""Transformation utilities for the 3PL curated processing layer.

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
    """Return the site/3PL ID from the file's parent directory name, normalised to int-string."""
    folder = Path(file_path).parts[-2]
    try:
        return str(int(folder))
    except ValueError:
        return folder.replace(" ", "_")


def get_3pl_number(file_path: str) -> str:
    """Return the numeric portion at the start of the site folder name."""
    folder = Path(file_path).parts[-2]
    match = re.search(r"^\d+", folder)
    return match.group(0) if match else folder


# ---------------------------------------------------------------------------
# Metadata enrichment
# ---------------------------------------------------------------------------

def add_3pl_details(df: pd.DataFrame, file_path: str, plant_name_mapping_df: pd.DataFrame, pl_type_mapping_df: pd.DataFrame) -> pd.DataFrame:
    site_id = get_3pl_number(file_path)
    df.columns = df.columns.str.lower()
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
    df["Has_Error"] = False
    return df


# ---------------------------------------------------------------------------
# Header mapping
# ---------------------------------------------------------------------------

def _process_header_mapping(header_mapping_df: pd.DataFrame) -> dict:
    """Build {combined_key: {3pl_col: gilead_col}} from the header mapping DataFrame."""
    hdf = header_mapping_df.dropna(subset=["3PL"]).copy()
    hdf["Sheet_Name"] = hdf["Sheet_Name"].replace("nan", np.nan)
    hdf["3PL"] = hdf["3PL"].astype("int").astype("str")
    hdf["combined_key"] = hdf.apply(
        lambda row: (
            f"{row['3PL']}_{row['Sheet_Name'].replace(' ', '_')}"
            if pd.notna(row.get("Sheet_Name")) and row.get("Sheet_Name")
            else row["3PL"]
        ),
        axis=1,
    )
    mapping_dict = (
        hdf.groupby("combined_key")
        .apply(lambda g: dict(zip(g["3PL_Column_Header"], g["Gilead_Column_Header"])))
        .to_dict()
    )
    return mapping_dict


def _clean_all_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.replace("\xa0", "") if hasattr(df[col], "str") else df[col]
    return df


def _additional_preprocessing(df: pd.DataFrame, quantity_processed: bool) -> pd.DataFrame:
    df["3PL_Material_Code"] = df["3PL_Material_Code"].str.replace(r"\.0$", "", regex=True)
    if not quantity_processed:
        df["3PL_Quantity"] = df["3PL_Quantity"].str.replace(r"[^\d\.\-]", "", regex=True)
    df = df[df["3PL_Quantity"].isna() | pd.to_numeric(df["3PL_Quantity"], errors="coerce").notna()]
    df["3PL_Quantity"] = df["3PL_Quantity"].astype(float)
    materials_with_batch = df[df["3PL_Batch_Number"].notna()]["3PL_Material_Code"].unique()
    df = df[df["3PL_Batch_Number"].notna() | ~df["3PL_Material_Code"].isin(materials_with_batch)]
    return df


def map_filter_3pl_df(df: pd.DataFrame, site_id: str, file_path: str, header_mapping_df: pd.DataFrame) -> pd.DataFrame:
    hdf = header_mapping_df.copy()
    hdf["Sheet_Name"] = hdf.groupby("3PL")["Sheet_Name"].ffill().str.lower()
    hdf["3PL_Column_Header"] = hdf["3PL_Column_Header"].str.lower()

    header_mapping = _process_header_mapping(hdf)

    # Try sheet-aware key first (e.g. "1205_gilead_api_inventory_by_weight"),
    # then fall back to the plain site key (e.g. "1205") for single-sheet files.
    file_stem = os.path.splitext(os.path.basename(file_path))[0].lower().replace(" ", "_")
    sheet_key = f"{site_id}_{file_stem}"
    column_mapping = header_mapping.get(sheet_key) or header_mapping.get(site_id)
    if column_mapping is None:
        raise KeyError(
            f"No header mapping found for site '{site_id}' "
            f"(tried '{sheet_key}' and '{site_id}')"
        )

    complex_mappings = {}
    simple_mapping = {}
    for source_col, target_col in column_mapping.items():
        cleaned = re.sub(r"\s+", " ", source_col.replace("\xa0", " ")).strip()
        if "," in source_col and target_col == "3PL_Batch_Number":
            complex_mappings[target_col] = [re.sub(r"\s+", " ", c.replace("\xa0", " ")).strip() for c in source_col.split(",")]
        elif "," in source_col and target_col == "3PL_Material_Code":
            complex_mappings[target_col] = [re.sub(r"\s+", " ", c.replace("\xa0", " ")).strip() for c in source_col.split(",")]
        elif "+" in source_col and target_col == "3PL_Quantity":
            complex_mappings[target_col] = [re.sub(r"\s+", " ", c.replace("\xa0", " ")).strip() for c in source_col.split("+")]
        else:
            simple_mapping[cleaned] = target_col

    df = df.rename(columns=simple_mapping)

    quantity_processed = False
    for target_col, source_cols in complex_mappings.items():
        if target_col == "3PL_Quantity":
            for col in source_cols:
                if col in df.columns:
                    df[col] = df[col].str.replace(r"[^\d\.\-]", "", regex=True).astype(float)
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            valid_cols = [c for c in source_cols if c in df.columns]
            df[target_col] = df[valid_cols].sum(axis=1) if valid_cols else np.nan
            quantity_processed = True

        if target_col == "3PL_Batch_Number":
            df[target_col] = np.nan
            for src in source_cols:
                if src in df.columns:
                    mask = df[target_col].isna() & df[src].notna()
                    df.loc[mask, target_col] = df.loc[mask, src]

        if target_col == "3PL_Material_Code":
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

    df = df[["3PL", "3PL_Name", "3PL_Material_Code", "3PL_Batch_Number", "3PL_Quantity", "3PL_UOM", "3PL_Type"]]
    df = _additional_preprocessing(df, quantity_processed)
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
    item_mapping_df = _clean_all_string_columns(item_mapping_df).dropna(subset=["3PL_Part"])

    df = df.merge(
        item_mapping_df[["Plant_Number", "3PL_Part", "Material_Number", "Material_Description"]].drop_duplicates(),
        how="left",
        left_on=["3PL", "3PL_Material_Code"],
        right_on=["Plant_Number", "3PL_Part"],
    )

    mask = df["Material_Number"].notna()
    df.loc[mask, "Gilead_Material_Code"] = df.loc[mask, "Material_Number"]
    df = df.drop(columns=["Plant_Number", "3PL_Part", "Material_Number", "Material_Description"])

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
    df.loc[mask_null, "Validation_Remark"] = "Material Code is NULL in 3PL file"
    df.loc[mask_null, "Has_Error"] = True

    df["Gilead_Material_Code"] = df["Gilead_Material_Code"].astype(str)
    return df


# ---------------------------------------------------------------------------
# Lot number mapping
# ---------------------------------------------------------------------------

def map_lot_no_wildcard(df: pd.DataFrame, lot_number_master_df: pd.DataFrame, lnmdf: pd.DataFrame, sapdf: pd.DataFrame) -> pd.DataFrame:
    lot_number_master_df = lot_number_master_df.drop_duplicates()

    df = df.merge(lot_number_master_df, how="left", left_on=["Gilead_Material_Code", "3PL_Batch_Number"], right_on=["matnr", "charg"])
    mask = df["charg"].notna()
    df.loc[mask, "Gilead_Batch_Number"] = df.loc[mask, "charg"]

    already_matched = df["Gilead_Batch_Number"].notna()
    df = df.drop(columns=["matnr", "charg"])

    lnmdf = lnmdf.astype(str)
    lnmdf["atwrt"] = lnmdf["atwrt"].str.lstrip("0")
    lnmdf = lnmdf.drop_duplicates(["charg", "matnr", "atwrt"])

    unmatched_df = df[~already_matched].reset_index(drop=True)
    matched_df = df[already_matched].reset_index(drop=True)

    matches = []
    lnmdf_extra_cols = {c: None for c in lnmdf.columns if c not in df.columns}

    for _, row in unmatched_df.iterrows():
        potential = lnmdf[lnmdf["matnr"] == row["Gilead_Material_Code"]]
        found = False
        for _, right_row in potential.iterrows():
            if str(row["3PL_Batch_Number"]) in str(right_row["atwrt"]):
                matches.append({**row.to_dict(), **right_row.to_dict()})
                found = True
                break
        if not found:
            matches.append({**row.to_dict(), **lnmdf_extra_cols})

    for _, row in matched_df.iterrows():
        matches.append({**row.to_dict(), **lnmdf_extra_cols})

    df = pd.DataFrame(matches)

    mask = df["charg"].notna() & df["Gilead_Batch_Number"].isna()
    df.loc[mask, "Gilead_Batch_Number"] = df.loc[mask, "charg"]
    df = df.drop(columns=["charg", "matnr", "atinn", "atwrt"])

    mask_invalid_batch = (df["Gilead_Material_Code"].notna() & (df["Gilead_Material_Code"] != "nan")) & df["Gilead_Batch_Number"].isna()
    df.loc[mask_invalid_batch, "Validation_Remark"] = "Invalid 3PL Batch Number"
    df.loc[mask_invalid_batch, "Has_Error"] = True

    mask_invalid_material = df["Validation_Remark"] == "Invalid 3PL Material Code"
    mask_null_batch_q_0 = (~mask_invalid_material) & df["3PL_Batch_Number"].isna() & (df["3PL_Quantity"] == 0)
    df.loc[mask_null_batch_q_0, "Validation_Remark"] = "Batch Number NULL In 3PL File"
    df.loc[mask_null_batch_q_0, "Has_Error"] = False

    df["Gilead_Batch_Number"] = df["Gilead_Batch_Number"].astype(str)
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
        left_on=["Gilead_Material_Code", "3PL_UOM"],
        right_on=["matnr", "alternate_uom"],
    )
    mask = df["conversion_factor"].notna() & df["Conversion_Factor"].isna()
    df.loc[mask, "Conversion_Factor"] = df.loc[mask, "conversion_factor"]
    df = df.drop(columns=["3PL_UOM_upper", "matnr", "alternate_uom", "gilead_uom", "conversion_factor"])
    df["Conversion_Factor"] = df["Conversion_Factor"].astype(float)
    return df


def map_uom_and_convert(df: pd.DataFrame, uom_mapping_df: pd.DataFrame, uom_master_df: pd.DataFrame) -> pd.DataFrame:
    uom_mapping_df = uom_mapping_df.dropna(subset=["3PL_Part"])
    uom_mapping_df = _clean_all_string_columns(uom_mapping_df)
    uom_mapping_df["Conversion_Factor"] = uom_mapping_df["Conversion_Factor"].astype(float)

    df = df.merge(
        uom_mapping_df[["Plant_Number", "3PL_Part", "Gilead_UOM", "Conversion_Factor"]].drop_duplicates(),
        how="left",
        left_on=["3PL", "3PL_Material_Code"],
        right_on=["Plant_Number", "3PL_Part"],
    )
    df["Gilead_UOM"] = df["Gilead_UOM"].astype(str)
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
    unit_cost_df = unit_cost_df.copy()
    unit_cost_df["standard_cost_usd"] = unit_cost_df["standard_cost_usd"].astype(float)
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
    "3PL",
    "3PL_Name",
    "Gilead_Material_Code",
    "Gilead_Batch_Number",
    "Gilead_UOM",
    "Conversion_Factor",
    "3PL_Material_Code",
    "3PL_Batch_Number",
    "3PL_Quantity",
    "3PL_Converted_Quantity",
    "3PL_UOM",
    "3PL_Material_Type",
    "Cost",
    "3PL_Type",
    "File_Name",
    "Year",
    "Quarter",
    "Date_Processed",
    "Has_Error",
    "Validation_Remark",
]


def curated_processing(raw_df: pd.DataFrame, raw_file_path: str, mapping_cache: MappingDataCache) -> pd.DataFrame:
    """Apply the full curation pipeline to a single raw 3PL CSV.

    Returns a DataFrame with the columns defined in _OUTPUT_COLUMNS.
    """
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)

    df = raw_df.copy().replace([None, "None", "nan", "NaN", ""], np.nan)
    site_id = get_site_id(raw_file_path)
    print(f"\n  Site: {site_id}  |  input shape: {df.shape}")

    print("    Adding 3PL details")
    df = add_3pl_details(df, raw_file_path, mapping_cache.plant_name_mapping_df, mapping_cache.pl_type_mapping_df)

    print("    Applying header mapping")
    df = map_filter_3pl_df(df, site_id, raw_file_path, mapping_cache.header_mapping_df)

    print("    Adding metadata")
    df = add_metadata(df, raw_file_path)

    print("    Aggregating quantities")
    df = aggregate_quantities(df)

    print("    Mapping material codes")
    df = map_material_code(df, mapping_cache.item_mapping_df, mapping_cache.material_master_df)

    print("    Mapping lot numbers")
    df = map_lot_no_wildcard(df, mapping_cache.lot_no_master_df, mapping_cache.lot_no_mapping_df, mapping_cache.sap_report_df)

    print("    Processing UOM mapping and conversion")
    df = map_uom_and_convert(df, mapping_cache.uom_mapping_df, mapping_cache.uom_master_df)

    print("    Getting unit costs")
    df = get_unit_cost(df, mapping_cache.unit_cost_df)

    print("    Getting material type")
    df = get_material_type(df, mapping_cache.material_type_df)

    df = df[_OUTPUT_COLUMNS].replace("nan", np.nan)
    print(f"    Done — output shape: {df.shape}")
    return df

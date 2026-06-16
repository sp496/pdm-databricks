"""Transformation functions for the 3PL curated processing layer.

Shared utilities
----------------
remove_decimal_if_all_zeros  — strips trailing '.0', '.00', etc. from batch/
                               material strings produced by pandas float→string
                               conversion.  Used here when normalising
                               Gilead_Batch_Number after lot-number mapping,
                               and in write_mapping_tables when normalising the
                               SAP Batch_Number before writing to the Delta table.

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
# Shared utilities
# ---------------------------------------------------------------------------

def remove_decimal_if_all_zeros(value):
    """Strip trailing '.0', '.00', etc. from a string batch/material number.

    Pandas sometimes serialises integer-valued floats as '123456.0' when
    converting numeric columns to strings.  This function removes that suffix
    so batch numbers join correctly against SAP values.
    """
    if isinstance(value, str):
        parts = value.split(".")
        if len(parts) == 2 and all(c == "0" for c in parts[1]):
            return parts[0]
    return value

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def get_site_id(file_path: str) -> str:
    """Return the site/3PL ID from the file's parent directory name."""
    return Path(file_path).parts[-2]


# ---------------------------------------------------------------------------
# Metadata enrichment
# ---------------------------------------------------------------------------

def add_3pl_details(df: pd.DataFrame, site_id: str, plant_name_mapping_df: pd.DataFrame) -> pd.DataFrame:
    df["3PL"] = site_id

    plant_name_dict = plant_name_mapping_df.set_index("plant_number")["plant_name"].to_dict()
    df["3PL_Name"] = plant_name_dict.get(site_id)

    return df


def resolve_plants_from_sap(
    df: pd.DataFrame,
    candidate_plants: list,
    sap_plant_df: pd.DataFrame,
    plant_name_mapping_df: pd.DataFrame,
) -> pd.DataFrame:
    """Resolve the real plant(s) for a multi-plant 3PL file using the SAP report.

    A combined folder like `1696_1664_1635` holds rows for several plants in
    one file. The folder name is the universe of candidate plants; the actual
    plant(s) for each row are determined by looking its
    (Gilead_Material_Code, Gilead_Batch_Number) up in the SAP report, which
    carries the authoritative per-plant stock for each material/batch.

    Quantity split:
      A material-batch combo can sit in MORE THAN ONE candidate plant. We
      can't tell from the 3PL file how its reported quantity divides across
      those plants, so we use SAP's per-plant stock as the proxy:
        - per-plant fraction = plant's SAP stock / combo's total SAP stock
        - the row is duplicated to each matching plant and its 3PL_Quantity
          is multiplied by that plant's fraction.
      The split quantities sum back to the original 3PL_Quantity, so totals
      are preserved and no plant double-counts.

    Behaviour:
      - SAP is restricted to candidate_plants (prevents mis-assignment to
        unrelated plants that happen to hold the same material/batch).
      - The left-merge on (material, batch) DUPLICATES a row to every matching
        plant and yields a null Plant (and null fraction) when there is NO match.
      - Quantities are always non-negative, so a zero combo total means every
        candidate plant holds zero stock — in that case the quantity is split
        EQUALLY (1/N) across the matching plants.
      - No-match rows keep 3PL null, retain their ORIGINAL 3PL_Quantity (no
        fraction applied) and are flagged Has_Error=True /
        "Plant unresolved from SAP" (without clobbering an existing remark).
      - 3PL_Name is re-derived from plant_name_mapping for the resolved plant
        (add_3pl_details set it to null for the combined token).

    Returns df with 3PL / 3PL_Name rewritten, rows duplicated, and
    3PL_Quantity split across plants as needed.
    """
    sap = sap_plant_df[sap_plant_df["Plant"].astype(str).isin([str(p) for p in candidate_plants])].copy()
    sap["Material_Number"] = sap["Material_Number"].astype(str)
    sap["Batch_Number"]    = sap["Batch_Number"].astype(str)
    sap["_qty"] = pd.to_numeric(sap["Stock_Quantity__Base_UOM_"], errors="coerce").fillna(0)

    # Per-plant SAP quantity for each material-batch.
    per_plant = sap.groupby(["Plant", "Material_Number", "Batch_Number"], as_index=False)["_qty"].sum()

    # Combo total + plant count (the count drives the all-zero equal-split fallback).
    agg = per_plant.groupby(["Material_Number", "Batch_Number"]).agg(
        _total_qty=("_qty", "sum"),
        _n_plants =("Plant", "size"),
    ).reset_index()
    ratio = per_plant.merge(agg, on=["Material_Number", "Batch_Number"])

    # Fraction of the 3PL quantity this plant receives. Quantities are >= 0,
    # so a zero total means every candidate plant has zero stock -> equal split.
    ratio["fraction"] = np.where(
        ratio["_total_qty"] == 0,
        1.0 / ratio["_n_plants"],
        ratio["_qty"] / ratio["_total_qty"],
    )

    df = df.copy()
    df = df.merge(
        ratio[["Plant", "Material_Number", "Batch_Number", "fraction"]],
        how="left",
        left_on=["Gilead_Material_Code", "Gilead_Batch_Number"],
        right_on=["Material_Number", "Batch_Number"],
    )

    # Resolved plant overwrites the combined token; null where no SAP match.
    df["3PL"] = df["Plant"]

    # Split the quantity for matched rows only; unmatched rows keep their
    # original 3PL_Quantity (fraction is null there).
    matched = df["fraction"].notna()
    df.loc[matched, "3PL_Quantity"] = df.loc[matched, "3PL_Quantity"] * df.loc[matched, "fraction"]

    # Flag rows that could not be resolved (only where not already flagged).
    no_match = df["Plant"].isna()
    needs_flag = no_match & df["Validation_Remark"].isna()
    df.loc[needs_flag, "Validation_Remark"] = "Plant unresolved from SAP"
    df.loc[needs_flag, "Has_Error"] = True

    # Re-derive 3PL_Name for the resolved plant.
    plant_name_dict = plant_name_mapping_df.set_index("plant_number")["plant_name"].to_dict()
    df["3PL_Name"] = df["3PL"].map(lambda p: plant_name_dict.get(p) if pd.notna(p) else None)

    df = df.drop(columns=["Plant", "Material_Number", "Batch_Number", "fraction"])
    return df


def resolve_plants_from_facility(
    df: pd.DataFrame,
    facility_mapping_df: pd.DataFrame,
    plant_name_mapping_df: pd.DataFrame,
) -> pd.DataFrame:
    """Resolve the real plant per row from a Facility column (clinical almac).

    Some clinical 3PL files (e.g. the `almac` folder) carry no plant in the
    folder name; instead each row has a Facility (e.g. "ACS Durham") that maps
    to a Plant Number via the workbook's "Facility Mapping" sheet. This rewrites
    3PL / 3PL_Name per row from that mapping, mirroring resolve_plants_from_sap:
      - unmapped facilities keep 3PL null and are flagged Has_Error=True /
        "Plant unresolved from facility mapping" (without clobbering an existing
        remark);
      - 3PL_Name is re-derived from plant_name_mapping for the resolved plant.
    Facility is matched exactly after a strip (case-sensitive). The scratch
    columns and the Facility column are dropped before returning.
    """
    fm = (
        facility_mapping_df[["Facility", "Plant_Number"]].dropna().drop_duplicates()
        .rename(columns={"Facility": "_map_facility", "Plant_Number": "_map_plant"})
    )
    fm["_map_facility"] = fm["_map_facility"].astype(str).str.strip()

    df = df.copy()
    df["_facility_key"] = df["Facility"].astype(str).str.strip()
    df = df.merge(fm, how="left", left_on="_facility_key", right_on="_map_facility")

    # Resolved plant overwrites the folder token; null where no facility match.
    df["3PL"] = df["_map_plant"]

    needs_flag = df["_map_plant"].isna() & df["Validation_Remark"].isna()
    df.loc[needs_flag, "Validation_Remark"] = "Plant unresolved from facility mapping"
    df.loc[needs_flag, "Has_Error"] = True

    plant_name_dict = plant_name_mapping_df.set_index("plant_number")["plant_name"].to_dict()
    df["3PL_Name"] = df["3PL"].map(lambda p: plant_name_dict.get(p) if pd.notna(p) else None)

    return df.drop(columns=["Facility", "_facility_key", "_map_facility", "_map_plant"])


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
    # Strip stray parentheses from file-provided Gilead codes, e.g. "(FP-1234)"
    # -> "FP-1234", so they match SAP / item_mapping / master joins downstream.
    df["Gilead_Material_Code"] = (
        df["Gilead_Material_Code"].str.replace(r"[()]", "", regex=True).str.strip()
    )
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

        elif target_col in ("3PL_Batch_Number", "Gilead_Batch_Number"):
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

        elif target_col == "Gilead_Material_Code":
            df[target_col] = np.nan
            for src in source_cols:
                if src in df.columns:
                    mask = df[target_col].isna() & df[src].notna()
                    df.loc[mask, target_col] = df.loc[mask, src]

    # Ensure all four code columns exist so downstream functions never need to
    # check column presence — they only need to check whether values are present.
    # Use None (object dtype) so .str accessors downstream stay valid even when
    # the column is entirely empty.
    for col in ("3PL_Material_Code", "Gilead_Material_Code",
                "3PL_Batch_Number",  "Gilead_Batch_Number"):
        if col not in df.columns:
            df[col] = None

    # Validation — at least one of (3PL/Gilead) must have values for material and batch
    if df["3PL_Material_Code"].isna().all() and df["Gilead_Material_Code"].isna().all():
        raise ValueError(
            "Validation failed: both 3PL_Material_Code and Gilead_Material_Code are empty — "
            "header mapping must populate at least one of them."
        )
    if df["3PL_Batch_Number"].isna().all() and df["Gilead_Batch_Number"].isna().all():
        raise ValueError(
            "Validation failed: both 3PL_Batch_Number and Gilead_Batch_Number are empty — "
            "header mapping must populate at least one of them."
        )
    if "3PL_Quantity" not in df.columns or df["3PL_Quantity"].isna().all():
        raise ValueError("Validation failed: missing or empty required column: 3PL_Quantity")

    if "3PL_UOM" not in df.columns:
        df["3PL_UOM"] = None

    out_cols = [
        "3PL_Material_Code", "Gilead_Material_Code",
        "3PL_Batch_Number",  "Gilead_Batch_Number",
        "3PL_Quantity", "3PL_UOM",
    ]
    # Carry the Facility column through when the header mapping produced it
    # (clinical almac), so curated_processing can resolve the per-row plant.
    if "Facility" in df.columns:
        out_cols.append("Facility")
    df = df[out_cols]
    return df


# ---------------------------------------------------------------------------
# Quantity aggregation
# ---------------------------------------------------------------------------

def aggregate_quantities(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = [c for c in df.columns if c != "3PL_Quantity"]

    # A row is valid for aggregation when the 3PL is known and at least one of
    # the 3PL/Gilead variants is populated for both material and batch.
    valid_mask = (
        df["3PL"].notna()
        & (df["3PL_Material_Code"].notna() | df["Gilead_Material_Code"].notna())
        & (df["3PL_Batch_Number"].notna()  | df["Gilead_Batch_Number"].notna())
    )
    valid   = df[valid_mask]
    invalid = df[~valid_mask]

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

    valid_material_codes = set(material_master_df["matnr"].dropna().unique())
    df = df.merge(material_master_df.drop_duplicates(), how="left", left_on="3PL_Material_Code", right_on="matnr")

    # Our mappings take precedence over the file-provided Gilead_Material_Code:
    #   item_mapping (Material_Number) > material_master direct match (matnr)
    #   > file-provided value (from header mapping).
    df["Gilead_Material_Code"] = (
        df["Material_Number"]
        .combine_first(df["matnr"])
        .combine_first(df["Gilead_Material_Code"])
    )
    df = df.drop(columns=["Plant_Number", "3PL_Part", "Material_Number", "matnr"])

    mask_not_in_master = ~df["Gilead_Material_Code"].isin(valid_material_codes) & df["Gilead_Material_Code"].notna()
    df.loc[mask_not_in_master, "Validation_Remark"] = "Mapped Material Code Not In Material master"
    df.loc[mask_not_in_master, "Has_Error"] = True

    mask_invalid = df["Gilead_Material_Code"].isna()
    df.loc[mask_invalid, "Validation_Remark"] = "Invalid 3PL Material Code"
    df.loc[mask_invalid, "Has_Error"] = True

    # Only flag "Material Code is NULL in 3PL File" when neither code was provided.
    # Rows in "Gilead direct" files (3PL_Material_Code null, Gilead_Material_Code present)
    # are not errors.
    mask_null = df["3PL_Material_Code"].isna() & df["Gilead_Material_Code"].isna()
    df.loc[mask_null, "Validation_Remark"] = "Material Code is NULL in 3PL File"
    df.loc[mask_null, "Has_Error"] = True

    return df


# ---------------------------------------------------------------------------
# Lot number mapping
# ---------------------------------------------------------------------------

def map_lot_no_wildcard(df: pd.DataFrame, lot_number_master_df: pd.DataFrame, lot_no_mapping_lookup: dict,
                        sap_plant_df: pd.DataFrame = None) -> pd.DataFrame:
    # SAP batch membership per material, used to disambiguate wildcard matches
    # that resolve to more than one charg. None for clinical (no SAP report).
    sap_batches_by_material = {}
    if sap_plant_df is not None:
        _sap = sap_plant_df.astype({"Material_Number": str, "Batch_Number": str})
        sap_batches_by_material = (
            _sap.groupby("Material_Number")["Batch_Number"].apply(set).to_dict()
        )

    # ------------------------------------------------------------------
    # Pass 1 — exact match against lot number master
    # ------------------------------------------------------------------
    # Preserve the file-provided value; our lookups take precedence over it.
    df = df.rename(columns={"Gilead_Batch_Number": "_file_batch"})

    # Scratch lookup key: strip the 3PL-internal prefix before '/', e.g.
    # "2597-P53/24-080" -> "24-080". Used only for the lot-master / wildcard
    # lookups; the stored 3PL_Batch_Number keeps its original file value.
    # NaN passes through safely.
    df["_batch_lookup"] = (
        df["3PL_Batch_Number"]
        .str.split("/").str[-1]
        .str.split(".").str[-1]
        .str.strip()
    )

    # Exact match against the lot master. Commercial (SAP) zero-pads charg, so
    # its query supplies charg_stripped (leading zeros removed) and we match the
    # 3PL "176899" against it — the merge still returns the ORIGINAL charg, so
    # the resolved Gilead_Batch_Number keeps the master's canonical (padded)
    # form, which then matches SAP downstream. Clinical (EBS) has no
    # charg_stripped column, so we fall back to an exact charg match.
    #
    # Raw-first: try the original 3PL_Batch_Number against the master first and
    # only fall back to the normalised _batch_lookup for rows the raw value did
    # not match — so an already-valid file value is never replaced by a
    # manipulated one.
    master = lot_number_master_df.drop_duplicates()
    charg_key = "charg_stripped" if "charg_stripped" in master.columns else "charg"
    master_cols = [c for c in ("matnr", "charg", "charg_stripped") if c in master.columns]

    # Attempt 1 — exact match on the raw, file-provided 3PL_Batch_Number.
    df = df.merge(
        master, how="left",
        left_on=["Gilead_Material_Code", "3PL_Batch_Number"],
        right_on=["matnr", charg_key],
    )

    # Attempt 2 — for rows the raw value did not match, retry on the normalised
    # _batch_lookup (slash/period-stripped). A raw-matched row yields >=1 rows
    # (all charg non-null); a raw-unmatched row is exactly one NaN row that
    # flows to the fallback merge, so no row is double-counted.
    matched   = df[df["charg"].notna()]
    unmatched = df[df["charg"].isna()].drop(columns=master_cols)
    unmatched = unmatched.merge(
        master, how="left",
        left_on=["Gilead_Material_Code", "_batch_lookup"],
        right_on=["matnr", charg_key],
    )
    df = pd.concat([matched, unmatched], ignore_index=True)

    # ------------------------------------------------------------------
    # Pass 2 — wildcard/substring match for rows with no exact master match
    # ------------------------------------------------------------------
    # Raw-first here too: try a substring hit on the original 3PL_Batch_Number
    # before falling back to the normalised _batch_lookup.
    wildcard_batch = pd.Series(np.nan, index=df.index, dtype=object)
    for idx in df.index[df["charg"].isna()]:
        material   = df.at[idx, "Gilead_Material_Code"]
        candidates = lot_no_mapping_lookup.get(material, [])
        raw_batch  = str(df.at[idx, "3PL_Batch_Number"])
        # Tier 1 — raw substring match.
        matches = [charg for atwrt, charg in candidates if raw_batch in atwrt]
        # Tier 2 — hyphen-insensitive (stray/missing '-'), only if Tier 1 found none.
        # e.g. "PH-0701-0016-0-A43D1" vs "PH-0701-0016-0-A-43D1". Uses the raw
        # batch — the slash/period-stripped key is too short and would produce
        # false-positive substring matches against atwrt.
        if not matches:
            rb = raw_batch.replace("-", "")
            matches = [charg for atwrt, charg in candidates if rb in atwrt.replace("-", "")]

        if matches:
            matches = list(dict.fromkeys(matches))  # de-dup, keep order
            sap_batches = sap_batches_by_material.get(material, set())
            in_sap = [c for c in matches if c in sap_batches]
            # A single external batch can map to several SAP charg. Prefer one
            # SAP actually carries for this material; otherwise fall back to the
            # first match (no SAP signal to choose by).
            wildcard_batch.at[idx] = in_sap[0] if in_sap else matches[0]

    # Precedence: our lookups override the file-provided value:
    #   exact lot-master (charg) > wildcard mapping > file-provided.
    df["Gilead_Batch_Number"] = (
        df["charg"].combine_first(wildcard_batch).combine_first(df["_file_batch"])
    )
    df = df.drop(columns=["matnr", "charg", "_file_batch", "_batch_lookup"])
    df = df.drop(columns=["charg_stripped"], errors="ignore")  # commercial only

    # ------------------------------------------------------------------
    # Normalise batch numbers before validation
    # ------------------------------------------------------------------
    df["Gilead_Batch_Number"] = df["Gilead_Batch_Number"].apply(remove_decimal_if_all_zeros)

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
    # Only flag "Batch Number NULL In 3PL File" when neither batch column was provided —
    # rows with a file-provided Gilead_Batch_Number should not be flagged.
    mask_null_batch_q_0 = (
        (~mask_invalid_material)
        & df["3PL_Batch_Number"].isna()
        & df["Gilead_Batch_Number"].isna()
        & (df["3PL_Quantity"] == 0)
    )
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
    "Material_Description",
    # Validation
    "Has_Error",
    "Validation_Remark",
    # Metadata
    "File_Name",
    "Year",
    "Quarter",
    "Date_Processed",
]


def curated_processing(raw_df: pd.DataFrame, raw_file_path: str, mapping_cache: MappingDataCache, segment: str = "", sap_plant_df: pd.DataFrame = None) -> pd.DataFrame:
    """Apply the full curation pipeline to a single raw 3PL CSV.

    Args:
        mapping_cache: fully loaded cache including pre-built header_mapping dict.
        segment: business segment (e.g. 'commercial', 'clinical'). Written to
            the Segment column and used as a partition key in the Delta table.
        sap_plant_df: distinct (Plant, Material_Number, Batch_Number) from the
            SAP report for this quarter. Required only for multi-plant
            commercial folders (e.g. '1696_1664_1635') to resolve the real
            plant per row; None for single-plant or clinical files.

    Returns a DataFrame with the columns defined in _OUTPUT_COLUMNS.
    """
    df = raw_df.copy()
    header_mapping = mapping_cache.header_mapping

    df.columns = df.columns.str.lower().str.strip().str.replace(r"\s+", " ", regex=True)
    site_id = get_site_id(raw_file_path)

    # A combined folder like "1696_1664_1635" holds rows for several plants;
    # the real plant is resolved from SAP after material/lot mapping.
    candidate_plants = [t for t in site_id.split("_") if t]
    is_multi_plant   = len(candidate_plants) > 1

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
    df = add_3pl_details(df, site_id, mapping_cache.plant_name_mapping_df)

    print("    Post-processing mapped columns")
    df = postprocess_mapped_df(df)

    print("    Adding metadata")
    df = add_metadata(df, raw_file_path)

    print("    Aggregating quantities")
    df = aggregate_quantities(df)

    df["Has_Error"] = False
    df["Validation_Remark"] = None

    # Clinical almac: the folder name is not a plant — resolve the real plant per
    # row from the Facility column via the Facility Mapping sheet, before the
    # 3PL-keyed steps (item/UOM mapping). Gated by data presence (no Facility
    # column / no mapping for commercial), so it is a no-op elsewhere — same
    # pattern as resolve_plants_from_sap.
    if "Facility" in df.columns and mapping_cache.facility_mapping_df is not None:
        print("    Resolving plants from facility mapping")
        df = resolve_plants_from_facility(
            df, mapping_cache.facility_mapping_df, mapping_cache.plant_name_mapping_df
        )

    print("    Mapping material codes")
    df = map_material_code(df, mapping_cache.item_mapping_df, mapping_cache.material_master_df)

    print("    Mapping lot numbers")
    df = map_lot_no_wildcard(df, mapping_cache.lot_no_master_df, mapping_cache.lot_no_mapping_lookup, sap_plant_df)

    # Multi-plant folders: resolve the real plant per row from SAP before the
    # remaining 3PL-keyed steps (UOM, cost) and 3PL_Name run.
    if is_multi_plant and sap_plant_df is not None:
        print(f"    Resolving plants from SAP for combined folder {candidate_plants}")
        df = resolve_plants_from_sap(df, candidate_plants, sap_plant_df, mapping_cache.plant_name_mapping_df)

    print("    Processing UOM mapping and conversion")
    df = map_uom_and_convert(df, mapping_cache.uom_mapping_df, mapping_cache.uom_master_df)

    if mapping_cache.unit_cost_df is not None:
        print("    Getting unit costs")
        df = get_unit_cost(df, mapping_cache.unit_cost_df)
    else:
        # Clinical: no EBS unit-cost source — leave Cost null.
        print("    No unit-cost dataset — leaving Cost null")
        df["Cost"] = np.nan

    print("    Getting material type")
    df = get_material_type(df, mapping_cache.material_type_df)

    print("    Enriching material descriptions")
    df = enrich_material_description(df, mapping_cache.material_description_df, "Gilead_Material_Code")

    df["Segment"] = segment
    df = df[_OUTPUT_COLUMNS]
    print(f"    Done — output shape: {df.shape}")
    return df


# ---------------------------------------------------------------------------
# Material description enrichment (curated pipeline + SAP write time)
# ---------------------------------------------------------------------------

def enrich_material_description(df: pd.DataFrame, material_description_df: pd.DataFrame, join_key: str) -> pd.DataFrame:
    """Left-join material descriptions onto df and uppercase the result.

    Used in two places:
      - curated pipeline: join_key = 'Gilead_Material_Code'
      - write_mapping_tables (SAP): join_key = 'Material_Number'

    Both sources come from the same makt table query so descriptions are
    consistent across the two sides of the processed-layer reconciliation.
    """
    mat_desc = material_description_df[["matnr", "Material_Description"]].copy()
    df = df.merge(mat_desc, how="left", left_on=join_key, right_on="matnr")
    df["Material_Description"] = df["Material_Description"].str.upper()
    return df.drop(columns=["matnr"])



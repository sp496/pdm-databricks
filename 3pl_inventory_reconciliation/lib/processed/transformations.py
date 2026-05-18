"""Transformation functions for the 3PL processed (reconciliation) layer."""

import pandas as pd


# ---------------------------------------------------------------------------
# Commercial reconciliation pipeline
# ---------------------------------------------------------------------------

_SAP_INPUT_COLUMNS = [
    "Plant",
    "Plant_Name",
    "External_Material_Group",
    "Material_Number",
    "Material_Description_in_Uppercase_for_Matchcodes",
    "Group_Valuation_Standard_Price",
    "Price_Unit",
    "Group_Valuation_Standard_Cost",
    "Stock_Quantity__Base_UOM_",
    "Base_UOM",
    "Batch_Number",
    "Gilead_Receipts",  # enriched at curated write time via match_gilead_receipts
]

_OUTPUT_COLUMNS = [
    "Plant_Number",
    "Plant_Name",
    "External_Material_Group",
    "Material_Number",
    "Gilead_Material_Code",
    "Material_Description",
    "Cost_ea",
    "Cost_ea_per_unit",
    "Group_Valuation_Standard_Cost",
    "Cost",
    "Batch_Number",
    "Stock_OH",
    "UOM",
    "3PL_Quantity",
    "3PL_Converted_Quantity",
    "3PL_UOM",
    "3PL",
    "3PL_Name",
    "3PL_Type",
    "3PL_Material_Code",
    "3PL_Material_Type",
    "Line_item_variance_threshold_amount",
    "Gilead_Receipts",
    "File_Name",
    "Year",
    "Quarter",
    "Date_Processed",
    "Has_Error",
    "Validation_Remark",
    "Gilead_Batch_Number",
    "3PL_Batch_Number",
    "Plant_Classification",
    "Effective_Material_Code",
    "Effective_Batch_Number",
]


def process_commercial(
    curated_df: pd.DataFrame,
    sap_df: pd.DataFrame,
    header_mapping_df: pd.DataFrame,
    year: str,
    quarter: str,
) -> pd.DataFrame:
    """Reconcile commercial 3PL inventory against the SAP report.

    Performs an outer join on (plant, material, batch), enriches with material
    descriptions, plant classification, and derived columns.
    Gilead_Receipts is expected to be pre-populated in sap_df (written at curated
    time by write_mapping_tables via match_gilead_receipts).
    year and quarter are passed in explicitly (resolved by the notebook via
    resolve_quarter before loading) and stamped onto the output directly.
    Returns a pandas DataFrame ready for Spark conversion in the notebook.
    """
    # ----- SAP: select, aggregate -----
    # Stock_Quantity__Base_UOM_ and Group_Valuation_Standard_Cost are stored as
    # DOUBLE in the sap_report Delta table — no cast needed here
    sap_df = sap_df[_SAP_INPUT_COLUMNS].copy()

    group_cols = [c for c in _SAP_INPUT_COLUMNS
                  if c not in ("Stock_Quantity__Base_UOM_", "Group_Valuation_Standard_Cost")]
    sap_df = (
        sap_df
        .groupby(group_cols, as_index=False, dropna=False)
        .agg(
            Stock_Quantity__Base_UOM_     = ("Stock_Quantity__Base_UOM_", "sum"),
            Group_Valuation_Standard_Cost = ("Group_Valuation_Standard_Cost", "sum"),
        )
    )

    # ----- Filter SAP to known 3PL plant numbers -----
    plant_numbers = header_mapping_df["3PL"].astype(str).unique()
    sap_df = sap_df[sap_df["Plant"].isin(plant_numbers)]

    # ----- Outer merge -----
    combined_df = curated_df.merge(
        sap_df,
        how="outer",
        left_on  = ["3PL",   "Gilead_Material_Code", "Gilead_Batch_Number"],
        right_on = ["Plant", "Material_Number",      "Batch_Number"],
    )
    combined_df["Plant"] = combined_df["Plant"].fillna(combined_df["3PL"])
    combined_df["3PL"]   = combined_df["3PL"].fillna(combined_df["Plant"])

    # ----- Year/Quarter: stamped directly from the parameters resolved by the notebook -----
    combined_df["Year"]    = year
    combined_df["Quarter"] = quarter

    # ----- Material description: SAP column wins for matched/SAP-only rows;
    # curated Material_Description (from makt via enrich_material_description)
    # fills for curated-only rows where the SAP column is null -----
    combined_df["Material_Description"] = (
        combined_df["Material_Description_in_Uppercase_for_Matchcodes"]
        .fillna(combined_df["Material_Description"])
    )
    combined_df = combined_df.drop(columns=["Material_Description_in_Uppercase_for_Matchcodes"])

    # ----- Derived columns -----
    combined_df["Line_item_variance_threshold_amount"] = 250_000
    combined_df["Cost_ea_per_unit"] = (
        pd.to_numeric(combined_df["Group_Valuation_Standard_Price"], errors="coerce")
        / pd.to_numeric(combined_df["Price_Unit"], errors="coerce")
    )
    combined_df["Effective_Material_Code"] = (
        combined_df["Material_Number"]
        .combine_first(combined_df["Gilead_Material_Code"])
        .combine_first(combined_df["3PL_Material_Code"])
    )
    combined_df["Effective_Batch_Number"] = (
        combined_df["Batch_Number"]
        .combine_first(combined_df["Gilead_Batch_Number"])
        .combine_first(combined_df["3PL_Batch_Number"])
    )

    # ----- 3PL_Type enrichment for SAP-only rows -----
    type_lookup = (
        header_mapping_df[["3PL", "3PL_Type"]]
        .drop_duplicates(subset=["3PL"])
        .set_index("3PL")["3PL_Type"]
        .to_dict()
    )
    combined_df["3PL_Type"] = combined_df["3PL_Type"].fillna(
        combined_df["Plant"].map(type_lookup)
    )

    # ----- Plant classification -----
    classification_df = (
        header_mapping_df[["3PL", "3PL_Classification"]]
        .drop_duplicates(subset=["3PL"])
    )
    combined_df = combined_df.merge(
        classification_df, how="left", left_on="Plant", right_on="3PL",
    )
    combined_df = combined_df.drop(columns=["3PL_y"]).rename(columns={"3PL_x": "3PL"})

    # ----- Column rename and select -----
    combined_df = combined_df.rename(columns={
        "Plant":                          "Plant_Number",
        "Group_Valuation_Standard_Price": "Cost_ea",
        "Stock_Quantity__Base_UOM_":      "Stock_OH",
        "Base_UOM":                       "UOM",
        "3PL_Classification":             "Plant_Classification",
        # Material_Description_in_Uppercase_for_Matchcodes already handled above
    })

    return combined_df[_OUTPUT_COLUMNS]

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
    "3PL_Material_Code",
    "3PL_Material_Type",
    "Line_item_variance_threshold_amount",
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

    # ----- Outer merge -----
    # (SAP is pre-filtered to relevant plants by stage_sap_report before
    # being written to the sap_report Delta table, so no additional filter here.)
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


# ---------------------------------------------------------------------------
# Clinical reconciliation pipeline
# ---------------------------------------------------------------------------

_EBS_INPUT_COLUMNS = [
    "operating_unit_name",
    "legal_entity_name",
    "inventory_org",
    "inventory_org_name",
    "material_group",
    "item",
    "description",
    "lot_number",
    "lot_status",
    "lot_expiry_date",
    "lot_retest_date",
    "primary_uom",
    "onhand_quantity",
    "reservation_quantity",
    "available_quantity",
]

_CLINICAL_OUTPUT_COLUMNS = [
    # System-of-record (EBS) context
    "Org_Code",
    "Inventory_Org_Name",
    "Operating_Unit_Name",
    "Legal_Entity_Name",
    "Material_Group",
    # Material / item
    "Item_Number",
    "Gilead_Material_Code",
    "Item_Description",        # EBS source — NULL on curated-only rows
    "Material_Description",    # curated/SAP makt source — NULL on EBS-only rows
    # Lot
    "Lot_Number",
    "Lot_Status",
    "Lot_Expiry_Date",
    "Lot_Retest_Date",
    # EBS quantities
    "Onhand_Quantity",
    "Allocated_Quantity",
    "Available_To_Reserve_Quantity",
    # Curated 3PL (depot) quantities
    "3PL_Quantity",
    "3PL_Converted_Quantity",
    # Units
    "Primary_UOM",             # EBS primary_uom, renamed for unambiguous pairing
    "3PL_UOM",
    # 3PL / curated identifiers
    "3PL",
    "3PL_Name",
    "3PL_Material_Code",
    "3PL_Material_Type",
    # Audit / provenance
    "File_Name",
    "Year",
    "Quarter",
    "Date_Processed",
    "Has_Error",
    "Validation_Remark",
    # Curated batch linkage
    "Gilead_Batch_Number",
    "3PL_Batch_Number",
    # Derived
    "Plant_Classification",
    "Effective_Material_Code",
    "Effective_Batch_Number",
]
# Notes:
# - 3PL_Quantity and 3PL_Converted_Quantity carry the curated 3PL ("Depot
#   Inventory") quantities; NULL on EBS-only rows.
# - Item_Description and Material_Description are kept side-by-side with no
#   fillna/fallback — the two source systems remain visible. On EBS-only
#   rows Material_Description is NULL; on curated-only rows Item_Description
#   is NULL.
# - Cost is intentionally NOT included: clinical has no EBS unit-cost source
#   (curated sets Cost=NULL for clinical), so it would be null at all times.
#   Cost is retained in the curated table and the commercial reconciled table.


def process_clinical(
    curated_df: pd.DataFrame,
    ebs_df: pd.DataFrame,
    header_mapping_df: pd.DataFrame,
    year: str,
    quarter: str,
) -> pd.DataFrame:
    """Reconcile clinical 3PL inventory against the EBS clinical snapshot.

    Mirrors the shape of process_commercial, swapping SAP for EBS: full-outer
    join on (org/plant, material, lot) with cross-fill, plus header-mapping-
    driven enrichment for EBS-only rows.

    Returns a pandas DataFrame with the columns defined in
    _CLINICAL_OUTPUT_COLUMNS — ready for Spark conversion in the notebook.
    """
    # ----- EBS: select & aggregate across subinventories -----
    ebs_df = ebs_df[_EBS_INPUT_COLUMNS].copy()

    group_cols = [
        c for c in _EBS_INPUT_COLUMNS
        if c not in ("onhand_quantity", "reservation_quantity", "available_quantity")
    ]
    ebs_df = (
        ebs_df
        .groupby(group_cols, as_index=False, dropna=False)
        .agg(
            onhand_quantity      = ("onhand_quantity", "sum"),
            reservation_quantity = ("reservation_quantity", "sum"),
            available_quantity   = ("available_quantity", "sum"),
        )
    )

    # ----- Full outer merge -----
    # (EBS is pre-filtered to relevant inventory orgs by the SQL query in
    # stage_clinical_inventory before being written to ebs_clinical_inventory,
    # so no additional org filter is applied here.)
    combined_df = curated_df.merge(
        ebs_df,
        how="outer",
        left_on  = ["3PL",           "Gilead_Material_Code", "Gilead_Batch_Number"],
        right_on = ["inventory_org", "item",                 "lot_number"],
    )

    # ----- Cross-fill the plant key -----
    combined_df["inventory_org"] = combined_df["inventory_org"].fillna(combined_df["3PL"])
    combined_df["3PL"]           = combined_df["3PL"].fillna(combined_df["inventory_org"])

    # ----- Year/Quarter stamps -----
    combined_df["Year"]    = year
    combined_df["Quarter"] = quarter

    # ----- Item description / Material description: kept side-by-side, no fillna.
    #   EBS `description`             → Item_Description     (NULL on curated-only)
    #   Curated `Material_Description` is kept as-is          (NULL on EBS-only)

    # ----- Effective key columns -----
    combined_df["Effective_Material_Code"] = (
        combined_df["item"]
        .combine_first(combined_df["Gilead_Material_Code"])
        .combine_first(combined_df["3PL_Material_Code"])
    )
    combined_df["Effective_Batch_Number"] = (
        combined_df["lot_number"]
        .combine_first(combined_df["Gilead_Batch_Number"])
        .combine_first(combined_df["3PL_Batch_Number"])
    )

    # ----- Plant_Classification from header_mapping -----
    classification_df = (
        header_mapping_df[["3PL", "3PL_Classification"]]
        .drop_duplicates(subset=["3PL"])
    )
    combined_df = combined_df.merge(
        classification_df, how="left",
        left_on="inventory_org", right_on="3PL",
    )
    combined_df = combined_df.drop(columns=["3PL_y"]).rename(columns={"3PL_x": "3PL"})

    # ----- Rename to output schema -----
    combined_df = combined_df.rename(columns={
        "inventory_org":        "Org_Code",
        "inventory_org_name":   "Inventory_Org_Name",
        "operating_unit_name":  "Operating_Unit_Name",
        "legal_entity_name":    "Legal_Entity_Name",
        "material_group":       "Material_Group",
        "item":                 "Item_Number",
        "description":          "Item_Description",
        "lot_number":           "Lot_Number",
        "lot_status":           "Lot_Status",
        "lot_expiry_date":      "Lot_Expiry_Date",
        "lot_retest_date":      "Lot_Retest_Date",
        "primary_uom":          "Primary_UOM",
        "onhand_quantity":      "Onhand_Quantity",
        "reservation_quantity": "Allocated_Quantity",
        "available_quantity":   "Available_To_Reserve_Quantity",
        "3PL_Classification":   "Plant_Classification",
    })

    return combined_df[_CLINICAL_OUTPUT_COLUMNS]

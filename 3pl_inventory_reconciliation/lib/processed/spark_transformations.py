"""PySpark transformation for the 3PL processed (reconciliation) layer.

Mirrors process_commercial() in transformations.py but operates entirely on
Spark DataFrames — no toPandas() / createDataFrame() round-trip required.

Not importable in local (non-Spark) environments; import directly in notebooks:
    from lib.processed.spark_transformations import process_commercial_spark
"""

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Column lists  (kept in sync with transformations.py)
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
    "Gilead_Receipts",
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


# ---------------------------------------------------------------------------
# Commercial reconciliation — PySpark implementation
# ---------------------------------------------------------------------------

def process_commercial_spark(
    curated_sdf: SparkDataFrame,
    sap_sdf: SparkDataFrame,
    header_mapping_sdf: SparkDataFrame,
) -> SparkDataFrame:
    """Reconcile commercial 3PL inventory against the SAP report — pure Spark.

    Equivalent to process_commercial() in transformations.py.  All three input
    DataFrames must be pre-filtered to a single segment / year / quarter
    partition (matching the semantics of load_segment_data in the notebook).

    Returns a Spark DataFrame with the same columns as process_commercial().
    Plant_Name for curated-only rows is left null here; finalise_and_write()
    in the notebook fills it via a self-join on Plant_Number, same as before.
    """

    # ------------------------------------------------------------------
    # SAP: select and aggregate
    # (Stock_Quantity and Group_Valuation_Standard_Cost are already DOUBLE
    # in the Delta schema — no cast needed)
    # ------------------------------------------------------------------
    _group_cols = [
        c for c in _SAP_INPUT_COLUMNS
        if c not in ("Stock_Quantity__Base_UOM_", "Group_Valuation_Standard_Cost")
    ]

    sap_sdf = sap_sdf.select(_SAP_INPUT_COLUMNS)
    sap_sdf = sap_sdf.groupBy(_group_cols).agg(
        F.sum("Stock_Quantity__Base_UOM_").alias("Stock_Quantity__Base_UOM_"),
        F.sum("Group_Valuation_Standard_Cost").alias("Group_Valuation_Standard_Cost"),
    )

    # ------------------------------------------------------------------
    # Filter SAP to known 3PL plant numbers
    # ------------------------------------------------------------------
    plant_numbers_sdf = (
        header_mapping_sdf
        .select(F.col("3PL").alias("Plant"))
        .distinct()
    )
    sap_sdf = sap_sdf.join(F.broadcast(plant_numbers_sdf), on="Plant", how="inner")

    # ------------------------------------------------------------------
    # Year / Quarter: both tables are pre-filtered to the same partition
    # — extract once and stamp the whole column with lit()
    # ------------------------------------------------------------------
    year_val    = curated_sdf.agg(F.first("Year",    ignorenulls=True)).collect()[0][0]
    quarter_val = curated_sdf.agg(F.first("Quarter", ignorenulls=True)).collect()[0][0]

    # ------------------------------------------------------------------
    # Outer join on (plant, material, batch)
    # Alias both sides then immediately flatten to a clean DataFrame so
    # that all subsequent withColumn / join operations are unambiguous.
    # ------------------------------------------------------------------
    cur = curated_sdf.alias("cur")
    sap = sap_sdf.alias("sap")

    join_condition = (
        (F.col("cur.3PL")                 == F.col("sap.Plant"))          &
        (F.col("cur.Gilead_Material_Code") == F.col("sap.Material_Number")) &
        (F.col("cur.Gilead_Batch_Number")  == F.col("sap.Batch_Number"))
    )

    combined = cur.join(sap, join_condition, "outer").select(
        # --- Keys resolved from both sides ---
        F.coalesce(F.col("sap.Plant"),  F.col("cur.3PL")).alias("Plant"),
        F.coalesce(F.col("cur.3PL"),    F.col("sap.Plant")).alias("3PL"),

        # --- SAP-sourced columns ---
        F.col("sap.Plant_Name").alias("Plant_Name"),
        F.col("sap.External_Material_Group").alias("External_Material_Group"),
        F.col("sap.Material_Number").alias("Material_Number"),
        # Renamed to Cost_ea now; Price_Unit retained for Cost_ea_per_unit then dropped
        F.col("sap.Group_Valuation_Standard_Price").alias("Cost_ea"),
        F.col("sap.Price_Unit").alias("Price_Unit"),
        F.col("sap.Group_Valuation_Standard_Cost").alias("Group_Valuation_Standard_Cost"),
        F.col("sap.Stock_Quantity__Base_UOM_").alias("Stock_OH"),
        F.col("sap.Base_UOM").alias("UOM"),
        F.col("sap.Batch_Number").alias("Batch_Number"),
        F.col("sap.Gilead_Receipts").alias("Gilead_Receipts"),

        # --- Curated-sourced columns ---
        F.col("cur.Gilead_Material_Code").alias("Gilead_Material_Code"),
        F.col("cur.3PL_Name").alias("3PL_Name"),
        F.col("cur.3PL_Material_Code").alias("3PL_Material_Code"),
        F.col("cur.3PL_Material_Type").alias("3PL_Material_Type"),
        F.col("cur.3PL_Type").alias("3PL_Type"),
        F.col("cur.3PL_Batch_Number").alias("3PL_Batch_Number"),
        F.col("cur.Gilead_Batch_Number").alias("Gilead_Batch_Number"),
        F.col("cur.3PL_UOM").alias("3PL_UOM"),
        F.col("cur.3PL_Quantity").alias("3PL_Quantity"),
        F.col("cur.3PL_Converted_Quantity").alias("3PL_Converted_Quantity"),
        F.col("cur.Cost").alias("Cost"),
        F.col("cur.File_Name").alias("File_Name"),
        F.col("cur.Date_Processed").alias("Date_Processed"),
        F.col("cur.Has_Error").alias("Has_Error"),
        F.col("cur.Validation_Remark").alias("Validation_Remark"),

        # --- Material description: SAP column wins; curated fills curated-only rows ---
        F.coalesce(
            F.col("sap.Material_Description_in_Uppercase_for_Matchcodes"),
            F.col("cur.Material_Description"),
        ).alias("Material_Description"),

        # --- Year / Quarter stamped from partition values ---
        F.lit(year_val).alias("Year"),
        F.lit(quarter_val).alias("Quarter"),
    )

    # ------------------------------------------------------------------
    # Derived columns
    # ------------------------------------------------------------------
    combined = (
        combined
        .withColumn("Line_item_variance_threshold_amount", F.lit(250_000))
        .withColumn(
            "Cost_ea_per_unit",
            F.col("Cost_ea").cast("double") / F.col("Price_Unit").cast("double"),
        )
        .withColumn(
            "Effective_Material_Code",
            F.coalesce(
                F.col("Material_Number"),
                F.col("Gilead_Material_Code"),
                F.col("3PL_Material_Code"),
            ),
        )
        .withColumn(
            "Effective_Batch_Number",
            F.coalesce(
                F.col("Batch_Number"),
                F.col("Gilead_Batch_Number"),
                F.col("3PL_Batch_Number"),
            ),
        )
    )

    # ------------------------------------------------------------------
    # 3PL_Type enrichment for SAP-only rows
    # (curated rows already carry 3PL_Type; coalesce fills the gap)
    # ------------------------------------------------------------------
    type_lookup = (
        header_mapping_sdf
        .select(F.col("3PL").alias("_t_3PL"), F.col("3PL_Type").alias("_sap_3PL_Type"))
        .dropDuplicates(["_t_3PL"])
    )
    combined = (
        combined
        .join(F.broadcast(type_lookup), combined["Plant"] == type_lookup["_t_3PL"], "left")
        .withColumn("3PL_Type", F.coalesce(F.col("3PL_Type"), F.col("_sap_3PL_Type")))
        .drop("_t_3PL", "_sap_3PL_Type")
    )

    # ------------------------------------------------------------------
    # Plant classification
    # ------------------------------------------------------------------
    classification_lookup = (
        header_mapping_sdf
        .select(
            F.col("3PL").alias("_c_3PL"),
            F.col("3PL_Classification").alias("Plant_Classification"),
        )
        .dropDuplicates(["_c_3PL"])
    )
    combined = (
        combined
        .join(
            F.broadcast(classification_lookup),
            combined["Plant"] == classification_lookup["_c_3PL"],
            "left",
        )
        .drop("_c_3PL")
    )

    # ------------------------------------------------------------------
    # Final rename and select  (Price_Unit dropped here — intermediate only)
    # ------------------------------------------------------------------
    return combined.select(
        F.col("Plant").alias("Plant_Number"),
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
    )

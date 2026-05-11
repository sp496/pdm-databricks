import os
import pandas as pd
from dataclasses import dataclass
from textwrap import dedent
from typing import Optional, Dict, Any

_COL_CLEAN_PATTERN = r'[ ,;{}()\n\t=]'


def quarter_end_date(year: str, quarter: str) -> str:
    quarter_end_map = {"Q1": "03-31", "Q2": "06-30", "Q3": "09-30", "Q4": "12-31"}
    quarter = quarter.upper()
    if quarter not in quarter_end_map:
        raise ValueError("quarter must be one of Q1, Q2, Q3, Q4")
    return f"{year}-{quarter_end_map[quarter]}"


@dataclass
class MappingFilePaths:
    api_mapping_file_path: str
    dp_mapping_file_path: str
    plant_name_mapping_file_path: str
    uom_master_file_path: str
    material_master_file_path: str
    lot_no_mapping_file_path: str
    material_description_file_path: str
    lot_no_master_file_path: str
    unit_cost_file_path: str
    material_type_file_path: str
    gil_receipts_file_path: str
    sap_report_file_path: str
    header_mapping_sheet_name: str
    item_mapping_sheet_name: str
    uom_mapping_sheet_name: str


class MappingDataCache:
    """Cache class to store mapping data that's loaded once and reused"""

    def __init__(self):
        self.header_mapping_df = None
        self.cmo_type_mapping_df = None
        self.plant_name_mapping_df = None
        self.item_mapping_df = None
        self.material_master_df = None
        self.lot_no_master_df = None
        self.lot_no_mapping_df = None
        self.sap_report_df = None
        self.uom_mapping_df = None
        self.uom_master_df = None
        self.unit_cost_df = None
        self.material_type_df = None
        self.material_description_df = None
        self.gil_receipts_df = None


def _load_excel_sheet(file_path: str, sheet_name: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Required mapping file not found at {file_path}")
    print(f"\t\tLoading sheet '{sheet_name}' from {file_path}...")
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
    except Exception as e:
        raise Exception(f"Failed to read sheet '{sheet_name}' from file {file_path}: {e}")
    if df is None:
        raise ValueError(f"Sheet '{sheet_name}' not found in Excel file: {file_path}")
    return df.astype(str)


def _load_and_combine_sheet(dp_path: str, api_path: str, sheet_name: str) -> pd.DataFrame:
    dp_df = _load_excel_sheet(dp_path, sheet_name).assign(**{'3PL_Type': 'DP'})
    api_df = _load_excel_sheet(api_path, sheet_name).assign(**{'3PL_Type': 'API'})
    combined = pd.concat([dp_df, api_df], ignore_index=True)
    combined.columns = combined.columns.str.replace(_COL_CLEAN_PATTERN, '_', regex=True)
    return combined


def load_mapping_files(
        file_paths: MappingFilePaths,
        year: str,
        quarter: str,
        use_starburst: bool = False,
        starburst_config: Optional[Dict[str, Any]] = None,
) -> MappingDataCache:
    """
    Load all mapping files once and return a cache object containing all the dataframes.

    When use_starburst=True, fetches live data via Starburst/Trino JDBC (dev).
    When use_starburst=False, fetches live data via Spark SQL against Databricks tables (prod).
    Both paths fall back to local files if the live query fails.

    Args:
        file_paths: Dataclass containing all file path and sheet name configuration.
        use_starburst: Set True in dev to query via Starburst/Trino; False in prod to use Spark SQL.
        starburst_config: Required when use_starburst=True. Dict with keys:
            base_url, username, password, default_catalog, default_schema.
    """
    print("Loading mapping files...")
    cache = MappingDataCache()

    spark_session = None
    try:
        import pyspark
        spark_session = pyspark.sql.SparkSession.getActiveSession()
        if spark_session:
            spark_session.sql("SELECT 1").collect()
            print("\tSpark session is available")
        else:
            print("\tNo active Spark session found")
    except Exception as e:
        print(f"\tWarning: Could not access Spark session: {e}")
        spark_session = None

    # --- Starburst/Trino setup (dev only) ---
    trino_available = False
    base_url = None
    properties = None
    default_catalog = None
    default_schema = None

    if use_starburst:
        if not starburst_config:
            print("\tuse_starburst=True but no starburst_config provided — falling back to files")
        elif not spark_session:
            print("\tuse_starburst=True but no Spark session — falling back to files")
        else:
            try:
                base_url = starburst_config.get('base_url', 'jdbc:trino://query.gilead.com:443')
                username = starburst_config.get('username')
                password = starburst_config.get('password')
                default_catalog = starburst_config.get('default_catalog', 'pdm')
                default_schema = starburst_config.get('default_schema', 'default')

                properties = {
                    "user": username,
                    "password": password,
                    "SSL": "true",
                    "driver": "io.trino.jdbc.TrinoDriver"
                }

                test_url = f"{base_url}/{default_catalog}/{default_schema}"
                test_df = spark_session.read.jdbc(
                    url=test_url,
                    table="(SELECT 1 as test) AS tmp",
                    properties=properties
                )
                if test_df.collect()[0]['test'] == 1:
                    print("\tStarburst/Trino connection is available")
                    trino_available = True
                else:
                    print("\tStarburst/Trino connection test failed")
            except Exception as e:
                print(f"\tWarning: Could not establish Starburst connection: {e}")

    def load_query_from_trino(query: str) -> pd.DataFrame:
        url = f"{base_url}/{default_catalog}/{default_schema}"
        spark_df = spark_session.read.jdbc(url=url, table=f"({query}) AS tmp", properties=properties)
        if spark_df.isEmpty():
            raise Exception("Query returned no results")
        return spark_df.toPandas().astype(str)

    def load_data_with_fallback(data_name: str, table_query: str, file_path: str,
                                file_loader_func, **loader_kwargs) -> pd.DataFrame:
        if table_query and table_query.strip():
            backend_available = trino_available if use_starburst else bool(spark_session)
            backend_name = "Starburst" if use_starburst else "Spark"

            if backend_available:
                try:
                    print(f"\t\tTrying to load {data_name} from {backend_name}...")
                    indented_query = '\n'.join(['\t\t\t' + line for line in table_query.split('\n')])
                    print(f"\t\tQuery:\n{indented_query}")

                    if use_starburst:
                        pandas_df = load_query_from_trino(table_query)
                    else:
                        pandas_df = spark_session.sql(table_query).toPandas().astype(str)

                    print(f"\t\tSuccessfully loaded {data_name} from {backend_name} ({len(pandas_df)} rows)")
                    return pandas_df
                except Exception as e:
                    error_lines = str(e).split('\n')[:2]
                    truncated = [l[:100] + "..." if len(l) > 100 else l for l in error_lines]
                    print(f"\t\tFailed to load {data_name} from {backend_name}: {chr(10).join(truncated)}")
                    print(f"\t\tFalling back to file: {file_path}")
            else:
                print(f"\t\t{backend_name} not available for {data_name}, loading from file: {file_path}")
        else:
            print(f"\t\tNo query provided for {data_name}, loading from file: {file_path}")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Required file not found: {data_name} at {file_path}")

        result_df = file_loader_func(file_path, **loader_kwargs)
        print(f"\t\tSuccessfully loaded {data_name} from file ({len(result_df)} rows)")
        return result_df

    q_end_date = quarter_end_date(year, quarter)

    # --- Shared queries (Spark SQL dialect) ---
    queries = {
        'material_master': dedent("""
            SELECT DISTINCT
                matnr
            FROM
                pdm_raw_saphana_ptd.default_s4h_hana_mara
        """).strip(),

        'lot_no_master': dedent("""
            SELECT DISTINCT
                matnr,
                charg
            FROM
                pdm_raw_saphana_ptd.default_s4h_hana_mch1
            WHERE
                curr_flag='V'
        """).strip(),

        'lot_no_mapping': dedent("""
            SELECT DISTINCT
                charg,
                matnr,
                atwrt,
                atinn
            FROM
                pdm_raw_saphana_ptd.default_s4h_hana_mch1 m
                JOIN
                    pdm_raw_saphana_ptd.default_s4h_hana_ausp a
                    ON m.cuobj_bm = a.objek
            WHERE
                m.curr_flag = 'V'
                AND a.atinn IN ('0000000828','0000000819')
        """).strip(),

        'uom_master': dedent("""
            SELECT
                mara.matnr,
                marm.meinh AS alternate_uom,
                mara.meins AS gilead_uom,
                CAST(marm.umrez AS DECIMAL(18,6)) / CAST(marm.umren AS DECIMAL(18,6)) AS conversion_factor
            FROM
                pdm_raw_saphana_ptd.default_s4h_hana_mara mara
                JOIN
                    pdm_raw_saphana_ptd.default_s4h_hana_marm marm
                    ON marm.matnr = mara.matnr
            WHERE
                marm.curr_flag = 'V'
                AND mara.curr_flag = 'V'
        """).strip(),

        'unit_cost': dedent(f"""
            WITH mbew_q AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY bwkey, matnr ORDER BY end_dt DESC) AS rnk
                FROM
                    pdm_raw_saphana_ptd.default_s4h_hana_mbew
                WHERE
                    CAST(end_dt AS DATE) <= DATE '{q_end_date}'
            )
            SELECT
                mbew.matnr AS material_number,
                mbew.bwkey plant_code,
                t001w.name1 plant_name,
                mara.meins base_uom,
                (mbew.stprs / mbew.peinh) AS standard_cost,
                DATE_FORMAT(mbew.end_dt, 'yyyyMMdd') AS standard_cost_date,
                t001.waers currency_code,
                (CASE WHEN (tcurr.ukurs < 0) THEN abs((1 / tcurr.ukurs)) ELSE tcurr.ukurs END) exchange_rate,
                CONCAT(
                    SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS STRING), 1, 4),
                    SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS STRING), 5, 2),
                    SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS STRING), 7, 2)
                ) exchange_rate_date,
                (CASE WHEN (ROUND((mbew.stprs / mbew.peinh)*(CASE WHEN (ukurs < 0) THEN ABS((1 / tcurr.ukurs)) ELSE tcurr.ukurs END), 2)) IS NULL
                    THEN (mbew.stprs / mbew.peinh)
                    ELSE (ROUND((mbew.stprs / mbew.peinh)*(CASE WHEN (ukurs < 0) THEN ABS((1 / tcurr.ukurs)) ELSE tcurr.ukurs END), 2)) END) standard_cost_usd
            FROM
                (((((mbew_q mbew
                LEFT OUTER JOIN pdm_raw_saphana_ptd.default_s4h_hana_t001k t001k ON (mbew.bwkey=t001k.bwkey))
                LEFT OUTER JOIN pdm_raw_saphana_ptd.default_s4h_hana_t001 t001 ON (t001k.bukrs=t001.bukrs))
                LEFT OUTER JOIN pdm_raw_saphana_ptd.default_s4h_hana_t001w t001w ON (mbew.bwkey=t001w.werks) AND t001w.curr_flag='V')
                LEFT OUTER JOIN pdm_raw_saphana_ptd.default_s4h_hana_mara mara ON (mbew.matnr=mara.matnr))
                LEFT OUTER JOIN pdm_raw_saphana_otc.default_s4h_hana_tcurr tcurr
                    ON (t001.waers=tcurr.fcurr AND tcurr.tcurr='USD' AND tcurr.kurst='D')
                    AND CONCAT(
                        SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS STRING), 1, 4),
                        SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS STRING), 5, 2),
                        SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS STRING), 7, 2)
                    ) = DATE_FORMAT(mbew.end_dt, 'yyyyMMdd'))
            WHERE
                mara.curr_flag='V'
                AND t001k.curr_flag='V'
                AND t001.curr_flag='V'
                AND mbew.rnk=1
        """).strip(),

        'material_description': dedent("""
            SELECT
                matnr,
                maktx Material_Description
            FROM
                pdm_raw_saphana_ptd.default_s4h_hana_makt
            WHERE
                spras='E'
                AND curr_flag='V'
        """).strip(),

        'material_type': dedent("""
            SELECT DISTINCT
                matnr,
                extwg
            FROM
                pdm_raw_saphana_ptd.default_s4h_hana_mara
            WHERE
                curr_flag='V'
        """).strip(),

        'plant_name_mapping': dedent("""
            SELECT DISTINCT
                werks plant_number,
                name1 plant_name
            FROM
                pdm_raw_saphana_ptd.default_s4h_hana_t001w t001w
            WHERE
                curr_flag='V'
        """).strip(),

        'gilead_receipts': dedent("""
            SELECT
                plant,
                material,
                batch,
                SUM(qty_movement) qty,
                uom,
                MIN(posting_date) min_posting_date,
                MAX(posting_date) max_posting_date
            FROM (
                WITH cte_mara AS (
                    SELECT
                        matnr,
                        MAX(laeda) laeda
                    FROM
                        pdm_raw_saphana_ptd.default_s4h_hana_mara
                    GROUP BY matnr
                )
                SELECT
                    matdoc.werks plant,
                    t001w.name1 plant_name,
                    matdoc.zeile item,
                    matdoc.matnr material,
                    makt.maktx material_description,
                    mara.extwg material_type,
                    matdoc.bwart movement,
                    t156ht.btext movement_description,
                    matdoc.mblnr material_document,
                    matdoc.charg batch,
                    matdoc.shkzg stock_change,
                    matdoc.menge qty_in_unit_of_entry,
                    (CASE WHEN (matdoc.shkzg = 'H') THEN (matdoc.menge * -1) ELSE matdoc.menge END) qty_movement,
                    matdoc.meins uom,
                    matdoc.budat posting_date
                FROM
                    (((((pdm_raw_saphana_ptd.default_s4h_hana_matdoc matdoc
                    INNER JOIN pdm_raw_saphana_ptd.default_s4h_hana_mara mara ON (matdoc.matnr = mara.matnr))
                    INNER JOIN cte_mara ON ((mara.matnr = cte_mara.matnr) AND (mara.laeda = cte_mara.laeda) AND (mara.curr_flag = 'V')))
                    LEFT JOIN pdm_raw_saphana_ptd.default_s4h_hana_t001w t001w ON ((matdoc.werks = t001w.werks) AND (t001w.curr_flag = 'V')))
                    LEFT JOIN pdm_raw_saphana_ptd.default_s4h_hana_t156ht t156ht ON ((matdoc.bwart = t156ht.bwart) AND (t156ht.spras = 'E')))
                    LEFT JOIN pdm_raw_saphana_ptd.default_s4h_hana_makt makt ON ((matdoc.matnr = makt.matnr) AND (makt.spras = 'E') AND (makt.curr_flag = 'V')))
                WHERE
                    (matdoc.matnr IS NOT NULL)
                    AND (
                        matdoc.bwart IN ('101','102','107','109','110','501','502','503','504','505','506',
                                         '561','562','601','602','701','702','703','704','707','708',
                                         '711','712','713','714','715','716','717','718','901','902')
                        OR ((matdoc.bwart = '68C') AND (matdoc.shkzg = 'S'))
                    )
            )
            GROUP BY plant, material, batch, uom
        """).strip(),
    }

    # Starburst/Trino dialect override — only unit_cost differs (VARCHAR vs STRING cast)
    if use_starburst:
        queries.update({
            'unit_cost': dedent(f"""
                WITH mbew_q AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER(PARTITION BY bwkey, matnr ORDER BY end_dt DESC) AS rnk
                    FROM
                        pdm_raw_saphana_ptd.default_s4h_hana_mbew
                    WHERE
                        CAST(end_dt AS DATE) <= DATE '{q_end_date}'
                )
                SELECT
                    mbew.matnr AS material_number,
                    mbew.bwkey plant_code,
                    t001w.name1 plant_name,
                    mara.meins base_uom,
                    (mbew.stprs / mbew.peinh) AS standard_cost,
                    DATE_FORMAT(mbew.end_dt, 'yyyyMMdd') AS standard_cost_date,
                    t001.waers currency_code,
                    (CASE WHEN (tcurr.ukurs < 0) THEN abs((1 / tcurr.ukurs)) ELSE tcurr.ukurs END) exchange_rate,
                    CONCAT(
                        SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS VARCHAR), 1, 4),
                        SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS VARCHAR), 5, 2),
                        SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS VARCHAR), 7, 2)
                    ) exchange_rate_date,
                    (CASE WHEN (ROUND((mbew.stprs / mbew.peinh)*(CASE WHEN (ukurs < 0) THEN ABS((1 / tcurr.ukurs)) ELSE tcurr.ukurs END), 2)) IS NULL
                        THEN (mbew.stprs / mbew.peinh)
                        ELSE (ROUND((mbew.stprs / mbew.peinh)*(CASE WHEN (ukurs < 0) THEN ABS((1 / tcurr.ukurs)) ELSE tcurr.ukurs END), 2)) END) standard_cost_usd
                FROM
                    (((((mbew_q mbew
                    LEFT OUTER JOIN pdm_raw_saphana_ptd.default_s4h_hana_t001k t001k ON (mbew.bwkey=t001k.bwkey))
                    LEFT OUTER JOIN pdm_raw_saphana_ptd.default_s4h_hana_t001 t001 ON (t001k.bukrs=t001.bukrs))
                    LEFT OUTER JOIN pdm_raw_saphana_ptd.default_s4h_hana_t001w t001w ON (mbew.bwkey=t001w.werks) AND t001w.curr_flag='V')
                    LEFT OUTER JOIN pdm_raw_saphana_ptd.default_s4h_hana_mara mara ON (mbew.matnr=mara.matnr))
                    LEFT OUTER JOIN pdm_raw_saphana_otc.default_s4h_hana_tcurr tcurr
                        ON (t001.waers=tcurr.fcurr AND tcurr.tcurr='USD' AND tcurr.kurst='D')
                        AND CONCAT(
                            SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS VARCHAR), 1, 4),
                            SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS VARCHAR), 5, 2),
                            SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS VARCHAR), 7, 2)
                        ) = DATE_FORMAT(mbew.end_dt, 'yyyyMMdd'))
                WHERE
                    mara.curr_flag='V'
                    AND t001k.curr_flag='V'
                    AND t001.curr_flag='V'
                    AND mbew.rnk=1
            """).strip(),
        })

    # Load header, item, and UOM mappings from files (always)
    try:
        print("\tLoading header mappings...")
        cache.header_mapping_df = _load_and_combine_sheet(
            file_paths.dp_mapping_file_path,
            file_paths.api_mapping_file_path,
            file_paths.header_mapping_sheet_name
        )
        print(f"\tSuccessfully loaded and combined Header Mappings ({len(cache.header_mapping_df)} rows)")

        print("\tDeriving CMO type mapping...")
        cache.cmo_type_mapping_df = cache.header_mapping_df[['3PL', '3PL_Type']].drop_duplicates(
            ignore_index=True).astype(str)

    except FileNotFoundError as e:
        print(f"\tFailed to load Header Mappings (File Not Found): {e}")
        raise
    except Exception as e:
        print(f"\tFailed to load Header Mappings: {e}")
        raise

    try:
        print("\tLoading Item Mappings...")
        cache.item_mapping_df = _load_and_combine_sheet(
            file_paths.dp_mapping_file_path,
            file_paths.api_mapping_file_path,
            file_paths.item_mapping_sheet_name
        )
        print(f"\tSuccessfully loaded and combined Item Mappings ({len(cache.item_mapping_df)} rows)")

    except FileNotFoundError as e:
        print(f"\tFailed to load Item Mappings (File Not Found): {e}")
        raise
    except Exception as e:
        print(f"\tFailed to load Item Mappings: {e}")
        raise

    try:
        print("\tLoading UOM Mappings...")
        cache.uom_mapping_df = _load_and_combine_sheet(
            file_paths.dp_mapping_file_path,
            file_paths.api_mapping_file_path,
            file_paths.uom_mapping_sheet_name
        )
        print(f"\tSuccessfully loaded and combined UOM Mappings ({len(cache.uom_mapping_df)} rows)")

    except FileNotFoundError as e:
        print(f"\tFailed to load UOM Mappings (File Not Found): {e}")
        raise
    except Exception as e:
        print(f"\tFailed to load UOM Mappings: {e}")
        raise

    # Load SAP report (always from file)
    print("\tLoading SAP report from file...")
    if not os.path.exists(file_paths.sap_report_file_path):
        raise FileNotFoundError(f"Required file not found: sap_report at {file_paths.sap_report_file_path}")
    cache.sap_report_df = pd.read_csv(file_paths.sap_report_file_path, dtype=str)
    cache.sap_report_df.columns = cache.sap_report_df.columns.str.replace(_COL_CLEAN_PATTERN, '_', regex=True)

    print("\tLoading Plant name mapping...")
    cache.plant_name_mapping_df = load_data_with_fallback(
        'plant_name_mapping',
        queries.get('plant_name_mapping', ''),
        file_paths.plant_name_mapping_file_path,
        pd.read_csv,
        dtype=str
    )

    print("\tLoading material master...")
    cache.material_master_df = load_data_with_fallback(
        'material_master',
        queries.get('material_master', ''),
        file_paths.material_master_file_path,
        pd.read_csv,
        dtype=str
    )

    print("\tLoading lot number mapping files...")
    cache.lot_no_master_df = load_data_with_fallback(
        'lot_no_master',
        queries.get('lot_no_master', ''),
        file_paths.lot_no_master_file_path,
        pd.read_csv,
        dtype=str
    )

    cache.lot_no_mapping_df = load_data_with_fallback(
        'lot_no_mapping',
        queries.get('lot_no_mapping', ''),
        file_paths.lot_no_mapping_file_path,
        pd.read_csv,
        dtype=str
    )

    print("\tLoading Material Description Mapping Data...")
    cache.material_description_df = load_data_with_fallback(
        'material_description',
        queries.get('material_description', ''),
        file_paths.material_description_file_path,
        pd.read_csv,
        dtype=str
    )

    print("\tLoading UOM master...")
    cache.uom_master_df = load_data_with_fallback(
        'uom_master',
        queries.get('uom_master', ''),
        file_paths.uom_master_file_path,
        pd.read_csv,
        dtype=str
    )

    print("\tLoading unit cost data...")
    cache.unit_cost_df = load_data_with_fallback(
        'unit_cost',
        queries.get('unit_cost', ''),
        file_paths.unit_cost_file_path,
        pd.read_csv,
        dtype=str
    )

    print("\tLoading Material Type Data...")
    cache.material_type_df = load_data_with_fallback(
        'material_type',
        queries.get('material_type', ''),
        file_paths.material_type_file_path,
        pd.read_csv,
        dtype=str
    )

    print("\tLoading Gilead receipts...")
    cache.gil_receipts_df = load_data_with_fallback(
        'gil_receipts',
        queries.get('gilead_receipts', ''),
        file_paths.gil_receipts_file_path,
        pd.read_csv,
        dtype=str
    )

    print("All mapping files loaded successfully!")
    return cache

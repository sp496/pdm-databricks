from textwrap import dedent
from typing import Dict


def get_queries(q_end_date: str, data_source: str = "spark") -> Dict[str, str]:
    """
    Return the full set of SQL queries for loading reference data.

    Spark SQL (prod) uses STRING casts; Starburst/Trino (dev) uses VARCHAR.
    data_source='file' still returns queries (they won't be used, but the dict
    is always complete for consistency).
    """
    cast_str = "VARCHAR" if data_source == "starburst" else "STRING"

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
                    SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS {cast_str}), 1, 4),
                    SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS {cast_str}), 5, 2),
                    SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS {cast_str}), 7, 2)
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
                        SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS {cast_str}), 1, 4),
                        SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS {cast_str}), 5, 2),
                        SUBSTRING(CAST((99999999 - CAST(tcurr.gdatu AS INT)) AS {cast_str}), 7, 2)
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

    }

    return queries

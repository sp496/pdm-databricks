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

    return queries

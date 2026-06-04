from textwrap import dedent
from typing import Dict, Iterable, Optional


def get_clinical_inventory_query(inventory_orgs: Optional[Iterable[str]] = None,
                                 data_source: str = "spark") -> str:
    """Build the EBS clinical inventory staging query.

    Pulls lot-level on-hand inventory from EBS (apps_mtl_* tables), with
    reservations subtracted to yield an available_quantity per lot.

    Args:
        inventory_orgs: iterable of organization_code values to filter on
            (typically the distinct 3PL plants present in the curated table
             for the target quarter / segment='clinical'). If None, NO org
            filter is applied and all clinical inventory orgs are returned.
            If a non-None iterable is supplied it must contain at least one
            non-empty value (guards against a "discovered zero orgs" bug in
            the filtered staging path).
        data_source: 'spark' or 'starburst' — kept for parity with get_queries
            (the query body is the same for both).

    Returns: a SQL string ready for DataBackend.run_query().
    """
    if inventory_orgs is None:
        org_filter = ""
    else:
        orgs = [str(o).strip() for o in inventory_orgs if str(o).strip()]
        if not orgs:
            raise ValueError("inventory_orgs must contain at least one non-empty value")
        orgs_list = ", ".join(f"'{o}'" for o in orgs)
        org_filter = f"\n        WHERE inv.inventory_org IN ({orgs_list})"

    return dedent(f"""
        WITH inventory AS (
            SELECT
                hou.name                              AS operating_unit_name,
                xep.name                              AS legal_entity_name,
                mp.organization_code                  AS inventory_org,
                ood.organization_name                 AS inventory_org_name,
                mcv.segment2                          AS material_group,
                msi.segment1                          AS item,
                mtltl.description                     AS description,
                mln.lot_number                        AS lot_number,
                mmst.status_code                      AS lot_status,
                mln.expiration_date                   AS lot_expiry_date,
                mln.retest_date                       AS lot_retest_date,
                msi.primary_uom_code                  AS primary_uom,
                SUM(moq.transaction_quantity)         AS onhand_quantity,
                moq.subinventory_code                 AS subinventory_code,
                msi.inventory_item_id                 AS inventory_item_id,
                msi.organization_id                   AS organization_id
            FROM pdm.ebs_processed.apps_mtl_system_items_b msi
            JOIN pdm.ebs_processed.apps_mtl_parameters mp
                ON msi.organization_id = mp.organization_id
            JOIN pdm.ebs_processed.apps_mtl_onhand_quantities moq
                ON msi.inventory_item_id = moq.inventory_item_id
               AND msi.organization_id   = moq.organization_id
            LEFT JOIN pdm.ebs_processed.apps_mtl_lot_numbers mln
                ON moq.inventory_item_id = mln.inventory_item_id
               AND moq.organization_id   = mln.organization_id
               AND moq.lot_number        = mln.lot_number
            LEFT JOIN pdm.ebs_processed.apps_mtl_material_statuses_tl mmst
                ON moq.status_id = mmst.status_id
               AND mmst.language = 'US'
            JOIN pdm.ebs_processed.apps_mtl_system_items_tl mtltl
                ON mtltl.inventory_item_id = msi.inventory_item_id
               AND mtltl.organization_id   = msi.organization_id
               AND mtltl.language          = 'US'
            JOIN pdm.ebs_processed.apps_org_organization_definitions ood
                ON mp.organization_id = ood.organization_id
            JOIN pdm.ebs_processed.apps_hr_operating_units hou
                ON ood.operating_unit = hou.organization_id
            JOIN pdm.ebs_processed.apps_xle_entity_profiles xep
                ON CAST(hou.default_legal_context_id AS INTEGER) = xep.legal_entity_id
            JOIN pdm.ebs_processed.apps_mtl_item_categories mic
                ON mic.inventory_item_id = msi.inventory_item_id
               AND mic.organization_id   = msi.organization_id
            JOIN pdm.ebs_processed.apps_mtl_category_sets_tl mcs
                ON mcs.category_set_id   = mic.category_set_id
               AND mcs.category_set_name = 'Inventory'
               AND mcs.language          = 'US'
            JOIN pdm.ebs_processed.apps_mtl_categories_vl mcv
                ON mcv.category_id = mic.category_id
            GROUP BY
                hou.organization_id,
                hou.name,
                xep.name,
                mp.organization_code,
                ood.organization_name,
                mcv.segment1,
                mcv.segment2,
                mcv.segment3,
                msi.segment1,
                mtltl.description,
                mln.lot_number,
                mmst.status_code,
                mln.expiration_date,
                mln.retest_date,
                mln.creation_date,
                msi.primary_uom_code,
                msi.inventory_item_id,
                moq.subinventory_code,
                msi.organization_id
            HAVING SUM(moq.transaction_quantity) > 0
        ),
        reserve AS (
            SELECT
                SUM(mr.primary_reservation_quantity) AS reservation_quantity,
                mr.inventory_item_id,
                mr.organization_id,
                mr.lot_number
            FROM pdm.ebs_processed.apps_mtl_reservations mr
            GROUP BY mr.inventory_item_id, mr.organization_id, mr.lot_number
        )
        SELECT
            inv.operating_unit_name,
            inv.legal_entity_name,
            inv.inventory_org,
            inv.inventory_org_name,
            inv.material_group,
            inv.item,
            inv.description,
            inv.lot_number,
            inv.lot_status,
            inv.lot_expiry_date,
            inv.lot_retest_date,
            inv.primary_uom,
            inv.onhand_quantity,
            COALESCE(res.reservation_quantity, 0)                                AS reservation_quantity,
            (inv.onhand_quantity - COALESCE(res.reservation_quantity, 0))        AS available_quantity,
            inv.subinventory_code,
            inv.inventory_item_id,
            inv.organization_id
        FROM inventory inv
        LEFT JOIN reserve res
            ON inv.inventory_item_id = res.inventory_item_id
           AND inv.organization_id   = res.organization_id
           AND inv.lot_number        = res.lot_number{org_filter}
    """).strip()


def get_clinical_queries(data_source: str = "spark") -> Dict[str, str]:
    """Return the EBS-sourced reference queries for the clinical segment.

    Same dict keys as get_queries() so the curated pipeline is segment-agnostic,
    EXCEPT 'unit_cost' which is intentionally omitted — EBS has no unit-cost
    source, so clinical curated rows leave Cost null (handled in
    curated_processing by guarding on mapping_cache.unit_cost_df is None).

    Each query aliases its EBS columns to the SAME canonical names the
    transforms in transformations.py already expect (matnr, charg, atwrt,
    alternate_uom/gilead_uom/conversion_factor, plant_number/plant_name, extwg,
    Material_Description), so the commercial transformation functions are reused
    unchanged.

    data_source is accepted for parity with get_queries (no casts are needed in
    these queries).
    """
    return {
        'plant_name_mapping': dedent("""
            SELECT DISTINCT
                mp.organization_code   AS plant_number,
                ood.organization_name  AS plant_name
            FROM pdm.ebs_processed.apps_mtl_parameters mp
            JOIN pdm.ebs_processed.apps_org_organization_definitions ood
                ON mp.organization_id = ood.organization_id
        """).strip(),

        'material_master': dedent("""
            SELECT DISTINCT
                segment1 AS matnr
            FROM pdm.ebs_processed.apps_mtl_system_items_b
        """).strip(),

        'lot_no_master': dedent("""
            SELECT DISTINCT
                msi.segment1   AS matnr,
                mln.lot_number AS charg
            FROM pdm.ebs_processed.apps_mtl_lot_numbers mln
            JOIN pdm.ebs_processed.apps_mtl_system_items_b msi
                ON mln.inventory_item_id = msi.inventory_item_id
               AND mln.organization_id   = msi.organization_id
        """).strip(),

        'lot_no_mapping': dedent("""
            SELECT DISTINCT
                mln.lot_number AS charg,
                msi.segment1   AS matnr,
                mln.c_attribute1 AS atwrt
            FROM pdm.ebs_processed.apps_mtl_lot_numbers mln
            JOIN pdm.ebs_processed.apps_mtl_system_items_b msi
                ON mln.inventory_item_id = msi.inventory_item_id
               AND mln.organization_id   = msi.organization_id
            WHERE mln.lot_attribute_category IS NULL
        """).strip(),

        'uom_master': dedent("""
            SELECT
                msi.segment1         AS matnr,
                muc.uom_code         AS alternate_uom,
                msi.primary_uom_code AS gilead_uom,
                muc.conversion_rate  AS conversion_factor
            FROM pdm.ebs_processed.apps_mtl_uom_conversions muc
            JOIN pdm.ebs_processed.apps_mtl_system_items_b msi
                ON msi.inventory_item_id = muc.inventory_item_id
            WHERE msi.organization_id = 131
        """).strip(),

        'material_description': dedent("""
            SELECT DISTINCT
                segment1    AS matnr,
                description AS Material_Description
            FROM pdm.ebs_processed.apps_mtl_system_items_b
        """).strip(),

        'material_type': dedent("""
            SELECT DISTINCT
                msi.segment1  AS matnr,
                msi.item_type AS extwg
            FROM pdm.ebs_processed.apps_mtl_system_items_b msi
        """).strip(),
    }


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

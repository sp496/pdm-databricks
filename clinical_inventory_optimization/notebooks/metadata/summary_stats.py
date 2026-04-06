# Databricks notebook source
import os

# COMMAND ----------

env = dbutils.widgets.get("DATAENV")
print(f"Environment: {env}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Metadata

# COMMAND ----------

df = spark.sql(f"""
SELECT 
    * 
FROM 
    `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_studylist_status`
WHERE 
    extract_date = (SELECT MAX(extract_date) FROM `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_studylist_status`)
    AND study_protocol in (
"CB8025-41837",
"EDGE-Lung",
"GS-US-174-0144",
"GS-US-200-4625",
"GS-US-200-6712",
"GS-US-216-0128",
"GS-US-320-1092",
"GS-US-380-1474",
"GS-US-380-5578",
"GS-US-409-5704",
"GS-US-412-2055",
"GS-US-412-5624",
"GS-US-457-6411",
"GS-US-497-6486",
"GS-US-521-6317",
"GS-US-528-6020",
"GS-US-528-6363",
"GS-US-528-6727",
"GS-US-528-9023",
"GS-US-536-5939",
"GS-US-544-5905-04_05",
"GS-US-563-5925",
"GS-US-563-5926",
"GS-US-563-6041",
"GS-US-567-6968",
"GS-US-569-6172",
"GS-US-570-6015",
"GS-US-576-6220",
"GS-US-576-7321",
"GS-US-577-6153",
"GS-US-579-6764",
"GS-US-592-6173",
"GS-US-592-6238",
"GS-US-595-6184",
"GS-US-598-6168",
"GS-US-600-6165",
"GS-US-621-6289",
"GS-US-621-6290",
"GS-US-621-6463",
"GS-US-626-6216",
"GS-US-666-6692",
"GS-US-667-6882",
"GS-US-682-6769",
"GS-US-686-6854",
"GS-US-699-7184",
"GS-US-707-7297",
"GS-US-712-7286"
)
          """)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Study load summary

# COMMAND ----------

df = spark.sql(f"""
SELECT
    'clinical_depot_inventory' AS table_name,
    MAX(extract_date) AS max_extract_date,
    COUNT(*) AS row_count_at_max_extract_date,
    COUNT(DISTINCT study_protocol) AS distinct_study_count
FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_depot_inventory
WHERE
extract_date = (
    SELECT MAX(extract_date)
    FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_depot_inventory
)

UNION ALL

SELECT
    'clinical_site_inventory' AS table_name,
    MAX(extract_date) AS max_extract_date,
    COUNT(*) AS row_count_at_max_extract_date,
    COUNT(DISTINCT study_protocol) AS distinct_study_count
FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_site_inventory
WHERE extract_date = (
    SELECT MAX(extract_date)
    FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_site_inventory
)

UNION ALL

SELECT
    'clinical_subject_summary' AS table_name,
    MAX(extract_date) AS max_extract_date,
    COUNT(*) AS row_count_at_max_extract_date,
    COUNT(DISTINCT study_protocol) AS distinct_study_count
FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_subject_summary
WHERE extract_date = (
    SELECT MAX(extract_date)
    FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_subject_summary
)

UNION ALL

SELECT
    'country_supplymethod' AS table_name,
    MAX(extract_date) AS max_extract_date,
    COUNT(*) AS row_count_at_max_extract_date,
    COUNT(DISTINCT study_protocol) AS distinct_study_count
FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_supply_method_country_level
WHERE extract_date = (
    SELECT MAX(extract_date)
    FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_supply_method_country_level
)

UNION ALL

SELECT
    'site_supplymethod' AS table_name,
    MAX(extract_date) AS max_extract_date,
    COUNT(*) AS row_count_at_max_extract_date,
    COUNT(DISTINCT study_protocol) AS distinct_study_count
FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_supply_method_site_level
WHERE extract_date = (
    SELECT MAX(extract_date)
    FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_supply_method_site_level
)
;
          """)

df.display()

# COMMAND ----------

df = spark.sql(f"""
WITH 
depot AS (
    SELECT DISTINCT study_protocol
    FROM `pdm-pdm-gsc-bi-dev`.clinical_inventory.clinical_depot_inventory
    WHERE
        extract_date = (
            SELECT MAX(extract_date)
            FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_depot_inventory
        )
),

site AS (
    SELECT DISTINCT study_protocol
    FROM `pdm-pdm-gsc-bi-dev`.clinical_inventory.clinical_site_inventory
    WHERE
        extract_date = (
            SELECT MAX(extract_date)
            FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_site_inventory
        )
),

subject AS (
    SELECT DISTINCT study_protocol
    FROM `pdm-pdm-gsc-bi-dev`.clinical_inventory.clinical_subject_summary
    WHERE
        extract_date = (
            SELECT MAX(extract_date)
            FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_subject_summary
        )
),

supply_site AS (
    SELECT DISTINCT study_protocol
    FROM `pdm-pdm-gsc-bi-dev`.clinical_inventory.clinical_supply_method_site_level
    WHERE
        extract_date = (
            SELECT MAX(extract_date)
            FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_supply_method_site_level
        )
),

supply_country AS (
    SELECT DISTINCT study_protocol
    FROM `pdm-pdm-gsc-bi-dev`.clinical_inventory.clinical_supply_method_country_level
    WHERE
        extract_date = (
            SELECT MAX(extract_date)
            FROM `pdm-pdm-gsc-bi-{env}`.clinical_inventory.clinical_supply_method_country_level
        )
),

demand AS (
    SELECT DISTINCT study_name
    FROM `pdm-pdm-gsc-bi-dev`.clinical_inventory.clinical_demand_plan
)

SELECT
    d.study_protocol   AS depot_study,
    s.study_protocol   AS site_study,
    sub.study_protocol AS subject_study,
    dem.study_name AS demand_study,
    ss.study_protocol  AS supply_site_study,
    sc.study_protocol  AS supply_country_study

FROM depot d

FULL OUTER JOIN site s
    ON d.study_protocol = s.study_protocol

FULL OUTER JOIN subject sub
    ON COALESCE(d.study_protocol, s.study_protocol) = sub.study_protocol

FULL OUTER JOIN supply_site ss
    ON COALESCE(d.study_protocol, s.study_protocol, sub.study_protocol) = ss.study_protocol

FULL OUTER JOIN supply_country sc
    ON COALESCE(d.study_protocol, s.study_protocol, sub.study_protocol, ss.study_protocol) = sc.study_protocol

FULL OUTER JOIN demand dem
    ON COALESCE(d.study_protocol, s.study_protocol, sub.study_protocol, ss.study_protocol, sc.study_protocol) = dem.study_name
-- WHERE
--    sub.study_protocol in  ( 
--                         -- "GS-US-412-5624",
--                         -- "GS-US-521-6317",
--                         -- "GS-US-528-6020",
--                         -- "GS-US-528-6363",
--                         -- "GS-US-528-9023",
--                         -- "GS-US-563-5926",
--                         -- "GS-US-577-6153",
--                         -- "GS-US-592-6173",
--                         -- "GS-US-592-6238",
--                         -- "GS-US-595-6184",
--                         -- "GS-US-598-6168",
--                         -- "GS-US-600-6165",
--                         -- "GS-US-626-6216",
--                         -- "GS-US-682-6769",
--                         -- "GS-US-200-6712",
--                         -- "GS-US-570-6015",
--                         -- "GS-US-569-6172",
--                         -- "GS-US-576-6220",
--                         -- "GS-US-576-7321",
--                         -- 'GS-US-567-6968',
--                         -- 'GS-US-579-6764',
--                         -- 'GS-US-699-7184',
--                         -- 'GS-US-707-7297',
--                         -- 'GS-US-712-7286'

--                         -- 'GS-US-174-0144',
--                         'GS-US-200-4625',
--                         -- 'GS-US-216-0128_A',
--                         'GS-US-380-1474',
--                         'GS-US-380-5578',
--                         'GS-US-409-5704',
--                         'GS-US-412-2055',
--                         -- 'GS-US-457-6411',
--                         'GS-US-528-6727',
--                         -- 'GS-US-536-5939',
--                         'GS-US-544-5905-04_05',
--                         'GS-US-563-5925',
--                         'GS-US-563-6041',
--                         -- 'GS-US-621-6463',
--                         'GS-US-621-6289',
--                         'GS-US-621-6290',
--                         'GS-US-666-6692',
--                         'GS-US-667-6882',
--                         'GS-US-686-6854'
--                     )

ORDER BY
    COALESCE(
        d.study_protocol,
        s.study_protocol,
        sub.study_protocol,
        ss.study_protocol,
        sc.study_protocol,
        dem.study_name
    );
          """)

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `pdm-pdm-gsc-bi-dev`.clinical_inventory.clinical_depot_inventory
# MAGIC where
# MAGIC extract_date=(select max(extract_date) from `pdm-pdm-gsc-bi-dev`.clinical_inventory.clinical_depot_inventory)
# MAGIC and study_protocol = 'GS-US-412-2055'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `pdm-pdm-gsc-bi-dev`.clinical_inventory.clinical_site_inventory
# MAGIC where
# MAGIC extract_date=(select max(extract_date) from `pdm-pdm-gsc-bi-dev`.clinical_inventory.clinical_site_inventory)
# MAGIC and
# MAGIC study_protocol = 'GS-US-563-6041'

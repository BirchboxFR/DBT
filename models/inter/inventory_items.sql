{{ config(
    materialized='table',
    partition_by={
      "field": "id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 300000,
        "interval": 75
      }
    },
    cluster_by=['dw_country_code', 'company_id']
) }}



SELECT 'FR' AS dw_country_code, * FROM `bdd_prod_fr.wp_jb_inventory_items` 
UNION ALL 
SELECT 'DE' AS dw_country_code, * FROM `bdd_prod_de.wp_jb_inventory_items` 
UNION ALL 
SELECT 'ES' AS dw_country_code, * FROM `bdd_prod_es.wp_jb_inventory_items` 
UNION ALL 
SELECT 'IT' AS dw_country_code, * FROM `bdd_prod_it.wp_jb_inventory_items`

{{ config(
    materialized='table',
    partition_by={
      "field": "id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 1000000000,
        "interval": 100000
      }
    },
    cluster_by=['dw_country_code', 'code','parent_offer_id','validity_date']
) }}



SELECT 'FR' AS dw_country_code, * FROM `bdd_prod_fr.wp_jb_sub_offers` 
UNION ALL 
SELECT 'DE' AS dw_country_code, * FROM `bdd_prod_de.wp_jb_sub_offers` 
UNION ALL 
SELECT 'ES' AS dw_country_code, * FROM `bdd_prod_es.wp_jb_sub_offers` 
UNION ALL 
SELECT 'IT' AS dw_country_code, * FROM `bdd_prod_it.wp_jb_sub_offers`



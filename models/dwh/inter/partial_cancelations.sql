

{{ config(
    materialized='table',
    partition_by={
      "field": "order_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 50000000,
        "interval": 12500
      }
    },
    cluster_by=['dw_country_code']
) }}

SELECT 'FR' AS dw_country_code, * FROM `bdd_prod_fr.wp_jb_partial_cancelations` 
UNION ALL 
SELECT 'DE' AS dw_country_code, * FROM `bdd_prod_de.wp_jb_partial_cancelations` 
UNION ALL 
SELECT 'ES' AS dw_country_code, * FROM `bdd_prod_es.wp_jb_partial_cancelations` 
UNION ALL 
SELECT 'IT' AS dw_country_code, * FROM `bdd_prod_it.wp_jb_partial_cancelations`




{{ config(
    materialized='table',
    partition_by={
      "field": "comment_post_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 5000000,
        "interval": 1250
      }
    },
    cluster_by=['dw_country_code']
) }}


SELECT 'FR' AS dw_country_code, * FROM `bdd_prod_fr.wp_comments` 
UNION ALL 
SELECT 'DE' AS dw_country_code, * FROM `bdd_prod_de.wp_comments` 
UNION ALL 
SELECT 'ES' AS dw_country_code, * FROM `bdd_prod_es.wp_comments` 
UNION ALL 
SELECT 'IT' AS dw_country_code, * FROM `bdd_prod_it.wp_comments`



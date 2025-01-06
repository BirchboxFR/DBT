{{ config(
    materialized='table',
    partition_by={
      "field": "box_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 300,
        "interval": 1
      }
    },
    cluster_by=['dw_country_code']
) }}



SELECT 'FR' AS dw_country_code, t.* FROM `bdd_prod_fr.wp_jb_choose_users` t
UNION ALL
SELECT 'DE' AS dw_country_code, t.* FROM `bdd_prod_de.wp_jb_choose_users` t
UNION ALL
SELECT 'ES' AS dw_country_code, t.* FROM `bdd_prod_es.wp_jb_choose_users` t
UNION ALL
SELECT 'IT' AS dw_country_code, t.* FROM `bdd_prod_it.wp_jb_choose_users` t

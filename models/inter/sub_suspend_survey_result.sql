{{ config(
    materialized='table',
    partition_by={
      "field": "result_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 3000000,
        "interval": 900
      }
    },
    cluster_by=['dw_country_code','last_received_box_id']
) }}


SELECT 'FR' AS dw_country_code, * FROM `bdd_prod_fr.wp_jb_sub_suspend_survey_result` 
UNION ALL 
SELECT 'DE' AS dw_country_code, * FROM `bdd_prod_de.wp_jb_sub_suspend_survey_result` 
UNION ALL 
SELECT 'ES' AS dw_country_code, * FROM `bdd_prod_es.wp_jb_sub_suspend_survey_result` 
UNION ALL 
SELECT 'IT' AS dw_country_code, * FROM `bdd_prod_it.wp_jb_sub_suspend_survey_result`



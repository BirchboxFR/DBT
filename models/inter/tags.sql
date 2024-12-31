

{{ config(
    materialized='table',
    partition_by={
      "field": "link_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 100000000,
        "interval": 25000
      }
    },
    cluster_by=['dw_country_code']
) }}


SELECT 'FR' as dw_country_code,user_id,id,timestamp,value,link_id,type FROM `teamdata-291012.bdd_prod_fr.wp_jb_tags` 
union all
SELECT 'DE' as dw_country_code,user_id,id,timestamp,value,link_id,type FROM `teamdata-291012.bdd_prod_de.wp_jb_tags` 
union all
SELECT 'ES' as dw_country_code,user_id,id,timestamp,value,link_id,type FROM `teamdata-291012.bdd_prod_es.wp_jb_tags` 
union all
SELECT 'IT' as dw_country_code,user_id,id,timestamp,value,link_id,type FROM `teamdata-291012.bdd_prod_es.wp_jb_tags` 

{{ config(
    materialized='table',
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}


SELECT 'FR' AS dw_country_code, * FROM `bdd_prod_fr.wp_jb_product_warehouse_location` 
UNION ALL 
SELECT 'DE' AS dw_country_code, * FROM `bdd_prod_de.wp_jb_product_warehouse_location` 
UNION ALL 
SELECT 'ES' AS dw_country_code, * FROM `bdd_prod_es.wp_jb_product_warehouse_location` 
UNION ALL 
SELECT 'IT' AS dw_country_code, * FROM `bdd_prod_it.wp_jb_product_warehouse_location`



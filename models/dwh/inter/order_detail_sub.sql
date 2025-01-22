{{ config(
    materialized='table',
    partition_by={
      "field": "order_detail_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 50000000,
        "interval": 12500
      }
    },
    cluster_by=['dw_country_code', 'box_id']
) }}


SELECT 'FR' AS dw_country_code, s.*except(last_payment_date,next_payment_date) ,safe_cast(last_payment_date as date) as last_payment_date,
safe_cast(next_payment_date as date) as next_payment_date FROM `bdd_prod_fr.wp_jb_order_detail_sub` s
UNION ALL 
SELECT 'DE' AS dw_country_code, s.*except(last_payment_date,next_payment_date) ,safe_cast(last_payment_date as date) as last_payment_date,
safe_cast(next_payment_date as date) as next_payment_date FROM `bdd_prod_de.wp_jb_order_detail_sub` s
UNION ALL 
SELECT 'ES' AS dw_country_code, s.*except(last_payment_date,next_payment_date) ,safe_cast(last_payment_date as date) as last_payment_date,
safe_cast(next_payment_date as date) as next_payment_date FROM `bdd_prod_es.wp_jb_order_detail_sub` s
UNION ALL 
SELECT 'IT' AS dw_country_code, s.*except(last_payment_date,next_payment_date) ,safe_cast(last_payment_date as date) as last_payment_date,
safe_cast(next_payment_date as date) as next_payment_date FROM `bdd_prod_it.wp_jb_order_detail_sub` s


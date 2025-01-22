{{ config(
    materialized='table',
    partition_by={
      "field": "id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 5000000,
        "interval": 1250
      }
    },
    cluster_by=['dw_country_code']
) }}

SELECT 'FR' AS dw_country_code, u.*except(user_birthday),
safe_cast(user_birthday as date) as user_birthday FROM `bdd_prod_fr.wp_users` u
UNION ALL 
SELECT 'DE' AS dw_country_code, u.*except(user_birthday),
safe_cast(user_birthday as date) as user_birthday FROM `bdd_prod_de.wp_users` u
UNION ALL 
SELECT 'ES' AS dw_country_code, u.*except(user_birthday),
safe_cast(user_birthday as date) as user_birthday FROM `bdd_prod_es.wp_users` u
UNION ALL 
SELECT 'IT' AS dw_country_code, u.*except(user_birthday),
safe_cast(user_birthday as date) as user_birthday FROM `bdd_prod_it.wp_users` u
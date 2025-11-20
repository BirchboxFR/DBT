{{
    config(
        materialized='incremental',
        unique_key=['dw_country_code', 'user_id'],
        on_schema_change='append_new_columns',
        incremental_strategy='merge'  
    )
}}

SELECT 
    dw_country_code,
    user_id,
    predicted_ltv
FROM {{ ref('ml_ltv_predictions') }}

{% if is_incremental() %}
WHERE CONCAT(dw_country_code, '-', CAST(user_id AS STRING)) NOT IN (
    SELECT CONCAT(dw_country_code, '-', CAST(user_id AS STRING))
    FROM {{ this }}
)
{% endif %}
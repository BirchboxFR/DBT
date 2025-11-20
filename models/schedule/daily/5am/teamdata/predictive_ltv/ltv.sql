{{
    config(
        materialized='incremental',
        unique_key=['dw_country_code', 'user_id'],
        on_schema_change='append_new_columns'
    )
}}

WITH new_predictions AS (
    SELECT 
        dw_country_code,
        user_id,
        predicted_ltv
    FROM {{ ref('ml_ltv_predictions') }}
)

{% if is_incremental() %}
, existing_keys AS (
    SELECT 
        dw_country_code,
        user_id
    FROM {{ this }}
)

SELECT 
    np.dw_country_code,
    np.user_id,
    np.predicted_ltv
FROM new_predictions np
LEFT JOIN existing_keys ek
    ON np.dw_country_code = ek.dw_country_code
    AND np.user_id = ek.user_id
WHERE ek.user_id IS NULL

{% else %}

SELECT * FROM new_predictions

{% endif %}
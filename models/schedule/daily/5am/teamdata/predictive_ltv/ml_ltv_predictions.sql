{{
    config(
        materialized='table'
    )
}}

SELECT 
    'FR' AS dw_country_code,
    user_id,
    1.69 * EXP(predicted_log_ltv) AS predicted_ltv
FROM ML.PREDICT(
    MODEL `teamdata-291012.predictive_ltv.bqml_ltv_log_boosted`,
    (
        SELECT 
            user_id,
            CASE 
                WHEN DATE_DIFF(cohorte_client, birth_date, YEAR) < 18 THEN "1.<18"
                WHEN DATE_DIFF(cohorte_client, birth_date, YEAR) BETWEEN 18 AND 24 THEN "2.18-24"
                WHEN DATE_DIFF(cohorte_client, birth_date, YEAR) BETWEEN 25 AND 31 THEN "3.25-31"
                WHEN DATE_DIFF(cohorte_client, birth_date, YEAR) BETWEEN 32 AND 39 THEN "4.32-39"
                WHEN DATE_DIFF(cohorte_client, birth_date, YEAR) BETWEEN 40 AND 49 THEN "5.40-49"
                WHEN DATE_DIFF(cohorte_client, birth_date, YEAR) BETWEEN 50 AND 59 THEN "6.50-59"
                WHEN DATE_DIFF(cohorte_client, birth_date, YEAR) BETWEEN 60 AND 69 THEN "7.60-69"
                WHEN DATE_DIFF(cohorte_client, birth_date, YEAR) >= 70 THEN "8.70+"
                ELSE "9.Inconnu"
            END AS birthday_group, 
            optin_email, 
            shop_internet, 
            shop_hairdressing, 
            shop_pharmacy, 
            beauty_routine, 
            beauty_budget, 
            initial_sub_type, 
            initial_is_committed, 
            initial_coupon_type
        FROM {{ ref('stg_customers_to_predict') }}
    )
)
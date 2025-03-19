    
    
    SELECT distinct 'FR' AS dw_country_code, email, NULL AS user_id
    FROM {{ ref('splio_data_dedup') }} 
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 36 MONTH)
    GROUP BY email
    
    
    SELECT distinct dw_country_code, email, NULL AS user_id,
            MAX(lower(status)  = 'open') AS open_email,
         MAX(lower(status) = 'click') AS click,
         MAX(CASE WHEN lower(status) = 'open' THEN event_date END) AS date_last_open_email,
         MAX(CASE WHEN lower(status) = 'click' THEN event_date END) AS date_last_click_email,
         SAFE_DIVIDE(COUNTIF(lower(status) = 'click' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)), COUNTIF(lower(status) = 'done' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))) AS ltm_client_email_rate,
         SAFE_DIVIDE(COUNTIF(lower(status) = 'open' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)), COUNTIF(lower(status) = 'done' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))) AS ltm_open_email_rate,
         COUNTIF(lower(status) = 'click' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_click_email,
         COUNTIF(lower(status) = 'open' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_open_email,
         COUNTIF(lower(status) = 'done' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_nb_email
    FROM {{ ref('splio_data_dedup') }} 
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 36 MONTH)
    GROUP BY ALL
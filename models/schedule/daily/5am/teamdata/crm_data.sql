    
    
    SELECT distinct dw_country_code, email, NULL AS user_id,
            MAX(status  = 'Open') AS open_email,
         MAX(status = 'Click') AS click,
         MAX(CASE WHEN status = 'Open' THEN event_date END) AS date_last_open_email,
         MAX(CASE WHEN status = 'Click' THEN event_date END) AS date_last_click_email,
         SAFE_DIVIDE(COUNTIF(status = 'Click' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)), COUNTIF(status = 'Done' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))) AS ltm_client_email_rate,
         SAFE_DIVIDE(COUNTIF(status = 'Open' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)), COUNTIF(status = 'Done' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))) AS ltm_open_email_rate,
         COUNTIF(status = 'Click' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_click_email,
         COUNTIF(status = 'Open' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_open_email,
         COUNTIF(status = 'Done' AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_nb_email
    FROM {{ ref('splio_data_dedup') }} 
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 36 MONTH)
    GROUP BY ALL
SELECT 
    min(source) AS source,
        COALESCE(
        max(CASE WHEN dw_country_code != 'EU' THEN dw_country_code END),
        'EU'
    ) AS dw_country_code,
    email,
    user_id,
    max(open_email) AS open_email,
    max(click) AS click,
    max(date_last_open_email) AS date_last_open_email,
    max(date_last_click_email) AS date_last_click_email,
    max(ltm_client_email_rate) AS ltm_client_email_rate,
    max(ltm_open_email_rate) AS ltm_open_email_rate,
    max(ltm_click_email) AS ltm_click_email,
    max(ltm_open_email) AS ltm_open_email,
    max(ltm_nb_email) AS ltm_nb_email 
FROM (
    SELECT DISTINCT 
        'splio' AS source,
        dw_country_code, 
        email, 
        NULL AS user_id,
        MAX(lower(status) = 'open') AS open_email,
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
    --AND email = 'clemclem.clement@orange.fr'
    GROUP BY ALL
    
    UNION ALL
     
    -- imagino part
    SELECT 
        'imagino' AS source,
        custom_country AS dw_country_code,
        address AS email,
        NULL AS user_id,
        MAX(lower(t.type) = 'open') AS open_email,
        MAX(lower(t.type) = 'click') AS click,
        date(MAX(CASE WHEN lower(t.type) = 'open' THEN eventdate END)) AS date_last_open_email,
        date(MAX(CASE WHEN lower(t.type) = 'click' THEN eventdate END)) AS date_last_click_email,
        SAFE_DIVIDE(COUNTIF(lower(t.type) = 'click' AND date(eventdate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)), COUNTIF(lower(t.type) = 'done' AND date(eventdate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))) AS ltm_client_email_rate,
        SAFE_DIVIDE(COUNTIF(lower(t.type) = 'open' AND date(eventdate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)), COUNTIF(lower(t.type) = 'done' AND date(eventdate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))) AS ltm_open_email_rate,
        COUNTIF(lower(t.type) = 'click' AND date(eventdate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_click_email,
        COUNTIF(lower(t.type) = 'open' AND date(eventdate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_open_email,
        COUNTIF(date(eventdate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AS ltm_nb_email
    FROM cdpimagino.imaginoreplicatedcampaign c
    LEFT JOIN cdpimagino.BQ_imagino_Tracking t ON t.activationid = c.id
    GROUP BY ALL
) 
WHERE dw_country_code IS NOT NULL 
  --AND email = 'clemclem.clement@orange.fr'
GROUP BY email, user_id 
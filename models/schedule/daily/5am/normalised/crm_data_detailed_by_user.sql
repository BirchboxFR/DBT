-- 1 ligne = 1 user (address)
-- Colonnes clés :
--   campaigns                -> toutes les campagnes reçues (avec flags delivered/softBounce/hardBounce + opened/clicked/unsubscribed + nb de clics)
--   opened_campaigns         -> sous-liste des campagnes avec au moins une ouverture
--   clicked_campaigns        -> sous-liste des campagnes avec au moins un clic
--   unsubscribed_campaigns   -> sous-liste des campagnes où l'utilisateur s'est désinscrit (lien contenant 'unsubscribe')

{{ config(
    materialized = 'table',
    partition_by = {
      "field": "last_activity_date",
      "data_type": "date",
      "granularity": "month"  
    },
    cluster_by = ["address"], 
    on_schema_change = 'sync',
    description = "Table des campagnes reçues par utilisateur dans Imagino, avec statut message et interactions (open/click/unsubscribe)."
) }}


WITH base_messages AS (
  SELECT
    c.id AS campaign_id,
    c.name AS campaign_name,
    c.created,
    c.startdate,
    m.address,
    m.status,
    m.eventDate,
    JSON_EXTRACT_SCALAR(m.contactData, '$.imo_variant') as imo_variant
  FROM `cdpimagino.imaginoreplicatedcampaign` c
  JOIN `cdpimagino.BQ_imagino_Message` m
    ON m.activationId = c.id
  WHERE DATE(m.eventDate) >= '2024-01-01'
),

-- On garde le "dernier statut message" par (campaign_id, address)
latest_message AS (
  SELECT
    campaign_id,
    campaign_name,
    imo_variant,
    address,
    (ARRAY_AGG(STRUCT(status, eventDate, created, startdate)
               ORDER BY eventDate DESC LIMIT 1))[OFFSET(0)].status    AS last_msg_status,
    (ARRAY_AGG(STRUCT(status, eventDate, created, startdate)
               ORDER BY eventDate DESC LIMIT 1))[OFFSET(0)].created   AS created,
    (ARRAY_AGG(STRUCT(status, eventDate, created, startdate)
               ORDER BY eventDate DESC LIMIT 1))[OFFSET(0)].startdate AS startdate
  FROM base_messages
  GROUP BY all
),

-- Stats de tracking par (campaign_id, address)
tracking AS (
  SELECT
    t.activationid AS campaign_id,
    t.address,
    COUNTIF(t.type = 'open')  > 0 AS opened,
    COUNTIF(t.type = 'click') > 0 AS clicked,
    COUNTIF(t.type = 'click')     AS clicks,
    case when t.type = 'open' then t.eventDate end as date_open,
    case when t.type = 'click' then t.eventDate end as date_click,
    COUNTIF(LOWER(t.url) LIKE '%unsubscribe%') > 0 AS unsubscribed
  FROM `cdpimagino.BQ_imagino_Tracking` t
  WHERE DATE(t.eventDate) >= '2024-01-01'
  GROUP BY all
),

-- Jointure message (statut dernier) + tracking
per_user_campaign AS (
  SELECT
    lm.address,
    lm.campaign_id,
    lm.campaign_name,
    imo_variant,
    DATE(COALESCE(lm.startdate, lm.created)) AS startdate,
    -- Flags message
    TRUE                                  AS targeted,
    (lm.last_msg_status = 'delivered')    AS delivered,
    (lm.last_msg_status = 'softBounce')   AS softBounce,
    (lm.last_msg_status = 'hardBounce')   AS hardBounce,
    -- Tracking
    COALESCE(tr.opened, FALSE)            AS opened,
    COALESCE(tr.clicked, FALSE)           AS clicked,
    COALESCE(tr.clicks, 0)                AS clicks,
    COALESCE(tr.unsubscribed, FALSE)      AS unsubscribed,

    tr.date_open     AS date_open,
    tr.date_click     AS date_click
  FROM latest_message lm
  LEFT JOIN tracking tr
    ON tr.campaign_id = lm.campaign_id
   AND tr.address     = lm.address
----splio
   union all

   SELECT 
    contactid AS email, 
    campaignid,
    campaignname,
    '' AS imo_variant,
    MIN(event_date) AS event_date,
    TRUE AS targeted,
    MAX(CASE WHEN status = 'Done' THEN TRUE END) AS delivered,
    NULL AS softBounce,
    NULL AS hardBounce,
    -- Tracking
    MAX(CASE WHEN status = 'Open' THEN TRUE END) AS opened,
    MAX(CASE WHEN status = 'Click' THEN TRUE END) AS clicked,
    SUM(CASE WHEN status = 'Click' THEN 1 ELSE 0 END) AS clicks,
    MAX(CASE WHEN status = 'Unsubscribe' THEN TRUE END) AS unsubscribed,
   cast( MIN(CASE WHEN status = 'Open' THEN event_date END) as timestamp) AS date_open,
    cast(MIN(CASE WHEN status = 'Click' THEN event_date END)as timestamp) AS date_click
FROM crm.splio_events
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    --AND contactid = 'mathieu.helie@blissim.fr' 
    -- AND campaignid = '7rY0neRrJ'
GROUP BY contactid, campaignid, campaignname
)

SELECT
  'IMAGINO' AS source,
  address,
  user_key,
    DATE(MAX(startdate)) AS last_activity_date,
  -- Toutes les campagnes reçues avec leurs indicateurs
  ARRAY_AGG(
    STRUCT(
      campaign_id,
      campaign_name,imo_variant,
      startdate,
      targeted,
      delivered,
      softBounce,
      hardBounce,
      opened,
      clicked,
      clicks,
      unsubscribed
    )
    ORDER BY startdate DESC, campaign_id
  ) AS campaigns,

  -- Sous-listes utiles
  ARRAY_AGG(IF(opened,
    STRUCT(campaign_id, campaign_name, date_open,imo_variant), NULL) IGNORE NULLS
    ORDER BY startdate DESC, campaign_id
  ) AS opened_campaigns,

  ARRAY_AGG(IF(clicked,
    STRUCT(campaign_id, campaign_name, date_click, clicks,imo_variant), NULL) IGNORE NULLS
    ORDER BY startdate DESC, campaign_id
  ) AS clicked_campaigns,

  ARRAY_AGG(IF(unsubscribed,
    STRUCT(campaign_id, campaign_name, startdate,imo_variant), NULL) IGNORE NULLS
    ORDER BY startdate DESC, campaign_id
  ) AS unsubscribed_campaigns

FROM per_user_campaign
inner join user.customers on customers.email=per_user_campaign.address

GROUP BY address,user_key





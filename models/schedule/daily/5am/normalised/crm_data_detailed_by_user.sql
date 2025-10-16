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
    m.eventDate
  FROM `cdpimagino.imaginoreplicatedcampaign` c
  JOIN `cdpimagino.imaginoreplicatedmessage` m
    ON m.activationId = c.id
  WHERE DATE(m.eventDate) >= '2024-01-01'
),

-- On garde le "dernier statut message" par (campaign_id, address)
latest_message AS (
  SELECT
    campaign_id,
    campaign_name,
    address,
    (ARRAY_AGG(STRUCT(status, eventDate, created, startdate)
               ORDER BY eventDate DESC LIMIT 1))[OFFSET(0)].status    AS last_msg_status,
    (ARRAY_AGG(STRUCT(status, eventDate, created, startdate)
               ORDER BY eventDate DESC LIMIT 1))[OFFSET(0)].created   AS created,
    (ARRAY_AGG(STRUCT(status, eventDate, created, startdate)
               ORDER BY eventDate DESC LIMIT 1))[OFFSET(0)].startdate AS startdate
  FROM base_messages
  GROUP BY campaign_id, campaign_name, address
),

-- Stats de tracking par (campaign_id, address)
tracking AS (
  SELECT
    t.activationid AS campaign_id,
    t.address,
    COUNTIF(t.type = 'open')  > 0 AS opened,
    COUNTIF(t.type = 'click') > 0 AS clicked,
    COUNTIF(t.type = 'click')     AS clicks,
    COUNTIF(LOWER(t.url) LIKE '%unsubscribe%') > 0 AS unsubscribed
  FROM `cdpimagino.imaginoreplicatedtracking` t
  WHERE DATE(t.eventDate) >= '2024-01-01'
  GROUP BY campaign_id, address
),

-- Jointure message (statut dernier) + tracking
per_user_campaign AS (
  SELECT
    lm.address,
    lm.campaign_id,
    lm.campaign_name,
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
    COALESCE(tr.unsubscribed, FALSE)      AS unsubscribed
  FROM latest_message lm
  LEFT JOIN tracking tr
    ON tr.campaign_id = lm.campaign_id
   AND tr.address     = lm.address
)

SELECT
  'IMAGINO' AS source,
  address,
    DATE(MAX(startdate)) AS last_activity_date,
  -- Toutes les campagnes reçues avec leurs indicateurs
  ARRAY_AGG(
    STRUCT(
      campaign_id,
      campaign_name,
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
    STRUCT(campaign_id, campaign_name, startdate), NULL) IGNORE NULLS
    ORDER BY startdate DESC, campaign_id
  ) AS opened_campaigns,

  ARRAY_AGG(IF(clicked,
    STRUCT(campaign_id, campaign_name, startdate, clicks), NULL) IGNORE NULLS
    ORDER BY startdate DESC, campaign_id
  ) AS clicked_campaigns,

  ARRAY_AGG(IF(unsubscribed,
    STRUCT(campaign_id, campaign_name, startdate), NULL) IGNORE NULLS
    ORDER BY startdate DESC, campaign_id
  ) AS unsubscribed_campaigns

FROM per_user_campaign

GROUP BY address





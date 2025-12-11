{{
  config(
    materialized='table',
    partition_by={
      "field": "startdate",
      "data_type": "date",
       "granularity": "month"
    }
  )
}}


WITH
message_variants AS (
  SELECT
    activationId AS campaign_id,
    address,
    JSON_EXTRACT_SCALAR(contactData, '$.imo_variant') AS imo_variant
  FROM cdpimagino.BQ_imagino_Message
  WHERE DATE(eventDate) >= '2024-01-01'
    AND JSON_EXTRACT_SCALAR(contactData, '$.imo_variant') IS NOT NULL
  GROUP BY campaign_id, address, imo_variant
),

campaign_message_stats AS (
 SELECT 
   c.id AS campaign_id,
   custom_Categorie_de_campagne,
   custom_Categorie_de_Campagne_Lvl_2,
   custom_Code_operation,
   custom_typologie,
   custom_country,
  channel,
   JSON_EXTRACT_SCALAR(m.contactData, '$.imo_variant') AS imo_variant,
   COUNTIF(m.status <> 'ignored') AS targeted,
   COUNT(DISTINCT IF(m.status = 'delivered', m.address, NULL))  AS delivered,
   COUNT(DISTINCT IF(m.status = 'softBounce', m.address, NULL)) AS softBounce,
   COUNT(DISTINCT IF(m.status = 'hardBounce', m.address, NULL)) AS hardBounce,

   c.name,
   c.startdate,
   c.created

 FROM cdpimagino.imaginoreplicatedcampaign c
 JOIN cdpimagino.BQ_imagino_Message m
   ON m.activationId = c.id
 WHERE DATE(m.eventDate) >= '2024-01-01'
 GROUP BY ALL
),

campaign_tracking_stats AS (
  WITH t AS (
     SELECT *
     FROM cdpimagino.BQ_imagino_Tracking
     WHERE DATE(eventDate) >= '2024-01-01'
  )
  
  SELECT
    c.id AS campaign_id,
    mv.imo_variant,

    COUNT(DISTINCT IF(t.type = 'open',  t.address, NULL)) AS open_uniques,
    COUNT(DISTINCT IF(t.type = 'click', t.address, NULL)) AS click_uniques,
    COUNT(            IF(t.type = 'click', t.address, NULL)) AS clicks,
    COUNT(DISTINCT IF(t.url LIKE '%unsubscribe%', t.address, NULL)) AS unsubscribes

  FROM cdpimagino.imaginoreplicatedcampaign c
  LEFT JOIN t
    ON t.activationId = c.id
  LEFT JOIN message_variants mv
    ON mv.campaign_id = c.id
   AND mv.address = t.address
  GROUP BY c.id, mv.imo_variant
)

SELECT
    'IMAGINO' AS source,
custom_Categorie_de_campagne,
   custom_Categorie_de_Campagne_Lvl_2,
   custom_Code_operation,
   custom_typologie,
   custom_country,
   channel,
    msg.campaign_id,
    msg.imo_variant,
    msg.targeted,
    msg.delivered,
    msg.softBounce,
    msg.hardBounce,
    msg.name,
    COALESCE(DATE(msg.startdate), DATE(msg.created)) AS startdate,

    COALESCE(trk.open_uniques, 0)  AS open_uniques,
    COALESCE(trk.click_uniques, 0) AS click_uniques,
    COALESCE(trk.clicks, 0)        AS clicks,
    COALESCE(trk.unsubscribes, 0)  AS unsubscribes

FROM campaign_message_stats msg
LEFT JOIN campaign_tracking_stats trk
  ON trk.campaign_id = msg.campaign_id
 AND (
        trk.imo_variant = msg.imo_variant
        OR (trk.imo_variant IS NULL AND msg.imo_variant IS NULL)
     )
     group by all
    
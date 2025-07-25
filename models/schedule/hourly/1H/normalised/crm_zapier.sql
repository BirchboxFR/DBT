-- models/marts/zapier_new_campaigns.sql
-- Version optimisée pour Zapier avec ID numérique stable

{{ config(
    materialized='incremental',
    unique_key='zapier_id' 
) }}

WITH new_campaigns AS (
  SELECT 
    id as campaign_id_string,  -- ID original (string)
    ABS(FARM_FINGERPRINT(id)) as zapier_id,  -- Hash BigQuery
    name as campaign_name,
    custom_country,
    custom_typologie,
    custom_contenu_communication,
    custom_categorie_de_campagne,
    custom_nouveaux_segments,  -- Virgule manquante ajoutée
    status,
    created,
    startDate as send_date,  -- Champ manquant pour le filtre incrémental
    'NEW' as zapier_status,
    CURRENT_TIMESTAMP() as processed_at
  FROM `teamdata-291012.cdpimagino.imaginoReplicatedCampaign`
  
  {% if is_incremental() %}
    WHERE created > (SELECT MAX(created) FROM {{ this }})
  {% endif %}
),

campaign_stats AS (
  SELECT 
    m.activationId,
    COUNT(DISTINCT m.address) as targeted,
    COUNT(DISTINCT CASE WHEN m.status = 'delivered' THEN m.address END) as delivered,
    COUNT(DISTINCT CASE WHEN m.status = 'softBounce' THEN m.address END) as soft_bounce,
    COUNT(DISTINCT CASE WHEN m.status = 'hardBounce' THEN m.address END) as hard_bounce
  FROM `teamdata-291012.cdpimagino.imaginoReplicatedMessage` m
  WHERE DATE(m.eventDate) >= '2024-01-01'
    {% if is_incremental() %}
      -- Optimisation : ne calculer que pour les nouvelles campagnes
      AND m.activationId IN (SELECT campaign_id_string FROM new_campaigns)
    {% endif %}
  GROUP BY m.activationId
)

SELECT 
  nc.*,  -- Virgule manquante ajoutée
  -- Stats avec targeted
  COALESCE(cs.targeted, 0) as targeted,
  COALESCE(cs.delivered, 0) as delivered,
  COALESCE(cs.soft_bounce, 0) as soft_bounce,
  COALESCE(cs.hard_bounce, 0) as hard_bounce,
  -- Métriques calculées
  ROUND(
    COALESCE(cs.delivered, 0) * 100.0 / NULLIF(COALESCE(cs.targeted, 0), 0), 2
  ) as delivery_rate
FROM new_campaigns nc
LEFT JOIN campaign_stats cs ON cs.activationId = nc.campaign_id_string

-- Optionnel : filtrer les campagnes avec au moins quelques contacts
-- WHERE COALESCE(cs.targeted, 0) > 0
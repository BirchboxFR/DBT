-- dans votre fichier .sql du modèle
{{
  config(
    materialized='incremental',
    unique_key=['email', 'status', 'event_date', 'campaignid'],
    incremental_strategy='merge'
  )
}}

SELECT 
  ContactID AS email,
  Status AS status,
  Event_date AS event_date,
  CampaignID AS campaignid
FROM (
  SELECT 
    ContactID,
    Status,
    Event_date,
    CampaignID,
    ROW_NUMBER() OVER (PARTITION BY CampaignID, ContactID, Status ORDER BY Event_date) AS rn
  FROM crm.splio_events
  WHERE event_date IS NOT NULL 
    AND Event_Date >= '2023-01-01'
    {% if is_incremental() %}
    -- En mode incrémental, prenez les données du jour précédent pour éviter les conflits
    AND Event_Date > (SELECT DATE_SUB(MAX(event_date), INTERVAL 1 DAY) FROM {{ this }})
    {% endif %}
)
WHERE rn = 1
-- dans votre fichier .sql du modèle
{{
  config(
    materialized='incremental',
    unique_key=['email', 'status','event_date'],
    incremental_strategy='merge'
  )
}}

SELECT * EXCEPT(rn)
FROM (
  SELECT 
    ContactID AS email,
    Status AS status,
    Event_date AS event_date,
    ROW_NUMBER() OVER (PARTITION BY CampaignID, ContactID, Status ORDER BY Event_date) rn
  FROM crm.splio_events
  WHERE event_date IS NOT NULL 
    AND Event_Date >= '2023-01-01'
    {% if is_incremental() %}
    -- Si c'est une exécution incrémentale, ne prenez que les données depuis la dernière exécution
    AND Event_Date >= (SELECT MAX(event_date) FROM {{ this }})
    {% endif %}
)
WHERE rn = 1
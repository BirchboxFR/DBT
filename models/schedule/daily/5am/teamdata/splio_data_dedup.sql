-- dans votre fichier .sql du modèle
{{
  config(
    materialized='incremental',
    unique_key=['email', 'status', 'event_date', 'campaignid'],
    incremental_strategy='merge'
  )
}}


with all_events as (
select distinct 
    dw_country_code,
    ContactID,
    Status,
    Event_date,
    CampaignID from (
SELECT 
    'FR' as dw_country_code,
    ContactID,
    Status,
    Event_date,
    CampaignID from `teamdata-291012.crm.splio_events`
    where  Event_Date >= '2023-01-01'
    union all
SELECT 
    'EU' as dw_country_code,
    Contact_ID,
    case when Status ='sent' then 'done' else lower(Status) end as Status,
    safe_cast(Event_date as date),
    CampaignID from `teamdata-291012.backup_splio.EU_splio_events`
    where  Event_Date >= '2023-01-01')

)

SELECT 
  dw_country_code,
  ContactID AS email,
  Status AS status,
  Event_date AS event_date,
  CampaignID AS campaignid
FROM (
  SELECT 
    dw_country_code,
    ContactID,
    Status,
    Event_date,
    CampaignID,
    ROW_NUMBER() OVER (PARTITION BY CampaignID, ContactID, Status ORDER BY Event_date) AS rn
  FROM all_events
  WHERE event_date IS NOT NULL 
    AND Event_Date >= '2023-01-01'
    {% if is_incremental() %}
    -- En mode incrémental, prenez les données du jour précédent pour éviter les conflits
    AND Event_Date > (SELECT DATE_SUB(MAX(event_date), INTERVAL 1 DAY) FROM {{ this }})
    {% endif %}
)
WHERE rn = 1

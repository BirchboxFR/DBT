
WITH message_with_status AS (
  -- Extrait toutes les occurrences des messages avec leurs statuts
  SELECT 
    ContactID,
    CampaignID,
    Subject,
    Status,
    ArrivedAt,
    _airbyte_extracted_at,
    -- Pour regrouper les messages liés par UUID ou par ID 
    UUID as message_uuid,
    ID as message_id
  FROM 
    `teamdata-291012.mailjetAPI.message_incr`
),

-- Pivot des statuts pour extraire les dates pour chaque type d'événement par message
status_pivoted AS (
  SELECT
    ContactID,
    message_id,
    message_uuid,
    CampaignID,
    Subject,
    -- Extraire les dates pour chaque statut (utiliser la plus ancienne date si plusieurs occurrences)
    MIN(CASE WHEN Status = 'sent' THEN _airbyte_extracted_at END) as sent_date,
    MIN(CASE WHEN Status = 'opened' THEN _airbyte_extracted_at END) as opened_date,
    MIN(CASE WHEN Status = 'clicked' THEN _airbyte_extracted_at END) as clicked_date,
    MIN(CASE WHEN Status = 'bounce' THEN _airbyte_extracted_at END) as bounce_date,
    MIN(CASE WHEN Status = 'blocked' THEN _airbyte_extracted_at END) as blocked_date,
    MIN(CASE WHEN Status = 'spam' THEN _airbyte_extracted_at END) as spam_date,
    MIN(CASE WHEN Status = 'unsub' THEN _airbyte_extracted_at END) as unsub_date,
    -- Dernier statut observé (le plus récent)
    ARRAY_AGG(Status ORDER BY _airbyte_extracted_at DESC)[OFFSET(0)] as last_status,
    MIN(ArrivedAt) as arrived_at,
    MAX(_airbyte_extracted_at) as last_extracted_at
  FROM 
    message_with_status
  GROUP BY 
    ContactID, message_id, message_uuid, CampaignID, Subject
)

-- Ajout des calculs de délais et jointure avec la table des contacts
SELECT 
  -- Infos contact
  c.ID as contact_id,
  c.Email as email,
  c.Name as name,
  
  -- Infos message
  sp.message_id,
  sp.message_uuid,
  sp.CampaignID as campaign_id,
  sp.Subject as subject,
  sp.last_status,
  sp.arrived_at,
  
  -- Dates des différents statuts
  sp.sent_date,
  sp.opened_date,
  sp.clicked_date,
  sp.bounce_date,
  sp.blocked_date,
  sp.spam_date,
  sp.unsub_date,
  
  -- Calcul des délais en heures
  TIMESTAMP_DIFF(sp.opened_date, sp.sent_date, HOUR) as hours_to_open,
  TIMESTAMP_DIFF(sp.clicked_date, sp.opened_date, HOUR) as hours_from_open_to_click,
  TIMESTAMP_DIFF(sp.clicked_date, sp.sent_date, HOUR) as hours_to_click,
  TIMESTAMP_DIFF(sp.bounce_date, sp.sent_date, HOUR) as hours_to_bounce,
  TIMESTAMP_DIFF(sp.blocked_date, sp.sent_date, HOUR) as hours_to_block,
  TIMESTAMP_DIFF(sp.spam_date, sp.sent_date, HOUR) as hours_to_spam,
  TIMESTAMP_DIFF(sp.unsub_date, sp.sent_date, HOUR) as hours_to_unsub
FROM 
  status_pivoted sp
LEFT JOIN 
  `teamdata-291012.mailjetAPI.contacts` c
  ON sp.ContactID = c.ID
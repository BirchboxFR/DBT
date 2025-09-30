WITH LatestEmails AS (
  SELECT
    user_id,
    dw_country_code,
    user_email
  FROM (
    SELECT
      user_id,
      dw_country_code,
      user_email,
      ROW_NUMBER() OVER(PARTITION BY user_id, dw_country_code ORDER BY updated_at DESC) as rn
    FROM {{ ref('user_consent') }}
    -- test multi WHERE user_id = 2327271 AND dw_country_code = 'DE'
  )
  WHERE rn = 1
),

ConsentStatus AS (
  SELECT 
    user_id,
    dw_country_code,
    max(case when t.name='email' and consent_status then true else false end) AS consent_email,
    max(case when t.name='sms' and consent_status then true else false end) AS consent_sms,
    max(case when t.name='whatsapp' and consent_status then true else false end) AS consent_whatsapp,
    max(case when t.name='partner_sharing' and consent_status then true else false end) AS consent_partner_sharing,
    max(case when t.name='beauty_profile' and consent_status then true else false end) AS consent_beauty_profile
  FROM {{ ref('user_consent') }} c
  LEFT JOIN {{ ref('consent_topic') }} t USING (consent_topic_id, dw_country_code)
 
  GROUP BY user_id, dw_country_code
)

SELECT
  e.user_id,
  e.user_email,
  e.dw_country_code,
  c.consent_email,
  c.consent_sms,
  c.consent_whatsapp,
  c.consent_partner_sharing,
  c.consent_beauty_profile
FROM LatestEmails e
JOIN ConsentStatus c USING (user_id, dw_country_code)
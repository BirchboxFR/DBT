SELECT c.*,name 
FROM {{ ref('user_consent') }} c
inner join {{ ref('consent_topic') }} ct using(consent_topic_id)
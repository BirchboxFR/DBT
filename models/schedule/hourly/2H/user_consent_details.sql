SELECT distinct user_id,user_email,dw_country_code,
max(case when t.name='email' and consent_status=1 then true else false end )consent_email ,
max(case when t.name='sms' and consent_status=1 then true else false end) consent_sms,
max(case when t.name='whatsapp' and consent_status=1 then true else false end) consent_whatsapp ,
max(case when t.name='partner_sharing' and consent_status=1 then true else false end) consent_partner_sharing ,
max(case when t.name='beauty_profile' and consent_status=1 then true else false end) consent_beauty_profile ,
FROM {{ ref('user_consent') }} c
left join {{ ref('consent_topic') }} t  using (consent_topic_id,dw_country_code)
group by all
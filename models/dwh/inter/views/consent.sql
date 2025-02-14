SELECT c.*,name 
FROM {{ ref('user_consent') }} c
inner join {{ ref('consent_topic') }} ct using(consent_topic_id)
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

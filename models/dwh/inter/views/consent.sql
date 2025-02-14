{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_user_consent')) -%}


SELECT c.*,name 
FROM {{ ref('user_consent') }} c
inner join {{ ref('consent_topic') }} ct using(consent_topic_id)
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

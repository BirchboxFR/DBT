{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_survey_result_answers')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_survey_result_answers')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_survey_result_answers')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_survey_result_answers')) -%}

SELECT 'FR' AS dw_country_code,
id,result_id,question_id,answer_id,null as date,null as ranking,created_at,updated_at 
FROM `bdd_prod_fr.wp_jb_survey_result_answers` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}

UNION ALL

SELECT 'DE' AS dw_country_code,
id,result_id,question_id,answer_id,null as date,null as ranking,created_at,updated_at 
FROM `bdd_prod_de.wp_jb_survey_result_answers` t
WHERE {% if '__deleted' in de_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}

UNION ALL

SELECT 'ES' AS dw_country_code,
id,result_id,question_id,answer_id,null as date,null as ranking,created_at,updated_at 
FROM `bdd_prod_es.wp_jb_survey_result_answers` t
WHERE {% if '__deleted' in es_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}

UNION ALL

SELECT 'IT' AS dw_country_code,
id,result_id,question_id,answer_id,null as date,null as ranking,created_at,updated_at 
FROM `bdd_prod_it.wp_jb_survey_result_answers` t
WHERE {% if '__deleted' in it_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}
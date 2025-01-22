{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_survey_questions')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_survey_questions')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_survey_questions')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_survey_questions')) -%}

SELECT 'FR' AS dw_country_code,
NULL AS comments, NULL AS question_category_id, display_inline, sort_order, id, survey_id, visible, created_at, shuffle, intro, NULL AS category_id, title, parent_id, type 
FROM `bdd_prod_fr.wp_jb_survey_questions` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}

UNION ALL

SELECT 'DE' AS dw_country_code,
NULL AS comments, NULL AS question_category_id, display_inline, sort_order, id, survey_id, visible, created_at, shuffle, intro, NULL AS category_id, title, parent_id, type 
FROM `bdd_prod_de.wp_jb_survey_questions` t
WHERE {% if '__deleted' in de_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}

UNION ALL

SELECT 'ES' AS dw_country_code,
NULL AS comments, NULL AS question_category_id, display_inline, sort_order, id, survey_id, visible, created_at, shuffle, intro, NULL AS category_id, title, parent_id, type 
FROM `bdd_prod_es.wp_jb_survey_questions` t
WHERE {% if '__deleted' in es_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}

UNION ALL

SELECT 'IT' AS dw_country_code,
NULL AS comments, NULL AS question_category_id, display_inline, sort_order, id, survey_id, visible, created_at, shuffle, intro, NULL AS category_id, title, parent_id, type  
FROM `bdd_prod_it.wp_jb_survey_questions` t
WHERE {% if '__deleted' in it_columns | map(attribute='name') %}t.__deleted is null {% else %}true{% endif %}
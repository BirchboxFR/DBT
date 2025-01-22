
{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_open_comment_posts')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_open_comment_posts')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_open_comment_posts')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_open_comment_posts')) -%}



SELECT 'FR' AS dw_country_code,id, post_id, created_at, created_by, 
safe_cast(start_date as date) as start_date,
safe_cast(end_date as date) as end_date,
  FROM `bdd_prod_fr.wp_jb_open_comment_posts` u
  WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL
SELECT 'DE' AS dw_country_code,id, post_id, created_at, created_by,  
safe_cast(start_date as date) as start_date,
safe_cast(end_date as date) as end_date, FROM `bdd_prod_de.wp_jb_open_comment_posts` u
  WHERE {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL
SELECT 'ES' AS dw_country_code,id, post_id, created_at, created_by,  
safe_cast(start_date as date) as start_date,
safe_cast(end_date as date) as end_date, FROM `bdd_prod_es.wp_jb_open_comment_posts` u
  WHERE {% if '__deleted' in es_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL
SELECT 'IT' AS dw_country_code,id, post_id, created_at, created_by, 
safe_cast(start_date as date) as start_date,
safe_cast(end_date as date) as end_date, FROM `bdd_prod_it.wp_jb_open_comment_posts` u
  WHERE {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

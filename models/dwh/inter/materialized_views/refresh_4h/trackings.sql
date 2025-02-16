{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_trackings')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_trackings')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_trackings')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_trackings')) -%}

SELECT 'FR' AS dw_country_code,
 id,
  order_id,
  order_detail_id,
  sub_id,
  mini_reexp_id,
  number,
  type,
  status,
  SAFE_CAST(date AS STRING) AS date,
  last_update,
  description,
  coffret_id,
  insert_date,
  created_at,
  updated_at
FROM `bdd_prod_fr.wp_jb_trackings` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL

SELECT 'DE' AS dw_country_code,
 id,
  order_id,
  order_detail_id,
  sub_id,
  mini_reexp_id,
  number,
  type,
  status,
  SAFE_CAST(date AS STRING) AS date,
  last_update,
  description,
  coffret_id,
  insert_date,
  created_at,
  updated_at
FROM `bdd_prod_de.wp_jb_trackings` t
WHERE {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL

SELECT 'ES' AS dw_country_code,
 id,
  order_id,
  order_detail_id,
  sub_id,
  mini_reexp_id,
  number,
  type,
  status,
  SAFE_CAST(date AS STRING) AS date,
  last_update,
  description,
  coffret_id,
  insert_date,
  created_at,
  updated_at
FROM `bdd_prod_es.wp_jb_trackings` t
WHERE {% if '__deleted' in es_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL

SELECT 'IT' AS dw_country_code,
 id,
  order_id,
  order_detail_id,
  sub_id,
  mini_reexp_id,
  number,
  type,
  status,
  SAFE_CAST(date AS STRING) AS date,
  last_update,
  description,
  coffret_id,
  insert_date,
  created_at,
  updated_at
FROM `bdd_prod_it.wp_jb_trackings` t
WHERE {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}
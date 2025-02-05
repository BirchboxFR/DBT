{{ config(
   materialized='view'
) }}

{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_raf_order_link')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_raf_order_link')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_raf_order_link')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_raf_order_link')) -%}

SELECT 'FR' AS dw_country_code,
max(raf_offer_id) as raf_offer_id,order_id,max(created_at) as created_at ,max(updated_at) as updated_at,max(id) as id 
FROM `bdd_prod_fr.wp_jb_raf_order_link` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}
group by all
UNION ALL

SELECT 'DE' AS dw_country_code,
max(raf_offer_id) as raf_offer_id,order_id,max(created_at) as created_at ,max(updated_at) as updated_at,max(id) as id 
FROM `bdd_prod_de.wp_jb_raf_order_link` t
WHERE {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}
group by all
UNION ALL

SELECT 'ES' AS dw_country_code,
max(raf_offer_id) as raf_offer_id,order_id,max(created_at) as created_at ,max(updated_at) as updated_at,max(id) as id 
FROM `bdd_prod_es.wp_jb_raf_order_link` t
WHERE {% if '__deleted' in es_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}
group by all
UNION ALL

SELECT 'IT' AS dw_country_code,
max(raf_offer_id) as raf_offer_id,order_id,max(created_at) as created_at ,max(updated_at) as updated_at,max(id) as id 
FROM `bdd_prod_it.wp_jb_raf_order_link` t
WHERE {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}
group by all
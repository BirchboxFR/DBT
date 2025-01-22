{{ config(
    materialized='table',
    partition_by={
      "field": "id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 1000000000,
        "interval": 100000
      }
    },
    cluster_by=['dw_country_code', 'code','parent_id','validity_date']
) }}


{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_coupons')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_coupons')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_coupons')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_coupons')) -%}

SELECT 'FR' AS dw_country_code, t.*except(valid_from,validity_date,
{% if '__deleted' in fr_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in fr_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in fr_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in fr_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in fr_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in fr_columns | map(attribute='name') %}_rivery_run_id,{% endif %}
 {% if '_rivery_last_update' in fr_columns | map(attribute='name') %}_rivery_last_update{% endif %}), 
safe_cast(validity_date as date) as validity_date,
safe_cast(valid_from as date) as valid_from
 FROM `bdd_prod_fr.wp_jb_coupons` t
 WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL 
SELECT 'DE' AS dw_country_code, t.*except(valid_from,validity_date,
{% if '__deleted' in de_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in de_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in de_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in de_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in de_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in de_columns | map(attribute='name') %}_rivery_run_id,{% endif %}
 {% if '_rivery_last_update' in de_columns | map(attribute='name') %}_rivery_last_update{% endif %}), 
safe_cast(validity_date as date) as validity_date,
safe_cast(valid_from as date) as valid_from 
FROM `bdd_prod_de.wp_jb_coupons` t
WHERE {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL 
SELECT 'ES' AS dw_country_code, t.*except(valid_from,validity_date,
{% if '__deleted' in es_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in es_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in es_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in es_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in es_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in es_columns | map(attribute='name') %}_rivery_run_id,{% endif %}
 {% if '_rivery_last_update' in es_columns | map(attribute='name') %}_rivery_last_update{% endif %}), 
safe_cast(validity_date as date) as validity_date,
safe_cast(valid_from as date) as valid_from 
FROM `bdd_prod_es.wp_jb_coupons` t
WHERE {% if '__deleted' in es_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}

UNION ALL 
SELECT 'IT' AS dw_country_code, t.*except(valid_from,validity_date,
{% if '__deleted' in it_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in it_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in it_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in it_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in it_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in it_columns | map(attribute='name') %}_rivery_run_id,{% endif %}
 {% if '_rivery_last_update' in it_columns | map(attribute='name') %}_rivery_last_update{% endif %}), 
safe_cast(validity_date as date) as validity_date,
safe_cast(valid_from as date) as valid_from 
FROM `bdd_prod_it.wp_jb_coupons` t
WHERE {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}





{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "order_detail_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 50000000,
        "interval": 30000
      }
    },
    cluster_by=['dw_country_code', 'box_id'],
    unique_key='id'
) }}


{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_order_detail_sub')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_order_detail_sub')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_order_detail_sub')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_order_detail_sub')) -%}

-- Approche simplifiée pour le modèle incrémental
SELECT 'FR' AS dw_country_code,
t.* EXCEPT(next_payment_date,last_payment_date,
 {% if '__deleted' in fr_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in fr_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in fr_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in fr_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in fr_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in fr_columns | map(attribute='name') %}_rivery_run_id,{% endif %}
 {% if '_rivery_last_update' in fr_columns | map(attribute='name') %}_rivery_last_update{% endif %}
), safe_cast(next_payment_date as date) as next_payment_date,
safe_cast(last_payment_date as date) as last_payment_date
FROM `bdd_prod_fr.wp_jb_order_detail_sub` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}true{% endif %}
{% if is_incremental() %}
-- Lors des exécutions incrémentielles, on se base sur la date de mise à jour plutôt que sur __ts_ms
AND t.updated_at > (SELECT COALESCE(MAX(updated_at), TIMESTAMP('1970-01-01')) FROM {{ this }} WHERE dw_country_code = 'FR')
{% endif %}

UNION ALL

SELECT 'DE' AS dw_country_code,
t.* EXCEPT(next_payment_date,last_payment_date,
 {% if '__deleted' in de_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in de_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in de_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in de_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in de_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in de_columns | map(attribute='name') %}_rivery_run_id,{% endif %}
 {% if '_rivery_last_update' in de_columns | map(attribute='name') %}_rivery_last_update{% endif %}
), safe_cast(next_payment_date as date) as next_payment_date,
safe_cast(last_payment_date as date) as last_payment_date
FROM `bdd_prod_de.wp_jb_order_detail_sub` t
WHERE {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}true{% endif %}
{% if is_incremental() %}
-- Lors des exécutions incrémentielles, on se base sur la date de mise à jour plutôt que sur __ts_ms
AND t.updated_at > (SELECT COALESCE(MAX(updated_at), TIMESTAMP('1970-01-01')) FROM {{ this }} WHERE dw_country_code = 'DE')
{% endif %}

UNION ALL

SELECT 'ES' AS dw_country_code,
t.* EXCEPT(next_payment_date,last_payment_date,
 {% if '__deleted' in es_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in es_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in es_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in es_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in es_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in es_columns | map(attribute='name') %}_rivery_run_id,{% endif %}
 {% if '_rivery_last_update' in es_columns | map(attribute='name') %}_rivery_last_update{% endif %}
), safe_cast(next_payment_date as date) as next_payment_date,
safe_cast(last_payment_date as date) as last_payment_date
FROM `bdd_prod_es.wp_jb_order_detail_sub` t
WHERE {% if '__deleted' in es_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}true{% endif %}
{% if is_incremental() %}
-- Lors des exécutions incrémentielles, on se base sur la date de mise à jour plutôt que sur __ts_ms
AND t.updated_at > (SELECT COALESCE(MAX(updated_at), TIMESTAMP('1970-01-01')) FROM {{ this }} WHERE dw_country_code = 'ES')
{% endif %}

UNION ALL

SELECT 'IT' AS dw_country_code,
t.* EXCEPT(next_payment_date,last_payment_date,
 {% if '__deleted' in it_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in it_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in it_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in it_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in it_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in it_columns | map(attribute='name') %}_rivery_run_id,{% endif %}
 {% if '_rivery_last_update' in it_columns | map(attribute='name') %}_rivery_last_update{% endif %}
), safe_cast(next_payment_date as date) as next_payment_date,
safe_cast(last_payment_date as date) as last_payment_date
FROM `bdd_prod_it.wp_jb_order_detail_sub` t
WHERE {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}true{% endif %}
{% if is_incremental() %}
-- Lors des exécutions incrémentielles, on se base sur la date de mise à jour plutôt que sur __ts_ms
AND t.updated_at > (SELECT COALESCE(MAX(updated_at), TIMESTAMP('1970-01-01')) FROM {{ this }} WHERE dw_country_code = 'IT')
{% endif %}
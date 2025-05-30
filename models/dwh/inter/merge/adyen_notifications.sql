
{{
  config(
    materialized = 'table',
    partition_by = {
      "field": "eventdate",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by = ['dw_country_code'] 
  )
}}


{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_adyen_notifications')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_adyen_notifications')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_adyen_notifications')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_adyen_notifications')) -%}
-- Le nombre d'heures en arrière pour lesquelles récupérer les données (4 heures par défaut)
{%- set lookback_hours = 4 -%}

-- Sélection des données françaises
SELECT 'FR' AS dw_country_code,
id,merchantaccountcode,value,pspreference,eventcode,success,reason,merchantreference,
originalreference,order_id,detail_id,sub_id,transaction_time,created_at,updated_at,
safe_cast(eventdate as date) as eventdate
FROM `bdd_prod_fr.wp_jb_adyen_notifications` t
WHERE 
  -- Filtre sur les lignes non supprimées (CDC)
  t._ab_cdc_deleted_at IS NULL 
  -- Filtre sur les données récentes uniquement
  {% if is_incremental() %}
  AND t._ab_cdc_updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
  {% endif %}
UNION ALL

SELECT 'DE' AS dw_country_code,
id,merchantaccountcode,value,pspreference,eventcode,success,reason,merchantreference,
originalreference,order_id,detail_id,sub_id,transaction_time,created_at,updated_at,
safe_cast(eventdate as date) as eventdate
FROM `bdd_prod_de.wp_jb_adyen_notifications` t
WHERE 
  {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) AND{% endif %}
  {% if is_incremental() %}
  (
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)

  )
  {% else %}
  TRUE
  {% endif %}
UNION ALL

SELECT 'ES' AS dw_country_code,
id,merchantaccountcode,value,pspreference,eventcode,success,reason,merchantreference,
originalreference,order_id,detail_id,sub_id,transaction_time,created_at,updated_at,
safe_cast(eventdate as date) as eventdate
FROM `bdd_prod_es.wp_jb_adyen_notifications` t
WHERE 
  {% if '__deleted' in es_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) AND{% endif %}
  {% if is_incremental() %}
  (
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)

  )
  {% else %}
  TRUE
  {% endif %}
UNION ALL

SELECT 'IT' AS dw_country_code,
id,merchantaccountcode,value,pspreference,eventcode,success,reason,merchantreference,
originalreference,order_id,detail_id,sub_id,transaction_time,created_at,updated_at,
safe_cast(eventdate as date) as eventdate
FROM `bdd_prod_it.wp_jb_adyen_notifications` t
WHERE 
  {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) AND{% endif %}
  {% if is_incremental() %}
  (
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)

  )
  {% else %}
  TRUE
  {% endif %}
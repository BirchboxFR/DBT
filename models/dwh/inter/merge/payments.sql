

{{ 
  config(
    partition_by = {
      "field": "date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by = ['dw_country_code'] 
  ) 
}}


{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_payments')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_payments')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_payments')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_payments')) -%}
-- Le nombre d'heures en arrière pour lesquelles récupérer les données (4 heures par défaut)
{%- set lookback_hours = 4 -%}

-- Sélection des données françaises
SELECT 'FR' as dw_country_code,
      id,user_id,order_id,sub_id,payment_gateway_id,transaction_id,amount,payment_profile_id,
      auto,status_id, payment_method_id,data,date(date) AS date,created_at,updated_at
FROM `bdd_prod_fr.wp_jb_payments` t
WHERE 
  -- Filtre sur les lignes non supprimées
  {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) AND{% endif %}
  -- Filtre sur les données récentes uniquement
  {% if is_incremental() %}
  (
    -- Données mises à jour récemment (dans les X dernières heures)
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)

  )
  {% else %}
  -- Premier chargement: toutes les données
  TRUE
  {% endif %}
UNION ALL

SELECT 'DE' as dw_country_code,
      id,user_id,order_id,sub_id,payment_gateway_id,transaction_id,amount,payment_profile_id,
      auto,status_id, payment_method_id,data,date(date) AS date,created_at,updated_at
FROM `bdd_prod_de.wp_jb_payments` t
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

SELECT 'ES' as dw_country_code,
      id,user_id,order_id,sub_id,payment_gateway_id,transaction_id,amount,payment_profile_id,
      auto,status_id, payment_method_id,data,date(date) AS date,created_at,updated_at
FROM `bdd_prod_es.wp_jb_payments` t
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

SELECT 'IT' as dw_country_code,
      id,user_id,order_id,sub_id,payment_gateway_id,transaction_id,amount,payment_profile_id,
      auto,status_id, payment_method_id,data,date(date) AS date,created_at,updated_at
FROM `bdd_prod_it.wp_jb_payments` t
WHERE 
  {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) AND{% endif %}
  {% if is_incremental() %}
  (
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)

  )
  {% else %}
  TRUE
  {% endif %}

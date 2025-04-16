{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_saved_cart_details')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_saved_cart_details')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_saved_cart_details')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_saved_cart_details')) -%}
--données FR
--partition 
{% set lookback_hours = 2 %}
--lookback 2h
SELECT 'FR' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in fr_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in fr_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in fr_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in fr_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in fr_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in fr_columns | map(attribute='name') %}_rivery_run_id{% endif %}
 --{% if '_rivery_last_update' in fr_columns | map(attribute='name') %}_rivery_last_update{% endif %}
) 
FROM `bdd_prod_fr.wp_jb_saved_cart_details` t
WHERE 
  -- Filtre sur les lignes non supprimées
  {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) AND{% endif %}
  -- Filtre sur les données récentes uniquement
  {% if is_incremental() %}
  (
    -- Données mises à jour récemment (dans les X dernières heures)
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
       AND 
    t.date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
  )
  {% else %}
  -- Premier chargement: toutes les données
  TRUE
  {% endif %}
UNION ALL

SELECT 'DE' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in de_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in de_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in de_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in de_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in de_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in de_columns | map(attribute='name') %}_rivery_run_id{% endif %}
 --{% if '_rivery_last_update' in de_columns | map(attribute='name') %}_rivery_last_update{% endif %}
) 
FROM `bdd_prod_de.wp_jb_saved_cart_details` t
WHERE 
  {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) AND{% endif %}
  {% if is_incremental() %}
  (
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
           AND 
    t.date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)

  )
  {% else %}
  TRUE
  {% endif %}
UNION ALL

SELECT 'ES' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in es_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in es_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in es_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in es_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in es_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in es_columns | map(attribute='name') %}_rivery_run_id{% endif %}
 --{% if '_rivery_last_update' in es_columns | map(attribute='name') %}_rivery_last_update{% endif %}
) 
FROM `bdd_prod_es.wp_jb_saved_cart_details` t
WHERE 
  {% if '__deleted' in es_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) AND{% endif %}
  {% if is_incremental() %}
  (
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
           AND 
    t.date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)

  )
  {% else %}
  TRUE
  {% endif %}
UNION ALL

SELECT 'IT' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in it_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in it_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in it_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in it_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in it_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in it_columns | map(attribute='name') %}_rivery_run_id{% endif %}
 --{% if '_rivery_last_update' in it_columns | map(attribute='name') %}_rivery_last_update{% endif %}
) 
FROM `bdd_prod_it.wp_jb_saved_cart_details` t
WHERE 
  {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) AND{% endif %}
  {% if is_incremental() %}
  (
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
           AND 
    t.date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)

  )
  {% else %}
  TRUE
  {% endif %}
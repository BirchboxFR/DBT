{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='prod_fr', identifier='wp_jb_tags')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_tags')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_tags')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_tags')) -%}

{{ config(
    partition_by={
      "field": "link_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 100000000,
        "interval": 100000
      }
    },
    cluster_by=['dw_country_code']
) }}

{% set lookback_hours = 2 %}

-- Données FR
SELECT 
  'FR' AS dw_country_code,
  t.id,
  t.link_id,
  t.value,
  t.type,
  t.timestamp,
  t.user_id,
  t._airbyte_extracted_at as _rivery_last_update,
  -- ✅ Ajout du champ is_deleted standardisé pour FR
  {% if '_ab_cdc_deleted_at' in fr_columns | map(attribute='name') %}
  CASE WHEN t._ab_cdc_deleted_at IS NOT NULL THEN true ELSE false END as is_deleted
  {% else %}
  false as is_deleted
  {% endif %}
FROM `prod_fr.wp_jb_tags` t
WHERE 
  {% if is_incremental() %}
  -- Inclure les données récentes ET les suppressions récentes
  (
    -- Données non supprimées récentes
    t._airbyte_extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
    {% if '_ab_cdc_deleted_at' in fr_columns | map(attribute='name') %}
    AND t._ab_cdc_deleted_at IS NULL
    {% endif %}
  )
  {% if '_ab_cdc_deleted_at' in fr_columns | map(attribute='name') %}
  OR 
  -- Suppressions récentes à détecter
  (t._ab_cdc_deleted_at IS NOT NULL AND t._airbyte_extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR))
  {% endif %}
  {% else %}
  -- Premier chargement: toutes les données non supprimées
  {% if '_ab_cdc_deleted_at' in fr_columns | map(attribute='name') %}
  t._ab_cdc_deleted_at IS NULL
  {% else %}
  TRUE
  {% endif %}
  {% endif %}

UNION ALL

-- Données DE
SELECT 'DE' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in de_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in de_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in de_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in de_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in de_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in de_columns | map(attribute='name') %}_rivery_run_id{% endif %}
),
-- ✅ Ajout du champ is_deleted standardisé pour DE
{% if '__deleted' in de_columns | map(attribute='name') %}
COALESCE(t.__deleted, false) as is_deleted
{% else %}
false as is_deleted
{% endif %}
FROM `bdd_prod_de.wp_jb_tags` t
WHERE 
  {% if is_incremental() %}
  -- Inclure les données récentes ET les suppressions récentes
  (
    -- Données non supprimées récentes
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
    AND {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}TRUE{% endif %}
  )
  {% if '__deleted' in de_columns | map(attribute='name') %}
  OR 
  -- Suppressions récentes à détecter
  (t.__deleted = true AND t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR))
  {% endif %}
  {% else %}
  -- Premier chargement: toutes les données non supprimées
  {% if '__deleted' in de_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}TRUE{% endif %}
  {% endif %}

UNION ALL

-- Données ES
SELECT 'ES' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in es_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in es_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in es_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in es_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in es_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in es_columns | map(attribute='name') %}_rivery_run_id{% endif %}
),
-- ✅ Ajout du champ is_deleted standardisé pour ES
{% if '__deleted' in es_columns | map(attribute='name') %}
COALESCE(t.__deleted, false) as is_deleted
{% else %}
false as is_deleted
{% endif %}
FROM `bdd_prod_es.wp_jb_tags` t
WHERE 
  {% if is_incremental() %}
  -- Inclure les données récentes ET les suppressions récentes
  (
    -- Données non supprimées récentes
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
    AND {% if '__deleted' in es_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}TRUE{% endif %}
  )
  {% if '__deleted' in es_columns | map(attribute='name') %}
  OR 
  -- Suppressions récentes à détecter
  (t.__deleted = true AND t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR))
  {% endif %}
  {% else %}
  -- Premier chargement: toutes les données non supprimées
  {% if '__deleted' in es_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}TRUE{% endif %}
  {% endif %}

UNION ALL

-- Données IT
SELECT 'IT' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in it_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in it_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in it_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in it_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in it_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in it_columns | map(attribute='name') %}_rivery_run_id{% endif %}
),
-- ✅ Ajout du champ is_deleted standardisé pour IT
{% if '__deleted' in it_columns | map(attribute='name') %}
COALESCE(t.__deleted, false) as is_deleted
{% else %}
false as is_deleted
{% endif %}
FROM `bdd_prod_it.wp_jb_tags` t
WHERE 
  {% if is_incremental() %}
  -- Inclure les données récentes ET les suppressions récentes
  (
    -- Données non supprimées récentes
    t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
    AND {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}TRUE{% endif %}
  )
  {% if '__deleted' in it_columns | map(attribute='name') %}
  OR 
  -- Suppressions récentes à détecter
  (t.__deleted = true AND t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR))
  {% endif %}
  {% else %}
  -- Premier chargement: toutes les données non supprimées
  {% if '__deleted' in it_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}TRUE{% endif %}
  {% endif %}
{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_survey_result_answers')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_survey_result_answers')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_survey_result_answers')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_survey_result_answers')) -%}

{{
  config(
    partition_by={
      "field": "_rivery_last_update",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['dw_country_code', 'question_id', 'answer_id']
  )
}}

-- Le nombre d'heures en arrière pour lesquelles récupérer les données (4 heures par défaut)
{%- set lookback_hours = 4 -%}

-- Sélection des données françaises
SELECT 'FR' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in fr_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in fr_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in fr_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in fr_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in fr_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in fr_columns | map(attribute='name') %}_rivery_run_id{% endif %}
),
-- ✅ Ajout du champ is_deleted standardisé
{% if '__deleted' in fr_columns | map(attribute='name') %}
COALESCE(t.__deleted, false) as is_deleted
{% else %}
false as is_deleted
{% endif %}
FROM `bdd_prod_fr.wp_jb_survey_result_answers` t
WHERE 
  {% if is_incremental() %}
  -- Inclure les données récentes ET les suppressions récentes
  (
    -- Données non supprimées récentes
    (t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
     OR t.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR))
    AND {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}TRUE{% endif %}
  )
  {% if '__deleted' in fr_columns | map(attribute='name') %}
  OR 
  -- Suppressions récentes à détecter
  (t.__deleted = true AND t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR))
  {% endif %}
  {% else %}
  -- Premier chargement: toutes les données non supprimées
  {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false){% else %}TRUE{% endif %}
  {% endif %}

UNION ALL

-- Sélection des données allemandes
SELECT 'DE' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in de_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in de_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in de_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in de_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in de_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in de_columns | map(attribute='name') %}_rivery_run_id{% endif %}
),
-- ✅ Ajout du champ is_deleted standardisé
{% if '__deleted' in de_columns | map(attribute='name') %}
COALESCE(t.__deleted, false) as is_deleted
{% else %}
false as is_deleted
{% endif %}
FROM `bdd_prod_de.wp_jb_survey_result_answers` t
WHERE 
  {% if is_incremental() %}
  -- Inclure les données récentes ET les suppressions récentes
  (
    -- Données non supprimées récentes
    (t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
     OR t.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR))
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

-- Sélection des données espagnoles
SELECT 'ES' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in es_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in es_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in es_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in es_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in es_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in es_columns | map(attribute='name') %}_rivery_run_id{% endif %}
),
-- ✅ Ajout du champ is_deleted standardisé
{% if '__deleted' in es_columns | map(attribute='name') %}
COALESCE(t.__deleted, false) as is_deleted
{% else %}
false as is_deleted
{% endif %}
FROM `bdd_prod_es.wp_jb_survey_result_answers` t
WHERE 
  {% if is_incremental() %}
  -- Inclure les données récentes ET les suppressions récentes
  (
    -- Données non supprimées récentes
    (t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
     OR t.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR))
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

-- Sélection des données italiennes
SELECT 'IT' AS dw_country_code,
t.* EXCEPT(
 {% if '__deleted' in it_columns | map(attribute='name') %}__deleted,{% endif %}
 {% if '__ts_ms' in it_columns | map(attribute='name') %}__ts_ms,{% endif %}
 {% if '__transaction_order' in it_columns | map(attribute='name') %}__transaction_order,{% endif %}
 {% if '__transaction_id' in it_columns | map(attribute='name') %}__transaction_id,{% endif %}
 {% if '_rivery_river_id' in it_columns | map(attribute='name') %}_rivery_river_id,{% endif %}
 {% if '_rivery_run_id' in it_columns | map(attribute='name') %}_rivery_run_id{% endif %}
),
-- ✅ Ajout du champ is_deleted standardisé
{% if '__deleted' in it_columns | map(attribute='name') %}
COALESCE(t.__deleted, false) as is_deleted
{% else %}
false as is_deleted
{% endif %}
FROM `bdd_prod_it.wp_jb_survey_result_answers` t
WHERE 
  {% if is_incremental() %}
  -- Inclure les données récentes ET les suppressions récentes
  (
    -- Données non supprimées récentes
    (t._rivery_last_update >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
     OR t.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR))
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
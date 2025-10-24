{%- set fr_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_fr', identifier='wp_jb_tva_product')) -%}
{%- set de_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_de', identifier='wp_jb_tva_product')) -%}
{%- set es_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_es', identifier='wp_jb_tva_product')) -%}
{%- set it_columns = adapter.get_columns_in_relation(api.Relation.create(schema='bdd_prod_it', identifier='wp_jb_tva_product')) -%}

SELECT 'FR' AS dw_country_code,
country_code,category,max(taux) taux
FROM `bdd_prod_fr.wp_jb_tva_product` t
WHERE {% if '__deleted' in fr_columns | map(attribute='name') %}(t.__deleted is null OR t.__deleted = false) {% else %}true{% endif %}
AND category='normal'
group by all
{# ==========================
   PARAMÈTRES À CHANGER
   ========================== #}
{%- set source_table = "wp_jb_tva_product" -%}   
{%- set target_table = "inter.tva_product" -%}   
{%- set countries = var('survey_countries') -%}
{%- set window_hours = 4 -%}
{%- set window_start -%}
TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ window_hours }} HOUR)
{%- endset -%}

{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['dw_country_code','id'],
  partition_by={"field": "_airbyte_extracted_at", "data_type": "timestamp", "granularity": "day"},
  cluster_by=["dw_country_code","id"],
  post_hook=[
    "{% if is_incremental() %} {{ delete_soft_deleted(var('survey_countries'), '" ~ source_table ~ "', '" ~ window_start ~ "') }} {% endif %}"
  ]
) }}

{# ---------- BUILD ---------- #}
{%- if is_incremental() -%}
  {# INCRÉMENTAL : actifs + fenêtre pour le pruning source #}
  {%- for country in countries %}
  SELECT
    '{{ country.code }}' AS dw_country_code,
country_code,category,max(taux) taux
  FROM `teamdata-291012.{{ country.dataset }}.{{ source_table }}` AS b
  WHERE NULLIF(b._ab_cdc_deleted_at, '') IS NULL
    AND b._airbyte_extracted_at >= {{ window_start }}
    group by all
  {{ "UNION ALL" if not loop.last }}
  {%- endfor %}
{%- else -%}
  {# PREMIER RUN ou FULL REFRESH : pas de fenêtre, on charge tous les actifs #}
  {%- for country in countries %}
  SELECT
    '{{ country.code }}' AS dw_country_code,
country_code,category,max(taux) taux
  FROM `teamdata-291012.{{ country.dataset }}.{{ source_table }}` AS b
  WHERE NULLIF(b._ab_cdc_deleted_at, '') IS NULL
  group by all
  {{ "UNION ALL" if not loop.last }}
  {%- endfor %}
{%- endif -%}

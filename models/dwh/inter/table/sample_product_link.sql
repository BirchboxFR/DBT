{# ==========================
   PARAMÈTRES À CHANGER
   ========================== #}
{%- set source_table = "sample_product_link" -%}   
{%- set target_table = "inter.sample_product_link" -%}   

{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['dw_country_code','id'],
  partition_by={"field": "_airbyte_extracted_at", "data_type": "timestamp", "granularity": "day"}
) }}

{%- set countries = var('survey_countries') -%}



  {# PREMIER RUN ou FULL REFRESH : pas de fenêtre, on charge tous les actifs #}
  {%- for country in countries %}
  SELECT
    '{{ country.code }}' AS dw_country_code,
    b.* 
  FROM `teamdata-291012.{{ country.dataset }}.{{ source_table }}` AS b

  {{ "UNION ALL" if not loop.last }}
  {%- endfor %}


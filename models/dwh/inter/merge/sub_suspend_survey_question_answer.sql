{# ==========================
   PARAMÈTRES À CHANGER
   ========================== #}
{%- set source_table = "wp_jb_sub_suspend_survey_question_answer" -%}   
{%- set target_table = "inter.sub_suspend_survey_question_answer" -%}   

{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['dw_country_code','id'],
  partition_by={"field": "_airbyte_extracted_at", "data_type": "timestamp", "granularity": "day"},
  cluster_by=["dw_country_code","id"]
) }}

{%- set countries = var('survey_countries') -%}
{%- set window_hours = 4 -%}
{%- set window_start -%}
TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ window_hours }} HOUR)
{%- endset -%}

{# ---------- POST HOOK : uniquement si la table existe déjà (vrai incrémental) ---------- #}
{% if is_incremental() %}
  {%- set to_delete_sql -%}
  DELETE FROM `teamdata-291012.{{ target_table }}`
  WHERE STRUCT(dw_country_code, question_answer_id) IN (
    {%- for country in countries -%}
    SELECT AS STRUCT
      '{{ country.code }}' AS dw_country_code,
      question_answer_id
    FROM `teamdata-291012.{{ country.dataset }}.{{ source_table }}` d
    WHERE d._airbyte_extracted_at >= {{ window_start }}  -- prune SOURCE uniquement
      AND SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', NULLIF(d._ab_cdc_deleted_at,'')) IS NOT NULL
    {{ "UNION ALL" if not loop.last }}
    {%- endfor -%}
  );
  {%- endset -%}
  {{ config(post_hook=[ to_delete_sql ]) }}
{% endif %}

{# ---------- BUILD ---------- #}
{%- if is_incremental() -%}
  {# INCRÉMENTAL : actifs + fenêtre pour le pruning source #}
  {%- for country in countries %}
  SELECT
    '{{ country.code }}' AS dw_country_code,
    CAST(question_answer_id AS INT64) AS id,
    b.* 
  FROM `teamdata-291012.{{ country.dataset }}.{{ source_table }}` AS b
  WHERE NULLIF(b._ab_cdc_deleted_at, '') IS NULL
    AND b._airbyte_extracted_at >= {{ window_start }}
  {{ "UNION ALL" if not loop.last }}
  {%- endfor %}
{%- else -%}
  {# PREMIER RUN ou FULL REFRESH : pas de fenêtre, on charge tous les actifs #}
  {%- for country in countries %}
  SELECT
    '{{ country.code }}' AS dw_country_code,
    CAST(question_answer_id AS INT64) AS id,
    b.* 
  FROM `teamdata-291012.{{ country.dataset }}.{{ source_table }}` AS b
  WHERE NULLIF(b._ab_cdc_deleted_at, '') IS NULL
  {{ "UNION ALL" if not loop.last }}
  {%- endfor %}
{%- endif -%}

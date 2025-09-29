{{ config(
  materialized='incremental',
  unique_key=['dw_country_code','id'],
  partition_by={
    "field": "_airbyte_extracted_at",
    "data_type": "timestamp",
    "granularity": "day"
  },
  cluster_by=["dw_country_code","id"]
) }}

{%- set countries = var('survey_countries') -%}
{%- set window_hours = 4 -%}
{%- set window_start -%}
TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ window_hours }} HOUR), DAY)
{%- endset -%}

{# ---------- POST HOOK: un seul DELETE avec IN (STRUCT) ---------- #}
{%- set to_delete_sql -%}
DELETE FROM `teamdata-291012.inter.coupons` AS t
WHERE
  -- prune partitions de TA table (cible)
  t._airbyte_extracted_at >= {{ window_start }}
  AND STRUCT(t.dw_country_code, t.id) IN (
    {%- for country in countries %}
    SELECT AS STRUCT
      '{{ country.code }}' AS dw_country_code,
      CAST(d.id AS INT64) AS id
    FROM `teamdata-291012.{{ country.dataset }}.wp_jb_coupons` AS d
    WHERE
      -- prune partitions source
      d._airbyte_extracted_at >= {{ window_start }}
      -- soft delete détecté (STRING → TIMESTAMP)
      AND SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', NULLIF(d._ab_cdc_deleted_at,'')) IS NOT NULL
    {{ "UNION ALL" if not loop.last }}
    {%- endfor %}
  );
{%- endset -%}

{{ config(post_hook=[ to_delete_sql ]) }}

{# ---------- BUILD : seulement les actifs, pruning + incrémental ---------- #}
{%- for country in countries %}
SELECT
  '{{ country.code }}' AS dw_country_code,
  b.*
FROM `teamdata-291012.{{ country.dataset }}.wp_jb_coupons` AS b
WHERE
  -- actif = pas de soft delete (gère aussi la chaîne vide)
  NULLIF(b._ab_cdc_deleted_at, '') IS NULL
  -- prune partitions source
  AND b._airbyte_extracted_at >= {{ window_start }}
{% if is_incremental() %}
  -- fenêtre glissante pour l'incrémental
  AND b._airbyte_extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ window_hours }} HOUR)
{% endif %}
{{ "UNION ALL" if not loop.last }}
{%- endfor %}

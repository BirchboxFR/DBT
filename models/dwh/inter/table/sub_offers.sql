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

{%- set delete_hooks = [] -%}
{%- for country in countries -%}
  {%- set delete_sql -%}
DELETE FROM inter.sub_offers
WHERE dw_country_code = '{{ country.code }}'
  -- prune partitions de TA table
  AND _airbyte_extracted_at >= {{ window_start }}
  AND id IN (
    SELECT CAST(d.id AS INT64)
    FROM `teamdata-291012.{{ country.dataset }}.wp_jb_sub_offers` AS d
    -- prune partitions des tables Airbyte
    WHERE d._airbyte_extracted_at >= {{ window_start }}
      -- soft delete détecté (STRING → TIMESTAMP)
      AND SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', NULLIF(d._ab_cdc_deleted_at, '')) IS NOT NULL
  );
  {%- endset -%}
  {%- do delete_hooks.append(delete_sql) -%}
{%- endfor %}

{{ config(post_hook=delete_hooks) }}

{%- for country in countries %}
SELECT
  '{{ country.code }}' AS dw_country_code,
  b.*
FROM `teamdata-291012.{{ country.dataset }}.wp_jb_sub_offers` AS b
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

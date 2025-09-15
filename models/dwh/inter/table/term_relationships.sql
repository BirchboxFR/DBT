{{ config(
    partition_by={
      "field": "_airbyte_extracted_at", 
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["dw_country_code"]
) }}

{%- set countries = var('survey_countries') -%}



{%- for country in countries %}
SELECT 
  '{{ country.code }}' as dw_country_code,
  b.*
FROM `teamdata-291012.{{ country.dataset }}.wp_term_relationships` b
WHERE `_ab_cdc_deleted_at` IS NULL
{% if is_incremental() %}
  AND `_airbyte_extracted_at` >= timestamp_SUB(CURRENT_timestamp(), INTERVAL 2 HOUR)
{% endif %}
{{ "UNION ALL" if not loop.last }}
{%- endfor %}


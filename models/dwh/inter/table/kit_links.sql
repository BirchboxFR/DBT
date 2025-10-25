{{ config(
    cluster_by=["dw_country_code"]
) }}

{%- set countries = var('survey_countries') -%}

--- partie pays

{%- for country in countries %}
SELECT 
  '{{ country.code }}' as dw_country_code,
  b.*
FROM `teamdata-291012.{{ country.dataset }}.wp_jb_kit_links` b
{{ "UNION ALL" if not loop.last }}
{%- endfor %}


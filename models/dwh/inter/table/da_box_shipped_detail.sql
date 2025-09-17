{{ config(
    cluster_by=["dw_country_code"]
) }}

{%- set countries = var('survey_countries') -%}

--- partie pays

{%- for country in countries %}
SELECT 
  '{{ country.code }}' as dw_country_code,
  free,box_id,yearly,monthly,modified,gift_1month,gift_3months,gift_6months,reexpedition,
  gift_12months,sold_and_free,sold_not_paid,monthly_committed,monthly_uncommitted
FROM `teamdata-291012.{{ country.dataset }}.da_box_shipped_detail` b

{{ "UNION ALL" if not loop.last }}
{%- endfor %}


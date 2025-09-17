{{ config(
    cluster_by=["dw_country_code"]
) }}

{%- set countries = var('survey_countries') -%}

--- partie pays

{%- for country in countries %}
SELECT 
  '{{ country.code }}' as dw_country_code,
  b.month,box_id,nb_sub,nb_forthcoming_already_paid,
  nb_forthcoming_payment_to_come
FROM `teamdata-291012.{{ country.dataset }}.da_monthly_sub_baseline` b

{{ "UNION ALL" if not loop.last }}
{%- endfor %}


{{ config(
    cluster_by=["dw_country_code"]
) }}

{%- set countries = var('survey_countries') -%}

--- partie pays

{%- for country in countries %}
SELECT 
  '{{ country.code }}' as dw_country_code,
  country_code,category,max(taux) taux
FROM `teamdata-291012.{{ country.dataset }}.wp_jb_tva_product` b
where _ab_cdc_deleted_at is null
group by all
{{ "UNION ALL" if not loop.last }}
{%- endfor %}


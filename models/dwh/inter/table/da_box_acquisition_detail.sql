{{ config(
    cluster_by=["dw_country_code"]
) }}

{%- set countries = var('survey_countries') -%}

--- partie pays

{%- for country in countries %}
SELECT 
  '{{ country.code }}' as dw_country_code,
  VIP,box_id,modified,sent_on_oc,total_churn,sold_and_free,gift_activation,
  free_rapat_bureau,new_subs_after_oc,new_subs_before_oc,
  forthcoming_to_come,reactivations_after_oc, reactivations_before_oc,forthcoming_already_paid,churn_monthly_suspendable
FROM `teamdata-291012.{{ country.dataset }}.da_box_acquisition_detail` b

{{ "UNION ALL" if not loop.last }}
{%- endfor %}


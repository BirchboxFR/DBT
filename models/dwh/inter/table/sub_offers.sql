{{ config(
    cluster_by=["dw_country_code"]
) }}

{%- set countries = var('survey_countries') -%}

--- partie pays

{%- for country in countries %}
SELECT 
  '{{ country.code }}' as dw_country_code,
     id,
    code,
    count,
    title,
    max_use,
    trigger,
    start_box,
    conditions,
    created_at,
    created_by,
    offer_type,
    updated_at,
    valid_from,
    description,
    offer_value,
    offer_target,
    user_max_use,
    validity_date,
    parent_offer_id,
    subs_paid_in_advance,
    secondary_offer_value,
    sub_engagement_period
FROM `teamdata-291012.{{ country.dataset }}.wp_jb_sub_offers` b

{{ "UNION ALL" if not loop.last }}
{%- endfor %}


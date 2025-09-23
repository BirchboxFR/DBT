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
    max_use,
    shipping,
    parent_id,
    conditions,
    created_at,
    created_by,
    store_only,
    updated_at,
    valid_from,
    description,
    allow_choose,
    start_box_id,
    discount_type,
    influencer_id,
    is_for_raffed,
    validity_date,
    applies_to_gift,
    conditions_text,
    discount_amount,
    discount_on_sub,
    excluded_brands,
    force_start_box,
    actif_point_rouge,
    discount_amount_2,
    excluded_products,
    influencer_points,
    keep_sub_discount,
    special_box_label,
    can_not_use_points,
    non_combinable_raf,
    can_not_gain_points,
    box_price_adjustment,
    subs_paid_in_advance,
    sub_engagement_period,
    actif_non_discountable,
    can_use_with_points_voucher
FROM `teamdata-291012.{{ country.dataset }}.wp_jb_coupons` b

{{ "UNION ALL" if not loop.last }}
{%- endfor %}


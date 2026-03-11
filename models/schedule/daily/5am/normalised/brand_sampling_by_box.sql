{{
  config(
    materialized='table',
    partition_by={
      "field": "box_month",
      "data_type": "date",
      "granularity": "month"
    }
  )
}}

select 
  dw_country_code,
  box_date,
  box_month,
  component_brand_name as brand_name,
  count(case when component_codification_lvl2 = 'Product' then fz_sku end) as total_sampled
from {{ ref('sku_by_user_by_box') }}
where box_id >= 130 
group by 1,2,3,4
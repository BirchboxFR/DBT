{{
  config(
    materialized='table',
    partition_by={
      "field": "box_id",
      "data_type": "int64",
      "range": {
        "start": 1,
        "end": 1000,
        "interval": 1
      }
    },
    cluster_by=['dw_country_code', 'user_id'],
    require_partition_filter=true
  )
}}

SELECT 
  concat(bs.dw_country_code, '_', bs.user_id) as user_key,
  bs.dw_country_code,
  bs.sub_id,
  bs.user_id,
  bs.box_id,
  bs.coffret_id,
  kt.kit_sku,
  case 
    when kt.component_sku in (
      'POL-NUITPOLAIRE-JBX1602',
      'POL-MASQUENUITPOLAI-JBX1911',
      'POL-NUITPOLAIRE-JBX1703',
      'POL-CREMEVISAGENUIT-JBX2010',
      'POL-NUITPOLAIRE-JBX1802',
      'POL-CREMENUITPOLAIR-JBX2401'
    ) then 'POL-CREMEVISAGENUIT-JBX2302' 
    else kt.component_sku 
  end as component_sku,
  email,
  component_product_id,
  kit_product_id,
  spl.product_id as fz_product_id,
  case 
    when fz.sku = 'POL-NUITPOLAIRE-ESHOP' then 'POL-CREMEREVITALISANT-ESHOP' 
    else fz.sku 
  end as fz_sku,
  fz.product_categories_lvl1,
  fz.product_categories_lvl2,
  fz.product_categories_lvl3,
  component_codification_lvl2,
  kt.box_date,
  kt.box_month,
  kt.box_year,
  fz.name as product_name,
  kit_brand_name,
  component_brand_name,
  ean,
  ii.euro_purchase_price AS component_purchase_price,
  ii.logistic_category AS component_logistic_category

from `teamdata-291012.sales.box_sales` bs
left join `teamdata-291012.product.kit_details` kt 
  using(box_id, dw_country_code, coffret_id)
left join `teamdata-291012.inter.sample_product_link` spl on spl.sample_id = component_product_id and spl.dw_country_code = bs.dw_country_code
left join `teamdata-291012.inter.products` fz on fz.id = spl.product_id and fz.dw_country_code = spl.dw_country_code
LEFT JOIN (SELECT sku, euro_purchase_price FROM `teamdata-291012.catalog.inventory_item_catalog`) ii ON kt.component_sku = ii.sku
inner join (
  select user_id, email, dw_country_code 
  from `teamdata-291012.user.customers`
) c 
  on c.user_id = bs.user_id 
  and c.dw_country_code = bs.dw_country_code
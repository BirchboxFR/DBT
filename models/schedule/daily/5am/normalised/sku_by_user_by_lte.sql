{{
  config(
    materialized='table',
    partition_by={
      "field": "product_id",
      "data_type": "int64",
      "range": {
        "start": 1,
        "end": 1000,
        "interval": 1
      }
    },
    cluster_by=['dw_country_code', 'user_id']
  )
}}

select distinct ss.product_codification,ss.dw_country_code,ss.product_id, ss.product_name,lower(c.product_nice_name) product_name,  c.brand_full_name,user_id,c.sku,c.category_lvl_1,c.category_lvl_2,c.category_lvl_3 from sales.shop_sales ss
inner join `teamdata-291012.inter.lte_kits` l on l.lte_product_id=ss.product_id and l.dw_country_code=ss.dw_country_code
inner join product.catalog c on c.product_id=l.product_id and c.dw_country_code=l.dw_country_code
--inner join product.catalog enfant on enfant.product_id=c.product_id and enfant.dw_country_code=ss.dw_country_code
where product_codification in ('LTE','SPLENDIST','CALENDAR')
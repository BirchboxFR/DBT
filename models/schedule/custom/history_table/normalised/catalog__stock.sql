{{ config(
    materialized='incremental',
    unique_key='archive_date',
    partition_by={
      "field": "archive_date", 
      "data_type": "date"
    },
    schema='history_table'
) }}



SELECT current_date AS archive_date,  
s.sku, s.pc_cat1, s.pc_cat2, s.codification, s.brand_full_name, s.name, s.stock, cast(s.sale_price as float64) as sale_price, s.euro_purchase_price, s.bank_reservations, s.available_stock, s.valo_stock, s.valo_available_stock,
SAFE_CAST(s.dluo_min AS DATE), s.stock_POT1, s.stock_POT2, s.product_class, s.stock_coverage, SAFE_CAST(s.dluo_min AS DATE) AS dluo_min, NULL AS row_num
 FROM `normalised-417010.catalog.stock` s

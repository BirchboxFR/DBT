{{ config(
    materialized='table'
) }}


{{ config(
    materialized='table'
) }}

SELECT 
  'FR' as dw_country_code,
  ID,
  SKU,
  date,
  stock,
  tampon,
  created_at,
  product_id,
  updated_at,
  stock_scamp,
  stock_physique,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_products_stock_log_2012

UNION ALL

SELECT 
  'FR' as dw_country_code,
  ID,
  SKU,
  date,
  stock,
  tampon,
  created_at,
  product_id,
  updated_at,
  stock_scamp,
  stock_physique,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_products_stock_log_2013

UNION ALL

SELECT 
  'FR' as dw_country_code,
  ID,
  SKU,
  date,
  stock,
  tampon,
  created_at,
  product_id,
  updated_at,
  stock_scamp,
  stock_physique,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_products_stock_log_2014

UNION ALL

SELECT 
  'FR' as dw_country_code,
  ID,
  SKU,
  date,
  stock,
  tampon,
  created_at,
  product_id,
  updated_at,
  stock_scamp,
  stock_physique,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_products_stock_log_2015

UNION ALL

SELECT 
  'FR' as dw_country_code,
  ID,
  SKU,
  date,
  stock,
  tampon,
  created_at,
  product_id,
  updated_at,
  stock_scamp,
  stock_physique,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_products_stock_log_2016

UNION ALL

SELECT 
  'FR' as dw_country_code,
  ID,
  SKU,
  date,
  stock,
  tampon,
  created_at,
  product_id,
  updated_at,
  stock_scamp,
  stock_physique,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_products_stock_log_2017

UNION ALL

SELECT 
  'FR' as dw_country_code,
  ID,
  SKU,
  date,
  stock,
  tampon,
  created_at,
  product_id,
  updated_at,
  stock_scamp,
  stock_physique,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_products_stock_log_2018

UNION ALL

SELECT 
  'FR' as dw_country_code,
  ID,
  SKU,
  date,
  stock,
  tampon,
  created_at,
  product_id,
  updated_at,
  stock_scamp,
  stock_physique,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_products_stock_log_2019

UNION ALL

SELECT 
  'FR' as dw_country_code,
  ID,
  SKU,
  date,
  stock,
  tampon,
  created_at,
  product_id,
  updated_at,
  stock_scamp,
  stock_physique,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_products_stock_log_2020

UNION ALL

SELECT 
  'FR' as dw_country_code,
  ID,
  SKU,
  date,
  stock,
  tampon,
  created_at,
  product_id,
  updated_at,
  stock_scamp,
  stock_physique,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_products_stock_log_2021

UNION ALL

SELECT 
  'FR' as dw_country_code,
  ID,
  SKU,
  date,
  stock,
  tampon,
  created_at,
  product_id,
  updated_at,
  stock_scamp,
  stock_physique,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_products_stock_log_2022

UNION ALL

SELECT 
  'FR' as dw_country_code,
  ID,
  SKU,
  date,
  stock,
  tampon,
  created_at,
  product_id,
  updated_at,
  stock_scamp,
  stock_physique,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_products_stock_log_2023
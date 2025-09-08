{{ config(
    materialized='table'
) }}


{{ config(
    materialized='table'
) }}

SELECT 
  'FR' as dw_country_code,
  id,
  user_id,
  order_id,
  sub_id,
  payment_gateway_id,
  transaction_id,
  amount,
  payment_profile_id,
  auto,
  status_id,
  payment_method_id,
  data,
  date(date) AS date,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_payments_2012

UNION ALL

SELECT 
  'FR' as dw_country_code,
  id,
  user_id,
  order_id,
  sub_id,
  payment_gateway_id,
  transaction_id,
  amount,
  payment_profile_id,
  auto,
  status_id,
  payment_method_id,
  data,
  date(date) AS date,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_payments_2013

UNION ALL

SELECT 
  'FR' as dw_country_code,
  id,
  user_id,
  order_id,
  sub_id,
  payment_gateway_id,
  transaction_id,
  amount,
  payment_profile_id,
  auto,
  status_id,
  payment_method_id,
  data,
  date(date) AS date,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_payments_2014

UNION ALL

SELECT 
  'FR' as dw_country_code,
  id,
  user_id,
  order_id,
  sub_id,
  payment_gateway_id,
  transaction_id,
  amount,
  payment_profile_id,
  auto,
  status_id,
  payment_method_id,
  data,
  date(date) AS date,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_payments_2015

UNION ALL

SELECT 
  'FR' as dw_country_code,
  id,
  user_id,
  order_id,
  sub_id,
  payment_gateway_id,
  transaction_id,
  amount,
  payment_profile_id,
  auto,
  status_id,
  payment_method_id,
  data,
  date(date) AS date,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_payments_2016

UNION ALL

SELECT 
  'FR' as dw_country_code,
  id,
  user_id,
  order_id,
  sub_id,
  payment_gateway_id,
  transaction_id,
  amount,
  payment_profile_id,
  auto,
  status_id,
  payment_method_id,
  data,
  date(date) AS date,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_payments_2017

UNION ALL

SELECT 
  'FR' as dw_country_code,
  id,
  user_id,
  order_id,
  sub_id,
  payment_gateway_id,
  transaction_id,
  amount,
  payment_profile_id,
  auto,
  status_id,
  payment_method_id,
  data,
  date(date) AS date,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_payments_2018

UNION ALL

SELECT 
  'FR' as dw_country_code,
  id,
  user_id,
  order_id,
  sub_id,
  payment_gateway_id,
  transaction_id,
  amount,
  payment_profile_id,
  auto,
  status_id,
  payment_method_id,
  data,
  date(date) AS date,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_payments_2019

UNION ALL

SELECT 
  'FR' as dw_country_code,
  id,
  user_id,
  order_id,
  sub_id,
  payment_gateway_id,
  transaction_id,
  amount,
  payment_profile_id,
  auto,
  status_id,
  payment_method_id,
  data,
  date(date) AS date,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_payments_2020

UNION ALL

SELECT 
  'FR' as dw_country_code,
  id,
  user_id,
  order_id,
  sub_id,
  payment_gateway_id,
  transaction_id,
  amount,
  payment_profile_id,
  auto,
  status_id,
  payment_method_id,
  data,
  date(date) AS date,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_payments_2021

UNION ALL

SELECT 
  'FR' as dw_country_code,
  id,
  user_id,
  order_id,
  sub_id,
  payment_gateway_id,
  transaction_id,
  amount,
  payment_profile_id,
  auto,
  status_id,
  payment_method_id,
  data,
  date(date) AS date,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_payments_2022

UNION ALL

SELECT 
  'FR' as dw_country_code,
  id,
  user_id,
  order_id,
  sub_id,
  payment_gateway_id,
  transaction_id,
  amount,
  payment_profile_id,
  auto,
  status_id,
  payment_method_id,
  data,
  date(date) AS date,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM teamdata-291012.Archives.wp_jb_payments_2023
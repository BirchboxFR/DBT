
{{
  config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date"
    },
    cluster_by=["dw_country_code"]
  )
}}



SELECT 
       DATE(payment_date) AS date,shipping_Date as first_day,
       b.date as month ,
       bs.dw_country_code,'yearly' type ,'BOX' as product_type,'ACQUIZ' acquis_type,
       SUM(discount) AS discount
FROM sales.box_sales bs
join inter.boxes using(dw_country_code,date)
inner join inter.boxes_by_day b on b.d= DATE(payment_date) and b.dw_country_code=bs.dw_country_code
WHERE yearly = 1
AND bs.dw_country_code IN ('FR', 'DE')
and year>2021
AND sub_payment_status_id = 1
GROUP BY 1,2,3,4,5,6
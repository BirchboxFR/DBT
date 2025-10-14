
{{
  config(
    materialized='table',
    partition_by={
      "field": "payment_date",
      "data_type": "date",
       "granularity": "month"
    },
    cluster_by=["dw_country_code"]
  )
}}

select dw_country_code,payment_date,date as month, acquis_status_lvl2,shipping_Date as first_day,count(distinct sub_id)nb_subs from sales.box_sales bs
join inter.boxes using(dw_country_code,date)
where acquis_status_lvl1='ACQUISITION'
and payment_status='paid'
and payment_Date >='2022-01-01'
and diff_current_box<=0
group by all

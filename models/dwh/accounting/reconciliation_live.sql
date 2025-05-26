{{ config(
    materialized='view',
    on_schema_change='ignore'
) }}

with GIft as (
SELECT date,
case when store_code = 'FR' then 'BirchboxFR'
when store_code = 'DE' then 'BlissimDE'
when store_code = 'ES' then 'BlissimES'
when store_code = 'IT' then 'BlissimIT'
when store_code = 'Store' then 'BirchboxFR_Store01'
else 'Unknown store' end as merchantAccountCode
,

 ifnull(SUM(gross_revenue_ttc) - SUM(total_discount_ttc),0) AS cash_gift
FROM (SELECT 'GIFT' AS product_codification,date_trunc(order_date,month) as date,
       ss.store_code,
       CAST(NULL AS STRING) AS shipping_country,
       CAST(NULL AS STRING) AS shipping_country_classification,
       SUM(gross_revenue + vat_on_gross_revenue) AS gross_revenue_ttc,
       SUM(total_discount + vat_on_total_discount) AS total_discount_ttc
FROM {{ ref('shop_sales') }} ss
WHERE product_codification_id = 34

GROUP BY ss.store_code,date)
GROUP BY merchantAccountCode,date



),
tktk as (

select 'BirchboxFR' as merchantAccountCode, date_trunc(order_date,month) as date, sum(net_revenue)*1.2 as net
from sales.shop_sales 
where dw_country_code='FR'
and store_id = 3
group by all

)
,Adyen as (
select merchantAccountCode,date(date_trunc(eventDate, month)) as date,
ifnull(sum(case when eventcode IN('AUTHORISATION','REFUND_FAILED') then value/100 end ),0) as adyen_authorisation,
ifnull(sum(case when eventcode in ('REFUND','CANCEL_OR_REFUND') then value/100 end ),0) as adyen_refund,
ifnull(sum(case when eventcode='AUTHORISATION' then value/100 end ),0)-ifnull(sum(case when eventcode in ('REFUND','CANCEL_OR_REFUND') then value/100 end ),0) as Adyen_total
 from {{ ref('adyen_notifications') }}
 
where success=1  
group by all

),

shop as (
SELECT case when store_code = 'FR' then 'BirchboxFR'
when store_code = 'DE' then 'BlissimDE'
when store_code = 'ES' then 'BlissimES'
when store_code = 'IT' then 'BlissimIT'
when store_code = 'Store' then 'BirchboxFR_Store01'
else 'Unknown store' end as merchantAccountCode,
date_trunc(date,month) as date,
 SUM(credit) - SUM(debit) AS cash_shop
FROM {{ ref('shop_detailed') }}

GROUP BY ALL
order by date desc
),

box as (
SELECT 
cast(concat(report_year,'-',case when report_month <10 then concat('0',cast(report_month as string))  else cast(report_month as string) end,'-01') as date) as date,
case when store_code = 'FR' then 'BirchboxFR'
when store_code = 'DE' then 'BlissimDE'
when store_code = 'ES' then 'BlissimES'
when store_code = 'IT' then 'BlissimIT'
when store_code = 'Store' then 'BirchboxFR_Store01'
else 'Unknown store' end as merchantAccountCode,
 ifnull(SUM(gross_revenue + vat_on_gross_revenue - (discount + vat_on_discount) + shipping + vat_on_shipping),0) AS cash_box
FROM {{ ref('box_turnover') }}
WHERE payment_period = '02- current_month'
GROUP BY ALL



)

select  merchantaccountcode,date,adyen_authorisation,adyen_refund,Adyen_total, cash_gift,cash_shop,cash_box ,
 cash_box+cash_shop+Cash_GIft as Query_total,
 (cash_box+cash_shop+Cash_GIft) -Adyen_total as ecart

 from (

select  merchantaccountcode,date,ifnull(adyen_authorisation,0)adyen_authorisation,ifnull(adyen_refund,0)adyen_refund,Adyen_total,ifnull(cash_gift,0) cash_gift,ifnull(cash_shop,0)cash_shop,ifnull(cash_box,0)cash_box,net from adyen 
left join GIft using(merchantaccountcode,date)
left join shop using(merchantaccountcode,date)
left join box using(merchantaccountcode,date)
left join tktk using(merchantaccountcode,date)
)

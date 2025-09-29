{{
  config(
    materialized='table',
    partition_by={
      "field": "first_date_campaign",
      "data_type": "date"
    }
  )
}}



SELECT avg(cast(round(l.predicted_ltv,0 ) as int64)) as predicted_ltv,c.user_id,t.*,min(date) as first_date_campaign
FROM `teamdata-291012.funnel.funnel_data_fbclid_event_IDtransaction_ID` t 
inner join (select distinct user_id,dw_country_code,order_id from sales .box_sales where dw_country_code='FR' ) bs on cast(bs.order_id as string)=t.Transaction_ID___GA4__Google_Analytics
inner join user.customers c on c.user_id=bs.user_id and c.dw_country_code=bs.dw_country_code
inner join teamdata-291012.predictive_ltv.ltv l on l.user_id=bs.user_id and l.dw_country_code=bs.dw_country_code
where Session_campaign___GA4__Google_Analytics<>'(not set)' and Session_campaign___GA4__Google_Analytics like 'ACQ%'
group by all

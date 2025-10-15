



SELECT cast(round(l.predicted_ltv,0 ) as int64) as predicted_ltv,o.created_at as conversion_time, bs.user_id,email,c.billing_phone,firstname,lastname,string_agg( cast(d.product_id as string)) products,birth_date, t.*

 FROM `teamdata-291012.funnel.funnel_data_fbclid_event_IDtransaction_ID` t 
inner join (select distinct user_id,dw_country_code,order_id from sales .box_sales where dw_country_code='FR' ) bs on cast(bs.order_id as string)=t.Transaction_ID___GA4__Google_Analytics
inner join user.customers c on c.user_id=bs.user_id and c.dw_country_code=bs.dw_country_code
inner join teamdata-291012.predictive_ltv.ltv l on l.user_id=bs.user_id and l.dw_country_code=bs.dw_country_code
left join inter.orders o on cast(o.id as string)=t.Transaction_ID___GA4__Google_Analytics and o.dw_country_code='FR'
left join inter.order_details d on d.order_id=o.id and d.dw_country_code='FR'
where  FBCLID___GA4__Google_Analytics<>'(not set)'
group by all

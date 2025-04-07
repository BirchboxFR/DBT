select distinct c.user_id,
last_box_paid_date,last_shop_order_date,
box_sub_status as status_lvl1,
'PROSPECTS' as status
 from teamdata-291012.user.customers c
 left join {{ ref('today_new') }} n on n.user_id=c.user_id 
where (date_diff(current_date(),last_box_paid_date,month) >36 
or
last_box_paid_date is null) and (
 date_diff(current_date(),last_shop_order_date,month) >36 
or last_shop_order_date is null
 )
and box_sub_status in ('CHURN','NEVERSUB')

and dw_country_code='FR'
and n.user_id is null

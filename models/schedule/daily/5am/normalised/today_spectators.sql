with shop_infos as (

select user_id,max(order_date) last_order_date,
sum(case when date_diff(current_date(),order_date,month)<=12 then gross_profit end ) as gp_shop_L12M
 from `teamdata-291012.sales.shop_orders_margin`group by all

)


select *,'SPECTATORS' as status from (
select dw_country_code,c.user_id,last_consecutive_box_paid, registration_date,is_shopper,min(box_id) first_box,min(date) first_date,max(case when diff_current_box <=0 and payment_status='paid' then date end ) last_box_paid_date,last_order_date,
date_diff(current_Date,min(date),month) nb_box_payable,
count(distinct case when diff_current_box <=0 and payment_status='paid' then date end )nb_box_paid,
gp_shop_L12M
from `teamdata-291012.user.customers` c
left join sales.box_sales s using( user_id,dw_country_code)
left join shop_infos using(user_id)
left join {{ ref('today_new') }} n on n.user_id=c.user_id 
left join {{ ref('today_whales') }} w on w.user_id=c.user_id 
left join {{ ref('today_stars') }} stars on stars.user_id=c.user_id 
where dw_country_code='FR' and n.user_id is null and w.user_id is null and stars.user_id is null
group by all)
where nb_box_paid >14
and (
safe_divide(nb_box_paid,nb_box_payable) >0.8 
)
and last_box_paid_date >'2024-08-01'
and (not is_shopper or last_order_date <'2023-01-01')


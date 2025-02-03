
select cat as status_lvl1,user_id, 'NEW' as status
from (

select  distinct 'box_new_new' as cat,user_id,min(date)  from sales.box_sales
where date_diff(current_date(),date,month)<3
and acquis_status_lvl2='NEW NEW'
and dw_country_code='FR'and diff_current_box<=0
group by all
union all

/*
select distinct 'shop_new',user_id,min(order_date) from sales.shop_sales ss
inner join shop using(user_id,dw_country_code)
where date_diff(current_date(),first_achat,day)<=60
group by all

union all
*/
select  distinct 'box_old_new_new',user_id,min(date) 
from sales.box_sales
where date_diff(current_date(),date,month)<3
and acquis_status_lvl1='ACQUISITION'
and date_diff(date,last_box_received_date,month)>=15 and diff_current_box<=0
and dw_country_code='FR'
group by all
)

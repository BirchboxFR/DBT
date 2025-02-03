select distinct t.user_id,box_sub_status as status_lvl1,'RISKY' as status from (
--end of gift
select distinct user_id,'end of gift' as status,box_sub_status
from sales.box_sales bs
inner join `teamdata-291012.user.customers` c using(user_id,dw_country_code)
where bs.gift = 1 AND bs.last_committed_box = 1
and date='2025-02-01' and dw_country_code='FR'
union all

-- moins de 7 box et sub
select distinct user_id ,'moins de 7 box',box_sub_status
from `teamdata-291012.user.customers`
where nb_box_paid <7
and box_sub_status='SUB'

-- les acquiz mega discount
union all
select distinct user_id,'mega discount',box_sub_status
from sales.box_sales bs
inner join `teamdata-291012.user.customers` c using(user_id,dw_country_code)
where  date>='2024-01-01' and dw_country_code='FR'
and bs.acquis_status_lvl2 IN ('NEW NEW','REACTIVATION') AND ( safe_divide(bs.total_discount,total_product) > 0.3) 
and box_sub_status='SUB'



-- react m-1 et m-2

)t
left join {{ ref('today_propsects') }} p on p.user_id=t.user_id 
left join {{ ref('today_spectators') }} sp on sp.user_id=t.user_id 
left join {{ ref('today_new') }} n on n.user_id=t.user_id 
left join {{ ref('today_whales') }} w on w.user_id=t.user_id 
left join {{ ref('today_stars') }} stars on stars.user_id=t.user_id 
where p.user_id is null and sp.user_id is null and n.user_id is null and w.user_id is null and stars.user_id is null

group by all


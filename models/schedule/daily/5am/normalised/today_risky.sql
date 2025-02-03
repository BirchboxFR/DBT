select distinct user_id,status as status_lvl1,'RISKY' as status from (
--end of gift
select distinct user_id,'end of gift' as status
from sales.box_sales bs
where bs.gift = 1 AND bs.last_committed_box = 1
and date='2024-11-01' and dw_country_code='FR'
union all

-- moins de 7 box et sub
select distinct user_id ,'moins de 7 box'
from `teamdata-291012.user.customers`
where nb_box_paid <7
and box_sub_status='SUB'

-- les acquiz mega discount
union all
select distinct user_id,'mega discount'
from sales.box_sales bs
where  date='2024-11-01' and dw_country_code='FR'
and bs.acquis_status_lvl2 IN ('NEW NEW','REACTIVATION') AND ( safe_divide(bs.total_discount,total_product) > 0.3) 




-- react m-1 et m-2

)
where user_id not in 

(select user_id from {{ ref('today_stars') }}
union all
select user_id from {{ ref('today_whales') }}
union all
select user_id from {{ ref('today_new') }}
union all
select user_id from {{ ref('today_spectators') }}
union all
select user_id from {{ ref('today_prospects') }}
)
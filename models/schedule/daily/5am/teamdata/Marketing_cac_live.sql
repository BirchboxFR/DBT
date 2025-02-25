
with marketing_live as (

SELECT d as jour,country,sum(spent) spent  FROM {{ ref('Marketing_cac_expenses') }}

where type<>'SHOP' --and levier  is not null 
and cac ='Oui'
and d>='2023-10-01'
 group by all
) 


, acq as (
select dw_country_code as country_code,date(payment_date) as d,cast(date  as string) as m,
count(case when acquis_status_lvl2 ='NEW NEW' then sub_id end ) as self,  
count(case when acquis_status_lvl2 ='REACTIVATION' then sub_id end ) as reactivations,
count(case when acquis_status_lvl2 ='GIFT' then sub_id end ) as gift,
count(* ) as total_this_year,
from sales.box_sales
where 1=1
and acquis_status_lvl1<>'LIVE'
--and date_trunc(payment_date,month)=date -- les cas pourris ou il ya plusieurs mois pour un jour ex 1 ocotbre 2024
and day_in_cycle>0
group by all
)

select * except (country_code,jour) from marketing_live ml
inner join acq a on country_code=country and jour=a.d
SELECT c.dw_country_code,cart_id,cd.created_at created_utc ,datetime(cd.created_at,'Europe/Paris') as date,concat(c.dw_country_code,'_',user_id) as user_key ,
cd.product_id, c.status_id,p.brand_full_name,p.ht_sale_price,p.product_nice_name,p.thumb_url,pro.price,pro.attr_special_sub_price,pro.attr_special_price,
case 

when datetime_diff(current_datetime('Europe/Paris'), datetime(cd.created_at,'Europe/Paris'), hour)>1  and c.status_id in (1,2) then true else false end as  is_abandonned,
datetime_diff(current_datetime('Europe/Paris'), datetime(cd.created_at,'Europe/Paris'), hour) as cart_age_hours,
datetime_diff(current_datetime('Europe/Paris'), datetime(cd.created_at,'Europe/Paris'), minute) as cart_age_minutes,
sum(cd.qty) as nb_items,
case when datetime_diff(current_datetime(), c.created_at, hour)<48 then true else false end as winback_possible
FROM `teamdata-291012.inter.saved_cart` c
left join {{ ref('saved_cart_details') }} cd on c.id=cd.cart_id and c.dw_country_code=cd.dw_country_code
left join {{ ref('catalog') }} p on p.product_id=cd.product_id and p.dw_country_code=cd.dw_country_code
left join {{ ref('products') }} pro on pro.id=cd.product_id and pro.dw_country_code=cd.dw_country_code
where 1=1

group by all
--having cart_age_hours < 1000
having  user_key='FR_2986148'
order by date desc
--LIMIT 1000
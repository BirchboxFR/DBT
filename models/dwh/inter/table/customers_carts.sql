SELECT c.dw_country_code,datetime(c.created_at) as date,concat(c.dw_country_code,'_',user_id) as user_key ,
cd.product_id, c.status_id,p.brand_full_name,p.ht_sale_price,p.product_nice_name,p.thumb_url,
case when timestamp_diff(current_timestamp(), c.created_at, hour)>1  and c.status_id in (1,2) then true else false end as  is_abandonned,
timestamp_diff(current_timestamp(), c.created_at, hour) as cart_age_hours,
case when timestamp_diff(current_timestamp(), c.created_at, hour)<48 then true else false end as winback_possible
FROM `teamdata-291012.inter.saved_cart` c
left join inter.saved_cart_details cd on c.id=cd.cart_id and c.dw_country_code=cd.dw_country_code
left join product.catalog p on p.product_id=cd.product_id and p.dw_country_code=cd.dw_country_code
where 1=1
and user_id=2622634
group by all
--having cart_age_hours < 1000
order by date desc
--LIMIT 1000
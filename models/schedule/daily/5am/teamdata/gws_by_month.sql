select date_trunc(date(d.created_at),month)date,p.sku,p.product_categories_lvl1,p.product_categories_lvl2,p.product_categories_lvl3,
p.name,product_id, p.brand_id,b.name as brand_name,count(distinct sub_id) nb from sales.box_sales bs
left join inter.order_details d using(order_id,dw_country_code)
left join inter.products p on p.id=d.product_id and p.dw_country_code=d.dw_country_code
left join inter.brands b on b.post_id=p.brand_id and b.dw_country_code=d.dw_country_code
inner join inter.boxes bo on bo.shipping_date<=date(d.created_at)
where 1=1
--and date_trunc(date(d.created_at),month)='2026-03-01'
and acquis_status_lvl1='ACQUISITION'
and bs.dw_country_code='FR'
and d.special_type='GWS' 
--and b.name='Dermalogica'
--and bs.order_detail_id=23380796
--and p.id=72969
and product_id<>1 and lower(p.name) not like '%flyer%' and lower(p.name) not like '%box%'
group by all
having date>='2024-01-01' --and nb>100

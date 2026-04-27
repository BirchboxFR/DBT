select distinct case when product_codification ='GWS/GWP' then 'GWP' else product_codification end as product_codification ,ss.dw_country_code,product_name,ss.sku,brand_name,
product_categories_lvl1,
product_categories_lvl2,
product_categories_lvl3,
 min(DATE(CAST(year AS INT64), CAST(month AS INT64), 1) )AS date_debut from sales.shop_sales ss
 inner join inter.products p on p.id=ss.product_id and p.dw_country_code=ss.dw_country_code
 where product_codification in ('ESHOP','GWS/GWP')
group by all

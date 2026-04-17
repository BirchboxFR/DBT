select distinct product_codification,dw_country_code,lte_name,product_name,sku,
lte_sku,product_categories_lvl1,product_categories_lvl2,product_categories_lvl3,
 min(DATE(CAST(year AS INT64), CAST(month AS INT64), 1) )AS date_debut from `normalised-417010.shop.sku_by_user_by_lte`
group by all
order by date_debut desc,lte_name desc
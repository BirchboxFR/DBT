select distinct box_date,dw_country_code,component_sku,box_id,component_brand_name,
product_categories_lvl1,
product_categories_lvl2,
product_categories_lvl3,
 from `normalised-417010.box.sku_by_user_by_box`
where box_id>150
and component_codification_lvl2='Product'
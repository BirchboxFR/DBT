
{{ config(
    partition_by={
    "field": "box_date",
      "data_type": "date"
    },
    cluster_by=['review_date', 'dw_country_code', 'brand_name']
) }}


WITH catalog_filtered AS (
  SELECT
    ca.*,
    ROW_NUMBER() OVER (PARTITION BY ca.sku ORDER BY 
      CASE 
        WHEN ca.product_post_id IS NOT NULL THEN 1 
        WHEN ca.visible = 'catalog_and_search' THEN 2 
        ELSE 3 
      END
    ) as row_num
  FROM `teamdata-291012.product.catalog` ca
 -- where  sku='CAU-SOINCORPSTDV-JBX2205'

)
select ifnull(s.dw_country_code,original_language) as dw_country_code,FORMAT_DATE('%Y-%m',bo.date) AS box_month,
r.box_id as box_id,bo.date as box_date,date(r.created_at) review_date,r.id as review_id,ifnull(s.component_sku,ii.sku)as sku,iifz.sku as fz_sku,
ifnull(s.component_brand_name,b.name) as brand_name,ri.inventory_item_id,component_codification_lvl2,brand_lvl1,brand_lvl2,brand_lvl3,
b.id as brand_id,r.ip,
category_lvl_1,category_lvl_2,category_lvl_3,
rimini.priority,
ifnull(s.ean,ii.ean)ean,
ifnull(s.product_name,ii.name)product_name,
max(product_nice_name) as product_nice_name,
replace(JSON_EXTRACT(trim(r.title), '$.FR'),'"', '') as FR_title,
replace(JSON_EXTRACT(trim(r.comment), '$.FR'),'"', '') as FR_comment,
r.moderation_status,rating,
concat(c.dw_country_code,'_',c.user_id)as user_key,
c.user_id,c.email,firstname,lastname,status,
case when sampled = 1 then true else false end as is_box,
count(distinct case when (status ='completed' and moderation_status ='validated') or ( status='done' and moderation_status='validated') then r.id end ) as nb_reviews,
cast((SELECT value FROM UNNEST(tab) WHERE slug = 'efficiency' LIMIT 1)as int64) AS efficiency,
cast( (SELECT value FROM UNNEST(tab) WHERE slug = 'texture' LIMIT 1) as int64) AS texture,
cast((SELECT value FROM UNNEST(tab) WHERE slug = 'fragrance' LIMIT 1)as int64) AS fragrance,
cast( (SELECT value FROM UNNEST(tab) WHERE slug = 'composition' LIMIT 1)as int64) AS composition,
cast( (SELECT value FROM UNNEST(tab) WHERE slug = 'ease-of-use' LIMIT 1)as int64) AS ease_of_use,
cast( (SELECT value FROM UNNEST(tab) WHERE slug = 'pigment' LIMIT 1)as int64) AS pigment,
cast( (SELECT value FROM UNNEST(tab) WHERE slug = 'wear' LIMIT 1)as int64) AS wear,
cast( (SELECT value FROM UNNEST(tab) WHERE slug = 'ease-of-application' LIMIT 1)as int64) AS ease_of_application,
cast( (SELECT value FROM UNNEST(tab) WHERE slug = 'color-intensity' LIMIT 1)as int64) AS color_intensity,
cast( (SELECT value FROM UNNEST(tab) WHERE slug = 'usefulness' LIMIT 1)as int64) AS usefulness,
cast( (SELECT value FROM UNNEST(tab) WHERE slug = 'taste' LIMIT 1)as int64) AS taste,
cast( (SELECT value FROM UNNEST(tab) WHERE slug = 'ease-of-intake' LIMIT 1)as int64) AS ease_of_intake,
cast( (SELECT value FROM UNNEST(tab) WHERE slug = 'resulting-shade' LIMIT 1)as int64) AS resulting_shade,
cast( (SELECT value FROM UNNEST(tab) WHERE slug = 'finish-on-hair' LIMIT 1)as int64) AS finish_on_hair,
cast( (SELECT value FROM UNNEST(tab) WHERE slug = 'feeling-of-comfort' LIMIT 1)as int64) AS feeling_of_comfort,
cast(  (SELECT value FROM UNNEST(tab) WHERE slug = 'finish-on-skin-eyes' LIMIT 1)as int64) AS finish_on_skin_eyes
from `teamdata-291012.bdd_prod_sublissim.review` r
inner join `teamdata-291012.bdd_prod_sublissim.review_author` ra on r.author_id=ra.id
inner join `teamdata-291012.bdd_prod_sublissim.review_item` ri on ri.id=r.full_size_review_item_id
left join `teamdata-291012.bdd_prod_sublissim.review_item` rimini on rimini.id=r.review_item_id
left join `teamdata-291012.bdd_prod_sublissim.review_item` rifz on rifz.id=full_size_review_item_id
inner join `teamdata-291012.bdd_prod_sublissim.inventory_item` ii on ii.id=rimini.inventory_item_id
inner join `teamdata-291012.bdd_prod_sublissim.inventory_item` iifz on iifz.id=rifz.inventory_item_id
inner join `normalised-417010.product.brands_enriched_table` b on b.brand_id=ii.brand_id and b.dw_country_code=r.original_language
left join `normalised-417010.box.sku_by_user_by_box` s on s.user_id=ra.external_id and s.box_id>=10 and component_sku=ii.sku and component_codification_lvl2 = 'Product' and s.dw_country_code=r.original_language
left join `teamdata-291012.inter.boxes` bo on bo.id=r.box_id and bo.dw_country_code=r.original_language
left join (select user_id,email,firstname,lastname,dw_country_code from `teamdata-291012.user.customers`)c on c.user_id=ra.external_id and r.original_language=c.dw_country_code
left join catalog_filtered ca on ca.sku=ii.sku and ca.dw_country_code=s.dw_country_code and ca.row_num = 1
LEFT JOIN (SELECT  review_id,ARRAY_AGG(STRUCT(slug, value)) AS tab 
FROM  `teamdata-291012.bdd_prod_sublissim.review_additional_response` rar
LEFT JOIN `teamdata-291012.bdd_prod_sublissim.review_additional_field` raf ON raf.id = rar.additional_field_id
GROUP BY 
review_id
) subquery on r.id = subquery.review_id

where (s.box_id <= (select id from `teamdata-291012.inter.boxes` where shipping_status_id=2 and dw_country_code='FR') or s.box_id is null)
AND DATE(r.created_at) >= DATE_ADD(ifnull(DATE(bo.date),'2000-01-01'), INTERVAL -8 DAY) 


group by all
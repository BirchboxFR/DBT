
  SELECT dw_country_code, box_id as mono_box_id, coffret_id as mono_coffret_id,max(component_brand_name) as mono_brand
  FROM
  (
  SELECT kd.dw_country_code, kd.box_id, kd.coffret_id, kd.box_date,  kd.component_brand_id, kd.component_brand_name, count(*) AS nb_distinct_brands
  FROM product.kit_details kd
  WHERE kd.component_brand_name NOT LIKE '%lissim%'
  AND kd.box_year >= 2018
  GROUP BY kd.box_id, kd.coffret_id, kd.box_date, kd.component_brand_id, kd.dw_country_code, kd.component_brand_name
  HAVING nb_distinct_brands =5
  ) t
  GROUP BY dw_country_code, box_id, coffret_id

-- PRODUITS AVEC DU STOCK
WITH products_with_stock as (
  SELECT sku, MAX(stock) as stock, MAX(stock_sublissim) AS stock_scamp
  FROM
(SELECT p.id,p.inventory_item_id, p.sku, p.dw_country_code, p.stock, i.stock as stock_sublissim
FROM inter.products p -- BRANCHE SUR PRODUCTS
LEFT JOIN `teamdata-291012.bdd_prod_sublissim.inventory_item` ii on ii.sku = p.sku 
LEFT JOIN `teamdata-291012.bdd_prod_sublissim.inventory` i ON i.inventory_item_id = ii.id 
where (p.type_produit <>"ESHOP" or (p.special_type is not null AND special_type <> '') or p.logistics_asset_id IN (2,3,4)) -- comment trouver que les produits shop autrement ?
AND p.stock_scamp > 0
)
 GROUP BY SKU
), 

-- PRODUITS AVEC DES COMMANDES EN COURS / EN COURS DE RECEPTION 
products_with_ongoing_command as (
  WITH already_delivered_po AS (
  SELECT DISTINCT po.id
  FROM bdd_prod_fr.wp_jb_reception_details rd
  INNER JOIN bdd_prod_fr.wp_jb_expected_inbound_details eid ON rd.expected_inbound_details_id = eid.id
  INNER JOIN bdd_prod_fr.wp_jb_expected_inbounds ei ON eid.expected_inbound_id = ei.id
  INNER JOIN bdd_prod_fr.wp_jb_purchase_orders po ON ei.purchase_order_id = po.id
  -- WHERE po.id = 23770
)

SELECT poi.sku AS SKU, SUM(poi.quantity_ordered) AS quantity_to_be_received, MAX(po.created_at) AS last_po_date
  FROM `bdd_prod_fr.wp_jb_purchase_orders` po
  INNER JOIN `bdd_prod_fr.wp_jb_purchase_order_items` poi ON po.id = poi.po_id
  -- INNER JOIN `bdd_prod_fr.wp_jb_products` p USING(sku)
  LEFT JOIN already_delivered_po adp ON po.id = adp.id
  WHERE po.status_id IN (4,5)
  AND po.created_at >= '2023-07-01'
   AND (po.project_id <> 114 OR po.project_id is null)
  --- AND poi.sku ="SAB-GELEELAMELLAIRE-JBX2407"
  AND adp.id IS NULL
  GROUP BY poi.sku 
  ), 

stock_in_kit as (
--SELECT p.dw_country_code, p.id, p.inventory_item_id, p.sku, sum(stock_in_kit) as stock_in_kit, CONCAT() FROM 
SELECT  p.sku, MAX(p_kit.stock_scamp) as stock_in_kit, STRING_AGG(distinct p_kit.sku) as kit_list, MAX( b.date) as last_time_box
FROM `inter.inventory_items` i 
JOIN inter.products p on p.inventory_item_id = i.id AND p.dw_country_code = i.dw_country_code
LEFT JOIN `product.all_kits` ak ON ak.product_id =p.id and ak.country_code = p.dw_country_code
LEFT JOIN inter.products p_kit ON p_kit.id = ak.kit_id and p_kit.dw_country_code = ak.country_code
LEFT JOIN inter.boxes b ON b.ID = p_kit.box_id and b.dw_country_code = p_kit.dw_country_code
WHERE  p_kit.stock > 0 
-- AND p.dw_country_code ="FR"
-- and  p.sku = "MEL-GUASHAJADEBLEUV-JBX2211"
GROUP BY  p.sku
), 
stock_in_kit2 as (
--SELECT p.dw_country_code, p.id, p.inventory_item_id, p.sku, sum(stock_in_kit) as stock_in_kit, CONCAT() FROM 
SELECT p.sku, MAX(p_kit.stock_scamp) as stock_in_kit,STRING_AGG(p_kit.sku) as kit_list, MAX( b.date) as last_time_box
FROM `inter.inventory_items` i 
JOIN inter.products p on p.inventory_item_id = i.id AND p.dw_country_code = i.dw_country_code
LEFT JOIN `product.all_kits` ak ON ak.product_id =p.id and ak.country_code = p.dw_country_code
LEFT JOIN inter.products p_kit ON p_kit.id = ak.kit_id and p_kit.dw_country_code = ak.country_code
LEFT JOIN inter.boxes b ON b.ID = p_kit.box_id and b.dw_country_code = p_kit.dw_country_code
-- WHERE -- p_kit.stock > 0 
-- p.dw_country_code ="FR"
GROUP BY  p.sku
), 
-- A BRANCHE SUR WM ensuite
products_in_kit as (
SELECT p.SKU AS SKU_kit,b.date as date_kit, STRING_AGG(distinct i.nice_name, ', ') AS list_products_in_kit
FROM product.all_kits kl 
LEFT JOIN inter.products p ON p.id = kl.kit_id and p.dw_country_code = kl.country_code
LEFT JOIN `inter.products` p2 ON p2.id = kl.product_id and p2.dw_country_code = kl.country_code
LEFT JOIN `catalog.inventory_item_catalog` i on i.sku = p2.sku 
LEFT join inter.boxes b ON b.id = p.box_id And p.dw_country_code = b.dw_country_code

GROUP BY p.SKU, b.date),


min_dluo as (

  WITH DLUOs as
(  SELECT SKU, MIN(dluo) as dluo_min ,
CASE WHEN dluo = MIn(dluo) then t.stock_position else 0 end as stock_dluo_min, 
CASE WHEN dluo = MIn(dluo) then position_key else NULL end as position_dluo_min
FROM(
SELECT p.dw_country_code, p.SKU , p.created_at, p.updated_at,  MAX(w.stock) as stock_position, w.position_key, format_date("%Y-%m-%d", SAFE_CAST(w.dluo AS DATE)) as dluo
FROM inter.products p 
LEFT JOIN  `teamdata-291012.bdd_prod_sublissim.pot_inventory_location` w ON w.SKU = p.SKU AND DATE(w.created_at) >=current_date()
WHERE  w.stock <> 0 
-- AND p.dw_country_code = "FR"
AND  DATE(w.created_at) >=current_date()
-- AND p.sku = "ADO-TOSCANAVITA-JBX2407"
GROUP BY  w.position_key, w.dluo, p.SKU , p.created_at, p.updated_at,p.dw_country_code, p.ID )t
GROUP BY  SKU, dluo, stock_position, position_key)
,
MINI_DLUO AS (
SELECT d.sku, MIN(d.dluo_min) as dluo_min --, d.stock_dluo_min
FROM DLUOs d 
GROUP BY d.sku

)

SELECT md.sku, md.dluo_min , sum(d.stock_dluo_min) as stock_dluo_min -- , d.position_dluo_min

FROM MINI_DLUO md 
LEFT JOIN DLUOs d on d.sku = md.sku  and d.dluo_min=md.dluo_min
GROUP BY md.sku, md.dluo_min ),

-- reservations en cours 
current_reservation as (
 SELECT sku, STRING_AGG(distinct project_reference) as project_reference, sum(Booked_volume) as Booked_volume, string_agg(concat(project_reference," - ", Booked_Volume)) as concat
  FROM(SELECT ic.sku,b.project_reference, MAX(b.Booked_volume) as Booked_Volume
FROM `catalog.inventory_item_catalog` ic 
JOIN catalog.booking b ON b.sku =ic.sku 
WHERE b.booked_volume > 0 
-- AND b.sku = "EGY-BAUMEMULTIUSAGE-JBX2402"
GROUP BY ic.sku, b.project_reference)
GROUP BY sku
), 
current_project AS 
(select project_full_name as c_project
FROM `catalog.projects` p 
 JOIN inter.boxes b ON b.date = p.start_date and b.dw_country_code = "FR"
 Join `snippets.current_box`  cb ON cb.current_box_id = b.id and cb.dw_country_code = "FR"
where project_type IN ("BOX", "GWS", "BOX ACQUIS")
), 
current_box_use AS (
select distinct p.sku as sku
FROM  inter.boxes b
Join `snippets.current_box`  cb ON cb.current_box_id= b.id AND b.dw_country_code = cb.dw_country_code 
LEFT JOIN inter.products p ON p.box_id = b.id and p.dw_country_code = b.dw_country_code
AND p.stock > 0
UNION ALL 
select distinct p.sku as sku
FROM  inter.boxes b
Join `snippets.current_box`  cb ON cb.current_box_id +1 = b.id AND b.dw_country_code = cb.dw_country_code 
LEFT JOIN inter.products p ON p.box_id = b.id and p.dw_country_code = b.dw_country_code
AND p.stock > 0

), 
current_lte_use AS (
 SELECT distinct SKU as sku FROM ( Select distinct ss.sku as sku , format_date ( "%m-%Y", min( order_date )) as min_date
  FROM sales.shop_sales  ss 
  WHERE ss.product_codification NOT IN ("LOYALTY", "ESHOP", "GWP", "GWS")
  GROUP BY ss.sku) 
  WHERE min_date = format_date ( "%m-%Y",  current_date()))
, 
depreciated_price as 
(SELECT SKU , MAX(date_start) as date_depreciation  , MAX(purchase_price_before) as purchase_price_before ,MIN(purchase_price_depreciated) as purchase_price_depreciated
FROM `ops.depreciation_detail` 
GROUP BY SKU )


SELECT 

-- INFORMATION DE INVENTORY ITEM CATALOG
/*MAX(ic.ID)*/ic.sku, MAX(ic.nice_name) as name, MAX(ic.brand_name) as brand_name,-- remplacer après par nice _name
MAX(ic.logistic_category) as logistic_category , MAX(planning_category_name) as planning_category_name, MAX(product_cat_lvl1) as product_cat_lvl1  ,MAX(product_cat_lvl2) as product_cat_lvl2 , MAX(product_cat_lvl3) as product_cat_lvl3,
MAX(ic.existing_product_types) as existing_product_types, MAX(size_type) as size_type, MAX(capacity) as capacity, MAX(capacity_unit) as capacity_unit, 
MAX(euro_purchase_price) as purchase_price, max(perceived_price) as perceived_price, 

-- STOCK 
COALESCE(pws.stock_scamp,0) as stock_scamp, 
pwc.quantity_to_be_received, 
MAX(sik.stock_in_kit) as stock_in_kit, 


-- TOTAL STOCK
COALESCE(pws.stock_scamp,0) +COALESCE(pwc.quantity_to_be_received,0)  AS Forecasted_stock, 
COALESCE(cr.Booked_Volume,0) as booked_volume, 
-- current available stock = current stock - booked stock
CASE  when COALESCE(pws.stock_scamp,0) -  COALESCE(cr.Booked_Volume,0)> 0 THEN COALESCE(pws.stock_scamp,0) - COALESCE(cr.Booked_Volume,0)
ELSE 0 END  as current_available_stock_corrected,
-- forecasted available stock = surrent + commands - booked 
CASE WHEN COALESCE(pws.stock_scamp,0) +COALESCE(quantity_to_be_received,0)- COALESCE(cr.Booked_Volume,0) >0 THEN COALESCE(pws.stock_scamp,0) +COALESCE(quantity_to_be_received,0)- COALESCE(cr.Booked_Volume,0) ELSE 0 END as forecasted_available_stock, 

-- SUJET DES SUR-Réservations ??

-- VALO STOCK 
ROUND(COALESCE(pws.stock_scamp,0) * MAX(euro_purchase_price)) as current_stock_value, 
(CASE  when COALESCE(pws.stock_scamp,0) - COALESCE(cr.Booked_Volume,0)> 0 THEN COALESCE(pws.stock_scamp,0) -COALESCE(cr.Booked_Volume,0)
ELSE 0 END) * MAX(euro_purchase_price) as available_stock_value,


-- DLUO MIN 
CASE WHEN MIN(md.dluo_min) = "3000-12-30" OR MIN(md.dluo_min) is NULL OR MAX(ic.logistic_category) IN ("print", "pack", "consumable item") THEN "" ELSE MIN(md.dluo_min) END as MIN_dluo, 
CASE WHEN MIN(md.dluo_min) = "3000-12-30" OR MIN(md.dluo_min) is NULL OR MAX(ic.logistic_category) IN ("print", "pack", "consumable item") THEN NULL ELSE MIN(md.stock_dluo_min) END as stock_min_dluo, 
CASE WHEN MIN(md.dluo_min) = "3000-12-30"  OR MIN(md.dluo_min) is NULL OR MAX(ic.logistic_category) IN ("print", "pack", "consumable item") then NULL ELSE  round(SAFE_DIVIDE(MIN(md.stock_dluo_min),MAX(pws.stock_scamp)),2) END as percent_dluo_min,

-- Products in kit si assembled product 
MAX(sik2.last_time_box) as last_time_in_box,
CASE WHEN MAX(ic.logistic_category) = "assembled product" then MAX(pik.list_products_in_kit) ELSE NULL END as products_in_kit, 

-- produit encore présent dans les kits affichage des kits
CASE WHEN MAX(sik.stock_in_kit) > 0 THEN MAX(sik.kit_list) ELSE NULL END AS kit_list, 

 cr.project_reference as current_booking, 
STRING_AGG(distinct f.flag_name) as flag, 

COALESCE(pws.stock_scamp,0)+COALESCE(quantity_to_be_received,0) -COALESCE(cr.Booked_Volume,0) as real_forecasted_available_stock, 

CASE WHEN MAX(cp.c_project) =cr.project_reference then NULL ELSE

      CASE WHEN (COALESCE(pws.stock_scamp,0)+COALESCE(quantity_to_be_received,0) -COALESCE(cr.Booked_Volume,0)) < -50 then 
"OVERBOOKING" ELSE NULL END 
END AS BOOKING_ALERT,

CASE WHEN MAX(cp.c_project) =cr.project_reference then NULL ELSE
CASE WHEN (coalesce(pws.stock_scamp,0)+ coalesce(quantity_to_be_received,0)- COALESCE(cr.Booked_Volume,0)) < -50 THEN 
STRING_AGG(distinct cr.concat ) ELSE NULL END END as alert_resume, 

CASE WHEN MAX(ic.logistic_category)= "assembled product" THEN 
  CASE WHEN MAX(ic.existing_product_types) = "BOX" AND MAX(cbu.sku) is not null then "YES" 
       WHEN  MAX(ic.existing_product_types) <> "BOX" and MAX(clu.sku) is not null then "YES" 
  ELSE NULL END 
  ELSE NULL END as Current_campaign, 

MAX(ic.note_moyenne) AS Average_grade, 
MIN ( dp.purchase_price_depreciated) as purchase_price_depreciated, 
MAX(dp.date_depreciation) as date_depreciation, 
MAX( ic.first_product_type  ) as first_product_type, 
STRING_AGG(distinct bf.brand_flag_name ) as brand_flag, 
MAX (ic.picture_link) as picture_link


FROM `catalog.inventory_item_catalog` ic
LEFT JOIN  products_with_stock pws ON pws.sku =ic.sku
LEFT JOIN products_with_ongoing_command pwc ON pwc.sku= ic.sku
LEFT JOIN  stock_in_kit sik ON sik.sku = ic.sku
LEFT JOIN stock_in_kit2 sik2 ON sik2.sku = ic.sku
LEFT JOIN catalog.booking b ON b.sku = ic.sku 
LEFT JOIN  min_dluo md ON md.sku = ic.sku
LEFT JOIN products_in_kit pik on pik.SKU_kit = ic.sku
LEFT JOIN current_reservation cr on cr.sku = ic.sku
LEFT JOIN catalog.flag f on f.sku = ic.sku
LEFT JOIN `teamdata-291012.catalog.brand_flag` bf on bf.brand_name = ic.brand_name
LEFT JOIN current_project cp ON cp.c_project=b.project_reference
LEFT JOIN current_box_use cbu ON cbu.sku = ic.sku
LEFT JOIN current_lte_use clu ON clu.sku = ic.sku
LEFT JOIN depreciated_price dp ON dp.sku = ic.sku  
WHERE (pws.stock_scamp > 0
OR pwc.quantity_to_be_received >0
OR sik.stock_in_kit > 0 
OR b.booked_volume > 0)
-- AND ic.sku= "AUT-DEFENSEBOTANICA-JBX2407"
GROUP BY ic.sku, pws.stock_scamp,  pwc.quantity_to_be_received  , cr.Booked_Volume,  cr.project_reference, cr.concat
ORDER BY pws.stock_scamp desc
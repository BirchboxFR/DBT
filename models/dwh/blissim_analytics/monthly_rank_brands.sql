WITH eligible_planning_categories AS (
  SELECT * EXCEPT(ratio_sell_out)
  FROM (
    SELECT brand_id,
           planning_category_1,
           SAFE_DIVIDE(sell_out, SUM(sell_out) OVER (PARTITION BY brand_id)) AS ratio_sell_out
    FROM (
      SELECT brand_id,
             planning_category_1,
             SUM(sell_out) AS sell_out
      FROM {{ ref('shop_sales') }} ss
      INNER JOIN {{ ref('products') }} p ON ss.dw_country_code = p.dw_country_code AND ss.product_id = p.id
      WHERE planning_category_1 IS NOT NULL
      AND ss.order_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 YEAR)
      AND ss.order_status = 'Validée'
      GROUP BY brand_id, planning_category_1
    )
  )
  WHERE ratio_sell_out >= 0.05
),
competitor_brands AS (
  SELECT DISTINCT t1.brand_id AS brand_id_1, t2.brand_id AS brand_id_2
  FROM eligible_planning_categories t1
  INNER JOIN eligible_planning_categories t2 USING(planning_category_1)
),
sell_out_by_brand_date AS (
  SELECT brand_id,
         DATE_TRUNC(order_date, MONTH) AS date,
         SUM(sell_out) AS sell_out
  FROM {{ ref('shop_sales') }} ss
  INNER JOIN {{ ref('products') }} p ON ss.dw_country_code = p.dw_country_code AND ss.product_id = p.id
  INNER JOIN eligible_planning_categories epc USING(brand_id, planning_category_1)
  WHERE planning_category_1 IS NOT NULL
  AND ss.order_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 YEAR)
  AND ss.order_status = 'Validée'
  GROUP BY brand_id, date
)
SELECT * EXCEPT(brand_comp)
FROM (
  SELECT date, brand_id, brand_comp, ROW_NUMBER() OVER (PARTITION BY date, brand_id ORDER BY sell_out_comp DESC) AS rang
  FROM (
    SELECT t1.brand_id, t1.date, t2.brand_id AS brand_comp, t2.sell_out AS sell_out_comp
    FROM sell_out_by_brand_date t1
    INNER JOIN competitor_brands cb ON t1.brand_id = cb.brand_id_1
    INNER JOIN sell_out_by_brand_date t2 ON t1.date = t2.date AND t2.brand_id = cb.brand_id_2
  )
)
WHERE brand_id = brand_comp

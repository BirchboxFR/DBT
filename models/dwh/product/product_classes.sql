WITH all_dates_table AS (
  SELECT date
  FROM (
    SELECT GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), CURRENT_DATE()) AS all_dates
  ),
  UNNEST(all_dates) date
),
real_parents AS (
  SELECT parent_product_id
  FROM (
    SELECT MAX(CASE WHEN is_parent = 1 THEN id END) AS parent_product_id, COUNT(*) AS nb
    FROM {{ ref('products') }}
    WHERE dw_country_code = 'FR'
    GROUP BY parent_post_id
  )
  WHERE nb >= 2
),
all_bundles AS (
  SELECT pbc.bundle_product_id, pbc.component_product_id
  FROM {{ ref('products_bundle_component') }} pbc
  INNER JOIN {{ ref('products') }} p ON pbc.bundle_product_id = p.id AND p.attr_is_bundle = 1 AND pbc.dw_country_code = p.dw_country_code
  WHERE pbc.dw_country_code = 'FR'
  GROUP BY bundle_product_id, component_product_id

),
raw_valid_products AS (
  SELECT p.id AS product_id
  FROM {{ ref('products') }}  p
  INNER JOIN {{ ref('brands') }}  b ON p.brand_id = b.post_id AND p.dw_country_code = b.dw_country_code
  INNER JOIN {{ ref('posts') }}  po ON p.post_id = po.id AND p.dw_country_code = po.dw_country_code
  LEFT JOIN real_parents rp ON p.id = rp.parent_product_id
  WHERE p.product_codification_id = 0
  AND p.attr_not_sold_anymore = 0
  AND b.attr_is_inactive = 0
  AND rp.parent_product_id IS NULL
  AND p.sku IS NOT NULL
  AND po.post_status = 'publish'
  AND p.special_type IS NULL
  AND p.dw_country_code = 'FR'
),
valid_products AS (
  SELECT *
  FROM (
    SELECT DISTINCT CASE WHEN ab.bundle_product_id IS NULL
                         THEN t.product_id
                         ELSE ab.component_product_id
                    END AS product_id
    FROM raw_valid_products t
    LEFT JOIN all_bundles ab ON t.product_id = ab.bundle_product_id
  )
),
valid_products_with_date AS (
  SELECT vp.product_id, adt.date
  FROM valid_products vp
  CROSS JOIN all_dates_table adt
),
days_in_stock AS (
  SELECT product_id, COUNTIF(in_stock) AS nb_days_in_stock
  FROM (
    SELECT date, product_id, MIN(in_stock) AS in_stock
    FROM (
      SELECT DATE(date) AS date, product_id, stock > 0 AS in_stock
      FROM {{ ref('products_stock_log') }} 
      WHERE dw_country_code = 'FR'
      UNION ALL
      SELECT archive_date AS date, id AS product_id, stock > 0 AS in_stock
      FROM history_table.bdd_prod_fr__wp_jb_products
    )
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 month)
    GROUP BY product_id, date
  )
  GROUP BY product_id
),
all_net_revenue_last_year AS (
  SELECT ss.product_id, SUM(ss.net_revenue) AS net_revenue
  FROM {{ ref('shop_sales') }} ss
  LEFT JOIN {{ ref('products') }} p ON ss.dw_country_code = p.dw_country_code AND ss.bundle_product_id = p.id
  WHERE ss.dw_country_code = 'FR'
  AND ss.order_status = 'Validée'
  AND DATE(ss.order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
  AND COALESCE(ss.product_discount, 0) = 0
  AND COALESCE(ss.sub_discount, 0) <= 0.16 * ss.gross_revenue
  AND (p.id IS NULL OR p.product_codification_id = 0)
  GROUP BY ss.product_id
),
all_stats_products AS (
  SELECT vp.product_id,
         CASE WHEN ds.nb_days_in_stock = 0
              THEN 0
              ELSE COALESCE(nr.net_revenue, 0) / ds.nb_days_in_stock
         END AS net_revenue_by_day,
         nb_days_in_stock
  FROM valid_products vp
  INNER JOIN days_in_stock ds USING(product_id)
  LEFT JOIN all_net_revenue_last_year nr USING(product_id)
  ORDER BY net_revenue_by_day DESC
),
total_net_revenue_one_day_table AS (
  SELECT SUM(net_revenue_by_day) AS total_net_revenue_one_day
  FROM all_stats_products
),
with_sumcum AS (
  SELECT product_id,
  net_revenue_by_day,
  SUM(net_revenue_by_day) OVER (PARTITION BY 1 ORDER BY net_revenue_by_day DESC ROWS UNBOUNDED PRECEDING) - net_revenue_by_day AS sumcum_net_revenue_by_day
FROM all_stats_products
)
SELECT ws.product_id,
       CASE WHEN sumcum_net_revenue_by_day <= 0.8 * tot.total_net_revenue_one_day THEN 'A'
            WHEN sumcum_net_revenue_by_day <= 0.95 * tot.total_net_revenue_one_day THEN 'B'
            ELSE 'C'
       END AS product_class
FROM with_sumcum ws
CROSS JOIN total_net_revenue_one_day_table tot

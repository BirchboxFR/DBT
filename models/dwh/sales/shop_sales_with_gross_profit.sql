WITH orders_with_repartition AS
(
  SELECT 
  ss.dw_country_code,
  ss.year,
  ss.month,
  ss.order_id,
  ss.sku,
  ss.quantity,
  ss.net_revenue,
  ss.product_codification,
  ss.order_total_shipping,
  COALESCE(ss.euro_purchase_price,0)*ss.quantity AS products_cost,
  CASE WHEN SUM(ss.net_revenue) OVER (PARTITION BY ss.order_id, ss.dw_country_code, ss.order_status) > 0 THEN 
  SAFE_DIVIDE(ss.net_revenue , SUM(ss.net_revenue) OVER (PARTITION BY ss.order_id, ss.dw_country_code, ss.order_status))
  ELSE SAFE_DIVIDE(ss.quantity , SUM(ss.quantity) OVER (PARTITION BY ss.order_id, ss.dw_country_code, ss.order_status))
  END
  AS net_revenue_share
  FROM sales.shop_sales as ss 
  WHERE 1 = 1
  AND ss.order_status NOT IN  ('refund', 'Annulée')
)

SELECT 
o.*,
COALESCE(order_total_shipping,0)*COALESCE(o.net_revenue_share,0) AS shipping_revenue,
o.net_revenue + COALESCE(order_total_shipping,0)*COALESCE(o.net_revenue_share,0) AS net_revenue_with_shipping,
COALESCE(som.order_picking,0)*COALESCE(o.net_revenue_share,0) AS picking_cost,
COALESCE(som.packaging_cost,0)*COALESCE(o.net_revenue_share,0) AS packaging_cost,
COALESCE(som.msg_perso,0)*COALESCE(o.net_revenue_share,0) AS msg_perso_cost,
COALESCE(som.shipping_cost,0)*COALESCE(o.net_revenue_share,0) AS shipping_cost,

o.products_cost + COALESCE(som.order_picking,0)*COALESCE(o.net_revenue_share,0) + COALESCE(som.packaging_cost,0)*COALESCE(o.net_revenue_share,0) + COALESCE(som.msg_perso,0)*COALESCE(o.net_revenue_share,0) + COALESCE(som.shipping_cost,0)*COALESCE(o.net_revenue_share,0) AS total_cost,

o.net_revenue + COALESCE(order_total_shipping,0)*COALESCE(o.net_revenue_share,0) 
- (o.products_cost + COALESCE(som.order_picking,0)*COALESCE(o.net_revenue_share,0) + COALESCE(som.packaging_cost,0)*COALESCE(o.net_revenue_share,0) + COALESCE(som.msg_perso,0)*COALESCE(o.net_revenue_share,0) + COALESCE(som.shipping_cost,0)*COALESCE(o.net_revenue_share,0)) AS gross_profit
FROM orders_with_repartition o
JOIN {{ ref('shop_orders_margin') }}  som USING (dw_country_code, order_id)


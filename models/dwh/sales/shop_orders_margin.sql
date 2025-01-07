SELECT orders.*, 
       sc.price AS shipping_transport_cost,
       orders.shipping_taxes_rate * sc.price AS total_shipping_taxes,
       (1 + orders.shipping_taxes_rate) * sc.price + 0.07 AS shipping_cost, -- ship up costs 0.07€ per order
       COALESCE(order_picking, 0) + COALESCE(packaging_cost, 0) +  COALESCE((1 + orders.shipping_taxes_rate) * sc.price, 0) + 0.07 + products_cost AS total_order_costs,
       net_revenue + shipping_revenue AS total_order_revenue,
       net_revenue + shipping_revenue - (COALESCE(order_picking, 0) + COALESCE(packaging_cost, 0) + COALESCE((1 + orders.shipping_taxes_rate) * sc.price, 0) + 0.07 + products_cost) AS gross_profit,
       CASE WHEN (net_revenue + shipping_revenue) <> 0
            THEN (net_revenue + shipping_revenue - (COALESCE(order_picking, 0) + COALESCE(packaging_cost, 0)  + COALESCE((1 + orders.shipping_taxes_rate) * sc.price, 0) + 0.07 + products_cost))
/ (net_revenue + shipping_revenue) ELSE 0
       END AS gross_margin
FROM
(
  SELECT ss.dw_country_code,
         ss.order_id, 
         MAX(ss.user_id) AS user_id,
         MAX(ss.order_date) AS order_date,
         MAX(ss.year) AS year,
         MAX(ss.month) AS month,
         MAX(ss.is_first_shop_order) AS is_first_shop_order,
         SUM(ss.gross_revenue) AS gross_revenue,
         SUM(ss.net_revenue) AS net_revenue, 
         MIN(ss.order_total_shipping) AS shipping_revenue,
         MAX(ss.order_coupon_code) AS order_coupon_code,
         MAX(ss.shipping_mode_id) AS shipping_mode_id,
         MAX(CASE WHEN ss.product_codification = 'BYOB' THEN 1
                  WHEN ss.product_codification = 'LTE' THEN 2
                  WHEN ss.product_codification = 'SPLENDIST' THEN 8
                  WHEN ss.product_codification = 'CALENDAR' THEN 13
                  ELSE 0
             END) AS codification,
          SUM(CASE WHEN pc.PC_GIFT AND ss.gift_card_type = 'PHYSICAL'
                  THEN 0.2 ELSE ss.quantity * COALESCE(c.weight, 0.2)
              END) / 1000 + 0.15 AS order_weight,
          SUM(COALESCE(ss.euro_purchase_price, 0) * ss.quantity) AS products_cost,
          MAX(CASE WHEN pc.PC_GIFT AND ss.gift_card_type <> 'PHYSICAL'
                   THEN 0 
                   WHEN pc.PC_DONATION
                   THEN 0
                   ELSE dlc_first_product.price 
              END)
              +
        (MAX(dlc_prepacked.price) - MAX(dlc_first_product.price)) * 
        (CASE WHEN 
          SUM(CASE WHEN pc.PC_GIFT
                  AND ss.gift_card_type <> 'PHYSICAL'
                  THEN 0 
                  WHEN pc.PC_DONATION
                  THEN 0
                  ELSE ss.quantity END) = 1 THEN 1 ELSE 0 END )
        *
        MAX(CASE WHEN pc.PC_SPLENDIST OR pc.PC_CALENDAR THEN 1 ELSE 0 END) -- deduction if only one calendar or splendist

        +

        (SUM(CASE 
                  WHEN pc.PC_GIFT AND ss.gift_card_type <> 'PHYSICAL'
                  THEN 0 
                  WHEN pc.PC_DONATION
                  THEN 0
                  WHEN pc.PC_BYOB
                  THEN 6
                  ELSE ss.quantity
              END)
          -
          MAX(CASE WHEN ss.gift_card_type <> 'PHYSICAL' OR pc.PC_DONATION
                    THEN 0 
                    ELSE 1 
              END)
              ) * MAX(dlc_next_product.price) AS order_picking,
          MAX(dlc_taxes.price) AS shipping_taxes_rate,
          MAX(CASE 
                  WHEN pc.PC_GIFT
                  AND ss.gift_card_type <> 'PHYSICAL'
                  THEN 0 
                  WHEN pc.PC_DONATION
                  THEN 0
                  ELSE 0.728
              END) AS packaging_cost,
    SUM(CASE 
                  WHEN pc.PC_GIFT AND ss.gift_card_type <> 'PHYSICAL'
                  THEN 0 
                  WHEN pc.PC_DONATION
                  THEN 0
                  WHEN pc.PC_BYOB
                  THEN 6
                  ELSE ss.quantity
              END)
          
               AS quantity,
    CASE WHEN o.gift_message IS NULL OR o.gift_message = '' THEN 0 ELSE 1 END AS msg_perso
  FROM {{ ref('shop_sales') }} AS ss
  LEFT JOIN {{ ref('orders') }} o ON o.id = ss.order_id AND ss.dw_country_code = o.dw_country_code
  LEFT JOIN {{ ref('catalog') }} AS c ON ss.dw_country_code = c.dw_country_code AND c.product_id = ss.product_id
  INNER JOIN {{ ref('products') }} p ON p.dw_country_code = ss.dw_country_code AND p.id = ss.product_id
  LEFT JOIN {{ ref('logistics_costs') }} dlc_taxes ON dlc_taxes.name = 'shipping_taxes_rate' AND ss.order_date >= DATE(dlc_taxes.date_start) AND (ss.order_date <= DATE(dlc_taxes.date_end) OR dlc_taxes.date_end IS NULL)
  LEFT JOIN {{ ref('logistics_costs') }} dlc_first_product ON dlc_first_product.name = 'picking first article' AND ss.order_date >= DATE(dlc_first_product.date_start) AND (ss.order_date <= DATE(dlc_first_product.date_end) OR dlc_first_product.date_end IS NULL)
  LEFT JOIN {{ ref('logistics_costs') }} dlc_next_product ON dlc_next_product.name = 'picking next article' AND ss.order_date >= DATE(dlc_next_product.date_start) AND (ss.order_date <= DATE(dlc_next_product.date_end) OR dlc_next_product.date_end IS NULL)
  LEFT JOIN {{ ref('logistics_costs') }} dlc_prepacked ON dlc_prepacked.name = 'picking prepacked' AND ss.order_date >= DATE(dlc_prepacked.date_start) AND (ss.order_date <= DATE(dlc_prepacked.date_end) OR dlc_prepacked.date_end IS NULL)
    LEFT JOIN {{ ref('logistics_costs') }} msg_perso ON msg_perso.name = 'perso_msg_print' AND ss.order_date >= DATE(msg_perso.date_start) AND (ss.order_date <= DATE(msg_perso.date_end) OR msg_perso.date_end IS NULL)
  LEFT JOIN snippets.product_codifications pc ON ss.product_codification_id = pc.product_codification_id
  WHERE ss.store_code <> 'Store'
  AND ss.order_status <> 'refund'
  AND ss.product_codification <> 'DONATION'
  GROUP BY ss.dw_country_code,
           ss.order_id,
           o.gift_message
) orders
LEFT JOIN {{ ref('shipping_costs') }} sc ON orders.order_date >= sc.date_start AND (sc.date_end IS NULL OR orders.order_date <= sc.date_end) AND orders.order_weight >= sc.min_weight AND (sc.max_weight IS NULL OR orders.order_weight < sc.max_weight) AND sc.shipping_mode_id = orders.shipping_mode_id

WITH acquisitions_cycle AS (
  SELECT 
    ss.user_key,
    ss.user_id,
    ss.dw_country_code,
    ss.order_date,
    SUM(net_revenue) AS net_revenue,
    SUM(CASE WHEN product_codification IN ('LTE','SPLENDIST','CALENDAR') THEN net_revenue ELSE 0 END) AS LTE_net_revenue,
    SUM(CASE WHEN product_codification = 'ESHOP' THEN net_revenue ELSE 0 END) AS ESHOP_net_revenue,
    sum(case when product_codification = ('GIFT') then quantity else 0 end) as gift_quantity
  FROM `teamdata-291012.sales.shop_sales` ss
  WHERE order_Status = 'Validée'
  GROUP BY ALL
),

customers_unified AS (
  SELECT *, email                      AS match_key, 'email' AS match_type
  FROM user.customers

  UNION ALL

  SELECT *, billing_phone_standardized AS match_key, 'sms'   AS match_type
  FROM user.customers
  WHERE billing_phone_standardized IS NOT NULL
),

campaign_ranked AS (
  SELECT
    c.user_key,
    u.address,
    c.user_id,
    c.dw_country_code,
    u.custom_country,
    u.channel,
    campaign.campaign_id,
    campaign.campaign_name,
    campaign.startdate,
    campaign.imo_variant,
    campaign.custom_Categorie_de_campagne,
    campaign.opened,
    campaign.delivered,
    a.order_date,
    a.user_key IS NOT NULL AS acquis,
    SUM(a.net_revenue)      AS net_revenue,
    SUM(a.LTE_net_revenue)  AS LTE_net_revenue,
    SUM(a.ESHOP_net_revenue) AS ESHOP_net_revenue,
    sum(a.gift_quantity) as gift_quantity,
    ROW_NUMBER() OVER (
      PARTITION BY c.user_key, a.order_date
      ORDER BY campaign.startdate DESC
    ) AS rn
  FROM `normalised-417010.crm.crm_data_detailed_by_user` u,
  UNNEST(campaigns) AS campaign
  INNER JOIN customers_unified c
    ON  c.match_key  = u.address
    AND c.match_type = u.channel
  LEFT JOIN acquisitions_cycle a
    ON  a.user_key        = c.user_key
    AND a.dw_country_code = u.custom_country
    AND a.dw_country_code = c.dw_country_code
    AND campaign.startdate BETWEEN DATE_SUB(DATE(a.order_date), INTERVAL 2 DAY) AND DATE(a.order_date)
  WHERE (
      upper(campaign.campaign_name) LIKE '%SHOP%'
    OR upper(campaign.campaign_name) LIKE '%LTE%'
    OR upper(campaign.custom_Categorie_de_Campagne_Lvl_2) LIKE '%MIXTE%'
    OR upper(campaign.campaign_id)   LIKE '%SHOP%'
    OR upper(campaign.campaign_id)   LIKE '%LTE%'
  )
    AND (
      (u.channel = 'email' AND campaign.opened    = true)
      OR (u.channel = 'sms' AND campaign.delivered = true)
    )
    AND a.user_key IS NOT NULL
  GROUP BY ALL
)

SELECT
  user_key,
  address,
  user_id,
  dw_country_code,
  custom_country,
  channel,
  campaign_id,
  campaign_name,
  imo_variant,
  custom_Categorie_de_campagne,
  startdate,
  opened,
  delivered,
  order_date,
  acquis,
  net_revenue,
  LTE_net_revenue,
  ESHOP_net_revenue,
  gift_quantity
FROM campaign_ranked
WHERE rn = 1
ORDER BY order_date
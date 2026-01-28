WITH acquisitions_cycle AS (
  SELECT 
    ss.user_key,
    ss.user_id,
    ss.dw_country_code,
    ss.order_date,
   sum(net_revenue) AS net_revenue,
  FROM `teamdata-291012.sales.shop_sales` ss
  WHERE 1=1
    and order_Status='Validée'
    group by all
),

campaign_ranked AS (
  SELECT DISTINCT   
    c.user_key,
    u.address,
    c.user_id,
    c.dw_country_code,
    campaign.campaign_id,
     campaign.campaign_name,
    campaign.startdate,
    imo_variant,
    custom_Categorie_de_campagne,
    sum(a.net_revenue) AS net_revenue,
    campaign.opened,
    a.order_date,
    a.user_key IS NOT NULL as acquis,
    ROW_NUMBER() OVER (
      PARTITION BY c.user_key, a.order_date 
      ORDER BY campaign.startdate DESC
    ) as rn
  FROM `normalised-417010.crm.crm_data_detailed_by_user` u,
  UNNEST(campaigns) as campaign
  INNER JOIN user.customers c ON c.email = u.address
  LEFT JOIN acquisitions_cycle a ON a.user_key = c.user_key  AND campaign.startdate BETWEEN DATE_SUB(DATE(a.order_date), INTERVAL 2 DAY) AND DATE(a.order_date)  and u.custom_country = c.dw_country_code and u.custom_country = a.dw_country_code
  WHERE (upper(campaign.campaign_name) LIKE 'SHOP%' or upper(campaign.campaign_id) LIKE 'SHOP%' )
    AND campaign.opened = true 
    AND a.user_key IS NOT NULL 
    group by all
)

SELECT 
  user_key,
  address,
  user_id,
  dw_country_code,
  campaign_id,
  campaign_name,
  imo_variant,
  custom_Categorie_de_campagne
  startdate,
  opened,
  order_date,
  acquis,
  net_revenue
FROM campaign_ranked
WHERE rn = 1  -- Une seule ligne par user_key + campaign_date (mois)
  
ORDER BY order_date
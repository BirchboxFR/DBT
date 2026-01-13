WITH acquisitions_cycle AS (
  SELECT 
    bs.user_key,
    bs.user_id,
    bs.dw_country_code,
    bs.acquis_status_lvl2,
    bs.payment_date,
    bs.day_in_cycle,
    bs.date as campaign_date
  FROM `teamdata-291012.sales.box_sales` bs
  WHERE bs.dw_country_code = 'FR'
    AND bs.acquis_status_lvl1 = 'ACQUISITION'
    AND bs.acquis_status_lvl2 IN ('REACTIVATION', 'NEW NEW')
    AND bs.payment_status = 'paid'
    AND bs.day_in_cycle > 0
    AND bs.gift = 0
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
    campaign.custom_Categorie_de_campagne,
    campaign.opened,
    a.campaign_date,
    a.user_key IS NOT NULL as acquis,
    ROW_NUMBER() OVER (
      PARTITION BY c.user_key, a.campaign_date 
      ORDER BY campaign.startdate DESC
    ) as rn
  FROM `normalised-417010.crm.crm_data_detailed_by_user` u,
  UNNEST(campaigns) as campaign
  INNER JOIN user.customers c ON c.email = u.address
  LEFT JOIN acquisitions_cycle a 
    ON a.user_key = c.user_key 
    AND campaign.startdate BETWEEN DATE_SUB(DATE(a.payment_date), INTERVAL 2 DAY) AND DATE(a.payment_date)
  WHERE campaign.custom_Categorie_de_campagne in ('BOX_Disclose','BOX_GWS','BOX_Promo','BOX_Relance_ouverture','WELCOME_PACK_SANS_ACHAT','WELCOME_PACK_ACHAT_SHOP')
    AND campaign.opened = true 
    AND a.user_key IS NOT NULL 
)

SELECT 
  user_key,
  address,
  user_id,
  dw_country_code,
  campaign_id,
  campaign_name,
  imo_variant,
  custom_Categorie_de_campagne,
  startdate,
  opened,
  campaign_date,
  acquis
FROM campaign_ranked
WHERE rn = 1  -- Une seule ligne par user_key + campaign_date (mois)
  
ORDER BY campaign_date
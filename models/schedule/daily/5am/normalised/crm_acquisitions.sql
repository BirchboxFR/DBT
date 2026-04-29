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
  WHERE 1=1
    AND bs.acquis_status_lvl1 = 'ACQUISITION'
    AND bs.acquis_status_lvl2 IN ('REACTIVATION', 'NEW NEW')
    AND bs.payment_status = 'paid'
    AND bs.day_in_cycle > 0
    AND bs.gift = 0
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
  SELECT DISTINCT
    c.user_key,
    u.address,
    c.user_id,
    c.dw_country_code,
    u.custom_country,
    campaign.campaign_id,
    campaign.campaign_name,
    campaign.startdate,
    campaign.imo_variant,
    campaign.custom_Categorie_de_campagne,
    campaign.opened,
    a.campaign_date,
    a.user_key IS NOT NULL AS acquis,
    ROW_NUMBER() OVER (
      PARTITION BY c.user_key, a.campaign_date 
      ORDER BY campaign.startdate DESC
    ) AS rn
  FROM `normalised-417010.crm.crm_data_detailed_by_user` u,
  UNNEST(campaigns) AS campaign
  INNER JOIN customers_unified c
    ON  c.match_key  = u.address
    AND c.match_type = u.channel
  LEFT JOIN acquisitions_cycle a
    ON  a.user_key       = c.user_key
    AND a.dw_country_code = u.custom_country
    AND campaign.startdate BETWEEN DATE_SUB(DATE(a.payment_date), INTERVAL 2 DAY) AND DATE(a.payment_date)
  WHERE (
      upper(campaign.campaign_name) LIKE 'ACQUISITION_BOX%'
    OR upper(campaign.campaign_id)  LIKE 'ACQUISITION_BOX%'
    OR campaign.custom_Categorie_de_campagne IN (
        'BOX_Promo','BOX_Disclose','BOX_Ouverture','WELCOME_PACK',
        'WELCOME_PACK_ACHAT_SHOP','BOX_GWS','BOX_Relance_ouverture','WELCOME_PACK_SANS_ACHAT'
      )
    OR campaign.custom_Categorie_de_Campagne_Lvl_2 IN ('MIXTE_LTE_BOX','MIXTE_BOX_SHOP')
  )
    AND (
      (u.channel = 'email' AND campaign.opened    = true)
      OR (u.channel = 'sms' AND campaign.delivered = true)
    )
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
  custom_Categorie_de_campagne
  startdate,
  opened,
  campaign_date,
  acquis
FROM campaign_ranked
WHERE rn = 1  -- Une seule ligne par user_key + campaign_date (mois)
  
ORDER BY campaign_date
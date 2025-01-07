WITH  
awin_transactions AS (
  SELECT 
    DATETIME_ADD(cr.transactiondate, INTERVAL 2 HOUR) as transaction_date, 
    REGEXP_EXTRACT(ac.accountname, r'([A-Z]{2})') as country_code,
    REGEXP_EXTRACT(tr.orderref, r'Order(\d+)') as order_id,
    cr.transactionpartscommissiongroupname,
    cr.transactionpartscommissiongroupcode,
    CASE 
      WHEN transactionpartscommissiongroupcode = "CALENDRIER" THEN "EXCLUSIVE"
      WHEN transactionpartscommissiongroupcode = "DEFAULT" THEN "BOX"
      WHEN transactionpartscommissiongroupcode = "BOX" THEN "BOX"
      WHEN transactionpartscommissiongroupcode = "SHOP" THEN "SHOP"
      WHEN transactionpartscommissiongroupcode = "EXCLUSIVE" THEN "EXCLUSIVE"
      WHEN transactionpartscommissiongroupcode IS NULL THEN "BOX" -- for special operations not in clickref
      ELSE "BOX" -- default is Box
    END as group_code,
    CASE
      WHEN REGEXP_EXTRACT(ac.accountname, r'([A-Z]{2})') = "FR" THEN 1.18
      WHEN REGEXP_EXTRACT(ac.accountname, r'([A-Z]{2})') = "DE" THEN 1.30
      ELSE 1.18
    END as markup,
    cr.transactionpartscommissionamount,
    cr.transactionpartsamount,
    tr.commissionamount,
    CASE 
      WHEN cr.transactionpartscommissiongroupname IS NULL THEN tr.commissionamount
      ELSE cr.transactionpartscommissionamount
    END as commission
  FROM `teamdata-291012.pipe.awin_combined_transactions` tr 
  LEFT JOIN `teamdata-291012.pipe.awin_combined_clickrefs` cr ON cr.id = tr.id
  JOIN `teamdata-291012.pipe.awin_accounts` as ac ON ac.accountid = tr.advertiserid
  ORDER BY cr.transactiondate DESC
),
cd  AS
(
SELECT cd.country, cd.campaign_name, MAX(cd.levier) AS levier, case when MAX(cd.cat1)  ='Réseaux Sociaux' then 'RESEAUX SOCIAUX' else upper(MAX(cd.cat1)) end AS cat1, MAX(cd.cat2) AS cat2, MAX(cd.cat3) AS cat3 
  FROM `marketing.campaign_details` cd where campaign_name not like '%_old%'
  GROUP BY cd.country, cd.campaign_name 
)


-- META marketing OPEX

SELECT DATE(dma.date_start) as d, dma.campaign_name AS campaign_name,'FR' AS country,
SUM(dma.impressions) AS impression,
SUM(dma.clicks) AS clic,
SUM(dma.spend) AS spent,
COALESCE(cd.levier,'FACEBOOK / INSTAGRAM') AS levier,
COALESCE(cd.cat1,'RESEAUX SOCIAUX') AS cat1,
COALESCE(cd.cat2, 
  CASE WHEN dma.campaign_name LIKE '%BOX%' THEN 'BOX'
       WHEN dma.campaign_name LIKE '%BRANDING%' THEN 'BRANDING'
       WHEN dma.campaign_name LIKE '%SHOP%' THEN 'SHOP' ELSE NULL END) AS cat2,
COALESCE(cd.cat3, 
CASE WHEN dma.campaign_name LIKE '%RTG%' THEN 'RTG'
     WHEN dma.campaign_name LIKE '%BOX%' THEN 'ACQUIS'
     WHEN dma.campaign_name LIKE '%BRANDING%' THEN 'BRANDING'
     WHEN dma.campaign_name LIKE '%SHOP%' THEN 'ACQUIS' ELSE NULL END) AS cat3
FROM  `teamdata-291012.fb_fr.ads_insights` dma 
LEFT JOIN cd ON cd.campaign_name = dma.campaign_name AND cd.country = 'FR'
GROUP BY d, dma.campaign_name,cd.levier,cd.cat1,cd.cat2,cd.cat3
HAVING impression > 0 OR clic > 0 OR spent > 0

UNION ALL

SELECT DATE(dma.date_start) as d, dma.campaign_name,'ES',
SUM(dma.impressions) AS impression,
SUM(dma.clicks) AS clic,
SUM(dma.spend) AS spent,
COALESCE(cd.levier,'FACEBOOK / INSTAGRAM') AS levier,
COALESCE(cd.cat1,'RESEAUX SOCIAUX') AS cat1,
COALESCE(cd.cat2, 
  CASE WHEN dma.campaign_name LIKE '%BOX%' THEN 'BOX'
       WHEN dma.campaign_name LIKE '%BRANDING%' THEN 'BRANDING'
       WHEN dma.campaign_name LIKE '%SHOP%' THEN 'SHOP' ELSE 'BOX' END) AS cat2,
COALESCE(cd.cat3, 
CASE WHEN dma.campaign_name LIKE '%RTG%' THEN 'RTG'
     WHEN dma.campaign_name LIKE '%BOX%' THEN 'ACQUIS'
     WHEN dma.campaign_name LIKE '%BRANDING%' THEN 'BRANDING'
     WHEN dma.campaign_name LIKE '%SHOP%' THEN 'ACQUIS' ELSE 'ACQUIS' END) AS cat3
FROM  `teamdata-291012.fb_es.ads_insights` dma 
inner JOIN  cd ON cd.campaign_name = dma.campaign_name AND cd.country = 'ES'
GROUP BY d, dma.campaign_name,cd.levier,cd.cat1,cd.cat2,cd.cat3
HAVING impression > 0 OR clic > 0 OR spent > 0

UNION ALL

SELECT DATE(dma.date_start) as d, dma.campaign_name,'DE',
SUM(dma.impressions) AS impression,
SUM(dma.clicks) AS clic,
SUM(dma.spend) AS spent,
COALESCE(cd.levier,'FACEBOOK / INSTAGRAM') AS levier,
COALESCE(cd.cat1,'RESEAUX SOCIAUX') AS cat1,
COALESCE(cd.cat2, 
  CASE WHEN dma.campaign_name LIKE '%BOX%' THEN 'BOX'
       WHEN dma.campaign_name LIKE '%BRANDING%' THEN 'BRANDING'
       WHEN dma.campaign_name LIKE '%SHOP%' THEN 'SHOP' ELSE 'BOX' END) AS cat2,
COALESCE(cd.cat3, 
CASE WHEN dma.campaign_name LIKE '%RTG%' THEN 'RTG'
     WHEN dma.campaign_name LIKE '%BOX%' THEN 'ACQUIS'
     WHEN dma.campaign_name LIKE '%BRANDING%' THEN 'BRANDING'
     WHEN dma.campaign_name LIKE '%SHOP%' THEN 'ACQUIS' ELSE 'ACQUIS' END) AS cat3
FROM  `teamdata-291012.fb_de_eu.ads_insights` dma 
inner JOIN  cd ON cd.campaign_name = dma.campaign_name AND cd.country = 'DE'
GROUP BY d, dma.campaign_name,cd.levier,cd.cat1,cd.cat2,cd.cat3
HAVING impression > 0 OR clic > 0 OR spent > 0

UNION ALL

-- Marketing OPEX - Awin 
SELECT 
  
   DATE(transaction_date) AS d,
  concat('AWIN ', group_code) AS campaign_name,
  country_code,
  NULL AS impression,
  NULL AS clic,
  ROUND(SUM(commission * markup),4) as spent,
  concat('AWIN ', group_code) AS levier,
  'AFFILIATION' as cat1,
  group_code  as cat2,
  'ACQUIS' as cat3 
FROM awin_transactions
WHERE commission * markup > 0
GROUP BY d, country_code, levier, cat1, cat2, cat3

UNION ALL


-- google ads expenses
SELECT
cs.segments_date as d,
c.Campaign_Name AS campaign_name,
  COALESCE(REGEXP_EXTRACT(cu.customer_descriptive_name, r'Blissim (\w+)'),cu.customer_descriptive_name) AS country,
  SUM(cs.metrics_impressions) AS impressions,
  SUM(cs.metrics_clicks) AS clics,
  (SUM(cs.metrics_cost_micros) / 1000000) AS spent,
  'Google',
  cd.cat1,
  cd.cat2,
  cd.cat3
  
FROM (
  SELECT customer_id, campaign_id,
  case when campaign_id in (97499006,20677581094,21088391199,17854460874) then min( campaign_name) 
  else max( campaign_name) end as campaign_name 
  FROM googleads.p_ads_Campaign_3790578098
  GROUP BY campaign_id, customer_id
    ) c
JOIN (
  SELECT cu.customer_id, cu.customer_descriptive_name 
  FROM `googleads.p_ads_Customer_3790578098` cu 
  GROUP BY cu.customer_id, cu.customer_descriptive_name
  ) cu ON cu.customer_id = c.customer_id
LEFT JOIN `googleads.p_ads_CampaignBasicStats_3790578098` cs ON c.Campaign_id = cs.Campaign_id
INNER JOIN  cd ON cd.campaign_name = c.campaign_name
WHERE cs.metrics_cost_micros > 0
AND cu.customer_descriptive_name LIKE '%Blissim%'
GROUP BY cs.segments_date,c.Campaign_Name, country, cd.cat1, cd.cat2, cd.cat3, cs.campaign_id

UNION ALL

-- TikTok Live Expenses
SELECT DATE(ad.stat_time_day) AS d, ad.campaign_name, LEFT(TRIM(ad.campaign_name), 2) AS country, SUM(ad.impressions) AS impressions, SUM(ad.clicks) AS clicks, SUM(ad.spend) AS spent,
COALESCE(cd.levier,'TIKTOK') AS levier, COALESCE(cd.cat1,  'RESEAUX SOCIAUX') AS cat1, COALESCE(cd.cat2,'BOX') AS cat2, COALESCE(cd.cat3,'ACQUIS') AS cat3
FROM `tik_tok.ad_insights` ad
LEFT JOIN cd ON cd.country = LEFT(TRIM(ad.campaign_name), 2) AND cd.campaign_name = ad.campaign_name
GROUP BY ad.stat_time_day, DATE(ad.stat_time_day) , ad.campaign_name, cd.levier, cd.cat1, cd.cat2, cd.cat3



UNION ALL

-- Snapchat Live Expenses

SELECT DATE(asd.start_time) AS d ,'FR - SNAP - BOX - RETARGETING'AS campaign_name,case when id='2b45026e-25fe-4948-b594-db0bb563b04e' then 'FR' when id='d3f9a490-7a93-4aa8-814c-8b3ad8f81898' then 'DE' end AS country,0 AS impressions,0  AS clicks ,
SUM(asd.spend/1000000) AS spent,
'SNAPCHAT' AS levier, 'RESEAUX SOCIAUX' AS cat1, 'BOX' AS cat2, 'ACQUIS'AS cat3
FROM `teamdata-291012.snapchat.ad_account_stats_daily` asd
group by 1,2,3,4,5

  /*SELECT DATE(csd.start_time) AS d, c.name AS campaign_name, LEFT(TRIM(c.name), 2) AS country, SUM(csd.impressions) AS impressions, SUM(csd.conversion_ad_click) AS clicks,   SUM(csd.spend/1000000) AS spent, 
COALESCE(cd.levier,'SNAPCHAT') AS levier, COALESCE(cd.cat1,  'RESEAUX SOCIAUX') AS cat1, COALESCE(cd.cat2,'BOX') AS cat2, COALESCE(cd.cat3,'ACQUIS') AS cat3
FROM `snapchat.campaign_stats_daily` csd
JOIN `snapchat.campaigns` c ON c.id = csd.id
LEFT JOIN cd ON cd.country = LEFT(TRIM(c.name), 2) AND cd.campaign_name = c.name
WHERE csd.spend > 0
GROUP BY c.name, d, c.name, cd.levier, cd.cat1, cd.cat2, cd.cat3*/

  

ORDER BY d 


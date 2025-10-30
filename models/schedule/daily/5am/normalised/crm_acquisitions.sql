  {{ config(
    materialized = 'table',
    partition_by = {
      "field": "startdate",
      "data_type": "date",
      "granularity": "month"  
    },
    cluster_by = ["address"], 
    on_schema_change = 'sync',
    description = "Table des acquisitions box corrélées aux ouvertures de campagnes CRM."
) }}
  
  
  WITH acquisitions_cycle AS (
  SELECT 
    bs.user_key,
    bs.user_id,
    bs.dw_country_code,
    bs.acquis_status_lvl2,
    bs.payment_date,
    bs.day_in_cycle
  FROM `teamdata-291012.sales.box_sales` bs
  WHERE bs.dw_country_code = 'FR'
    -- AND FORMAT_DATE('%Y-%m', bs.date) = '2025-09'
    AND bs.acquis_status_lvl1 = 'ACQUISITION'
    AND bs.acquis_status_lvl2 IN ('REACTIVATION', 'NEW NEW')
    AND bs.payment_status = 'paid'
    AND bs.day_in_cycle > 0
    AND bs.gift = 0
)
SELECT 
    c.user_key,address,
    campaign.campaign_id,
    campaign.startdate,
    campaign.opened,
    a.user_key is not null as acquis
  FROM `normalised-417010.crm.crm_data_detailed_by_user` u,
  UNNEST(campaigns) as campaign
  inner join user.customers c on c.email = u.address

  left join acquisitions_cycle a ON a.user_key = c.user_key AND campaign.startdate BETWEEN DATE_SUB(DATE(a.payment_date), INTERVAL 2 DAY) AND DATE(a.payment_date)
  WHERE  campaign.campaign_id LIKE 'ACQUISITION_BOX%'
    AND campaign.opened = true
    aND a.user_key is not null

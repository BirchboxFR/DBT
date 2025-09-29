WITH campaign_message_stats AS (
 SELECT 
   c.*,
   count(distinct m.address) as targeted,
   count(distinct case when m.status='delivered' then m.address end) as delivered,
   count(distinct case when m.status='softBounce' then m.address end) as softBounce,
   count(distinct case when m.status='hardBounce' then m.address end) as hardBounce
 FROM cdpimagino.imaginoreplicatedcampaign c  
 JOIN cdpimagino.imaginoreplicatedmessage m ON m.activationId = c.id
 WHERE DATE(m.eventDate) >= '2024-01-01'  -- Filtre partition
 GROUP BY ALL
),

campaign_tracking_stats AS (
 SELECT 
   c.id as campaign_id,
   count(distinct case when t.type='open' then t.address end) as open_uniques,
   count(distinct case when t.type='click' then t.address end) as click_uniques,
   count( case when t.type='click' then t.address end) as clicks,
   count(case when url like '%unsubscribe%' then 1 end ) as unsubscribes
 FROM cdpimagino.imaginoreplicatedcampaign c
 LEFT JOIN cdpimagino.imaginoreplicatedtracking t ON t.activationid = c.id
 WHERE DATE(t.eventDate) >= '2024-01-01'  -- Filtre partition
 GROUP BY c.id
)

SELECT 
    'IMAGINO' as source,
    cms.* EXCEPT(startdate),
    coalesce(date(startdate),date(created)) as startdate,
    COALESCE(cts.open_uniques, 0) as open_uniques,
    COALESCE(cts.click_uniques, 0) as click_uniques,
    COALESCE(cts.clicks, 0) as clicks,
    COALESCE(cts.unsubscribes, 0) as unsubscribes
    
FROM campaign_message_stats cms
LEFT JOIN campaign_tracking_stats cts ON cts.campaign_id = cms.id


SELECT channel,-- string_agg( distinct Traffic_source ) traffic_source,Campaign_type_,
channel_type,
Traffic_source,
product_type,
b.date,
cac_data ='1' as is_cac_eligible,
case when market='France' then 'FR' when market='Spain' then 'ES' when market ='Germany' then 'DE' WHEN market = 'Poland' THEN 'PL' WHEN market = 'Sweden' THEN 'SE' end as dw_country_code,cac_data,
EXTRACT(YEAR FROM b.date) year,
EXTRACT(month FROM b.date) month,
market,sum(cost) as spent ,sum(impressions) as impressions
FROM funnel.funnel_data d
inner join inter.boxes b on d.date>=b.shipping_date  and case when market='France' then 'FR' when market='Spain' then 'ES' when market ='Germany' then 'DE' WHEN market = 'Poland' THEN 'PL' WHEN market = 'Sweden' THEN 'SE' end=b.dw_country_code  AND d.date <= b.closing_Date
WHERE  product_type IN ('Box', 'Gift') 

  group by all

--  having market='France' and month = 12 and year=2025
ORDER BY spent DESC
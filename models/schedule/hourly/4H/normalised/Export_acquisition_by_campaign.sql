

SELECT
  campaign_name,
  campaign_date,
  startdate,
  SUM(CASE WHEN acquis = TRUE THEN 1 ELSE 0 END) AS total_acquis
FROM `normalised-417010.crm.crm_acquisitions`
GROUP BY all
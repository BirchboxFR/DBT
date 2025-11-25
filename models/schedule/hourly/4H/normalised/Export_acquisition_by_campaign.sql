

SELECT
  campaign_name,
  SUM(CASE WHEN acquis = TRUE THEN 1 ELSE 0 END) AS total_acquis
FROM `normalised-417010.crm.crm_acquisitions`
GROUP BY campaign_name
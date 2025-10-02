{{
  config(
    materialized='table',
    partition_by={
      "field": "first_date_campaign",
      "data_type": "date"
    }
  )
}}





-- LTV moyenne par mois d’ouverture (calendrier FR) en utilisant first_date_campaign comme date d’observation
WITH ltv AS (
  SELECT
    Session_campaign___GA4__Google_Analytics AS campaign,
    first_date_campaign,
    -- conversion robuste vers DATE si la colonne est STRING/TIMESTAMP
    COALESCE(
      SAFE_CAST(first_date_campaign AS DATE),
      DATE(SAFE_CAST(first_date_campaign AS TIMESTAMP))
    ) AS observation_date,
    predicted_ltv
  FROM `normalised-417010.marketing.Export_predictive_ltv_per_campaign`
  -- WHERE  Session_campaign___GA4__Google_Analytics = 'ACQUISITION BOX summer Choose ton sac CHURNEVER'
),

openings AS (
  SELECT
    DATE(shipping_Date) AS opening_date,
    LEAD(DATE(shipping_Date)) OVER (ORDER BY DATE(shipping_Date)) AS next_opening_date
  FROM `teamdata-291012.inter.boxes`
  WHERE dw_country_code = 'FR'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE(shipping_Date)) = 1  -- déduplique les dates
),

binned AS (
  SELECT
    l.campaign,
    l.first_date_campaign,
    l.predicted_ltv,
    o.opening_date
  FROM ltv l
  JOIN openings o
    ON l.observation_date >= o.opening_date
   AND (o.next_opening_date IS NULL OR l.observation_date < o.next_opening_date)
)

SELECT
  campaign,
  DATE_TRUNC(opening_date, MONTH) AS month,     -- agrégation au mois d’ouverture
  MIN(first_date_campaign)        AS first_date_campaign,  -- info de référence
  AVG(predicted_ltv)              AS average_ltv                   
FROM binned
group by all


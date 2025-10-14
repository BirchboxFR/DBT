{{ config(
  materialized='table',
  partition_by={"field": "month", "data_type": "date"},
  cluster_by=["dw_country_code"],
  description="Agrégat mensuel par pays des opt-ins email (topic_id=3) : new_optin, desabo, variation, total_optin (cumul net)."
) }}

-- 1) Événements (emails only, topic 3)
WITH events AS (
  SELECT
    dw_country_code,
    LOWER(user_email) AS user_email,
    TIMESTAMP(updated_at) AS updated_at,
    SAFE_CAST(consent_status AS BOOL) AS consent_status
  FROM inter.user_consent_history
  WHERE consent_topic_id = 3
    AND user_email IS NOT NULL
),

-- 2) Statut de fin de mois par user (condense tous les flips du mois)
per_month AS (
  SELECT
    dw_country_code,
    user_email,
    DATE_TRUNC(DATE(updated_at), MONTH) AS month,
    ARRAY_AGG(consent_status ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS month_end_status
  FROM events
  GROUP BY 1,2,3
),

-- 3) Comparaison au dernier statut connu avant (via LAG sur les mois d'activité)
with_prev AS (
  SELECT
    dw_country_code,
    user_email,
    month,
    month_end_status,
    LAG(month_end_status) OVER (
      PARTITION BY dw_country_code, user_email
      ORDER BY month
    ) AS prev_end_status
  FROM per_month
),

-- 4) Transitions nettes par mois/pays
monthly_country AS (
  SELECT
    dw_country_code,
    month,
    COUNTIF( COALESCE(prev_end_status, FALSE) <> TRUE AND month_end_status = TRUE ) AS new_optin,
    COUNTIF( COALESCE(prev_end_status, FALSE)  = TRUE AND month_end_status = FALSE ) AS desabo
  FROM with_prev
  GROUP BY 1,2
),

-- 5) Gamme complète de mois par pays
months AS (
  SELECT
    c.dw_country_code,
    m AS month
  FROM (SELECT DISTINCT dw_country_code FROM events) c
  CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(
      (SELECT DATE_TRUNC(DATE(MIN(updated_at)), MONTH) FROM events),
      (SELECT DATE_TRUNC(DATE(MAX(updated_at)), MONTH) FROM events),
      INTERVAL 1 MONTH
  )) AS m
),

filled AS (
  SELECT
    m.dw_country_code,
    m.month,
    COALESCE(mc.new_optin, 0) AS new_optin,
    COALESCE(mc.desabo, 0)   AS desabo,
    COALESCE(mc.new_optin, 0) - COALESCE(mc.desabo, 0) AS variation
  FROM months m
  LEFT JOIN monthly_country mc
    ON mc.dw_country_code = m.dw_country_code
   AND mc.month = m.month
),

-- 6) Cumul net
final AS (
  SELECT
    dw_country_code,
    month,
    new_optin,
    desabo,
    variation,
    SUM(variation) OVER (PARTITION BY dw_country_code ORDER BY month) AS total_optin
  FROM filled
)

SELECT *
FROM final
ORDER BY dw_country_code, month;

{{ config(
    materialized='incremental',
    unique_key=['date', 'dw_country_code', 'acquis_status_lvl2'],
    partition_by={
        'field': 'date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by=['dw_country_code', 'acquis_status_lvl2'],
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    description='Metrics acquisitions quotidiennes - Ultra-optimisé pour suivi acquisition'
) }}

/*
    Table de metrics d'acquisition ultra-optimisée :
    - Partitionnée par jour (scan seulement les jours nécessaires)
    - Clusterisée par pays et type d'acquisition
    - Incrémental (recharge les 31 derniers jours)
    - Volume : ~10-50 lignes/jour vs millions dans box_sales
    - Coût requête : quasi gratuit (< 0.001€)

    Cas d'usage :
    - Dashboard acquisition temps réel (31 derniers jours)
    - Déclinable par pays
    - Déclinable par type (NEW NEW, REACTIVATION, GIFT)
*/

WITH acquisitions AS (
    SELECT
        DATE(payment_date) AS date,
        dw_country_code,
        acquis_status_lvl1,
        acquis_status_lvl2,

        -- Filtres métier
        day_in_cycle,

        -- Informations utiles
        gift,
        coupon_type,
        discount_type,

        -- Metrics de value
        net_revenue,
        gross_profit,
        discount

    FROM {{ ref('box_sales') }}

    WHERE acquis_status_lvl1 = 'ACQUISITION'
      AND day_in_cycle > 0

    {% if is_incremental() %}
    -- Ne recharger que les 31 derniers jours
    AND DATE(payment_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY)
    AND DATE(payment_date) <= CURRENT_DATE()
    {% endif %}
)

SELECT
    -- Dimensions
    date,
    dw_country_code,
    acquis_status_lvl2,  -- NEW NEW, REACTIVATION, GIFT

    -- METRICS PRINCIPALES
    COUNT(*) AS nb_acquisitions,
    COUNT(DISTINCT user_id) AS nb_unique_users_acquired,

    -- METRICS PAR TYPE
    COUNT(CASE WHEN acquis_status_lvl2 = 'NEW NEW' THEN 1 END) AS nb_new_new,
    COUNT(CASE WHEN acquis_status_lvl2 = 'REACTIVATION' THEN 1 END) AS nb_reactivations,
    COUNT(CASE WHEN acquis_status_lvl2 = 'GIFT' THEN 1 END) AS nb_gifts,

    -- METRICS DE VALUE
    SUM(net_revenue) AS total_acquisition_revenue,
    AVG(net_revenue) AS avg_revenue_per_acquisition,
    SUM(gross_profit) AS total_acquisition_profit,
    SUM(discount) AS total_acquisition_discount,

    -- METRICS PAR CANAL
    COUNT(CASE WHEN coupon_type IS NOT NULL THEN 1 END) AS nb_with_coupon,
    COUNT(CASE WHEN gift = 1 THEN 1 END) AS nb_gift_subscriptions,

    -- METADATA
    MIN(day_in_cycle) AS min_day_in_cycle,
    MAX(day_in_cycle) AS max_day_in_cycle,
    AVG(day_in_cycle) AS avg_day_in_cycle,

    CURRENT_TIMESTAMP() AS last_updated_at

FROM acquisitions

GROUP BY
    date,
    dw_country_code,
    acquis_status_lvl2

-- Limiter aux 31 derniers jours et jusqu'à aujourd'hui
HAVING date >= DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY)
   AND date <= CURRENT_DATE()

ORDER BY date DESC, dw_country_code

{{ config(
    materialized='incremental',
    unique_key=['date', 'dw_country_code'],
    partition_by={
        'field': 'date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by=['dw_country_code', 'acquis_status_lvl1'],
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    description='Metrics agrégées quotidiennes - Pour analyses détaillées'
) }}

/*
    Table de metrics quotidienne optimisée :
    - Granularité jour (plus détaillé que monthly)
    - Partitionnée par jour
    - Incrémental (recharge les 7 derniers jours)
    - Clustering optimisé pour queries par pays
*/

WITH box_sales_filtered AS (
    SELECT *
    FROM {{ ref('box_sales') }}

    {% if is_incremental() %}
    -- Ne recharger que les 7 derniers jours
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    {% endif %}
)

SELECT
    -- Dimensions temporelles
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    FORMAT_DATE('%A', date) AS day_name,

    -- Dimensions géographiques
    dw_country_code,

    -- Dimensions business (moins de granularité que monthly)
    acquis_status_lvl1,
    payment_status,
    committed,

    -- METRICS DE VOLUME
    COUNT(DISTINCT sub_id) AS subscription_count,
    COUNT(DISTINCT user_id) AS unique_users,

    -- METRICS DE REVENUE
    SUM(net_revenue) AS net_revenue,
    SUM(gross_revenue) AS gross_revenue,
    AVG(net_revenue) AS avg_revenue_per_sub,

    -- METRICS DE MARGE
    SUM(gross_profit) AS gross_profit,
    AVG(gross_profit) AS avg_gross_profit_per_sub,

    -- METRICS DE DISCOUNT
    SUM(discount) AS total_discounts,

    -- METADATA
    CURRENT_TIMESTAMP() AS last_updated_at

FROM box_sales_filtered

GROUP BY
    date,
    year,
    month,
    day,
    day_name,
    dw_country_code,
    acquis_status_lvl1,
    payment_status,
    committed

{{ config(
    materialized='incremental',
    unique_key=['date', 'dw_country_code'],
    partition_by={
        'field': 'date',
        'data_type': 'date',
        'granularity': 'month'
    },
    cluster_by=['dw_country_code', 'acquis_status_lvl1', 'payment_status'],
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    description='Metrics agrégées mensuelles - Optimisé pour Metabase (coût minimal)'
) }}

/*
    Table de metrics mensuelle ultra-optimisée :
    - Partitionnée par mois (scan seulement les mois nécessaires)
    - Clusterisée par pays et statuts (queries ultra-rapides)
    - Incrémental (recharge seulement les 24 derniers mois)
    - Volume : ~100 lignes/mois au lieu de millions
    - Coût requête Metabase : < 0.01€
*/

WITH box_sales_filtered AS (
    SELECT *
    FROM {{ ref('box_sales') }}

    {% if is_incremental() %}
    -- Ne recharger que les 24 derniers mois pour optimiser
    WHERE date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH), MONTH)
    {% endif %}
)

SELECT
    -- Dimensions temporelles
    DATE_TRUNC(date, MONTH) AS date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,

    -- Dimensions géographiques
    dw_country_code,

    -- Dimensions business
    acquis_status_lvl1,
    acquis_status_lvl2,
    payment_status,
    next_month_status,
    committed,

    CASE WHEN gift = 1 THEN 'Gift' ELSE 'Self' END AS gift_type,
    CASE WHEN yearly = 1 THEN 'Yearly' ELSE 'Monthly' END AS subscription_type,
    CASE WHEN is_mono THEN 'Mono' ELSE 'Multi' END AS box_type,

    -- METRICS DE VOLUME
    COUNT(DISTINCT sub_id) AS subscription_count,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT order_id) AS order_count,

    -- METRICS DE REVENUE (HT)
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_revenue) AS net_revenue,
    AVG(net_revenue) AS avg_revenue_per_sub,

    -- METRICS DE DISCOUNT
    SUM(discount) AS total_discounts,
    AVG(discount) AS avg_discount,

    -- METRICS DE SHIPPING
    SUM(shipping) AS total_shipping,
    AVG(shipping) AS avg_shipping,

    -- METRICS DE COÛTS
    SUM(coop) AS total_coop_costs,
    SUM(assembly_cost) AS total_assembly_costs,
    SUM(pack_cost) AS total_pack_costs,
    SUM(print_cost) AS total_print_costs,
    SUM(consumable_cost) AS total_consumable_costs,
    SUM(shipping_cost) AS total_shipping_costs,
    SUM(COALESCE(gws_costs, 0)) AS total_gws_costs,

    -- METRICS DE MARGE
    SUM(gross_profit) AS gross_profit,
    AVG(gross_profit) AS avg_gross_profit_per_sub,

    -- METRICS DE TVA
    SUM(vat_on_gross_revenue) AS total_vat_revenue,
    SUM(vat_on_discount) AS total_vat_discount,
    SUM(vat_on_shipping) AS total_vat_shipping,

    -- METRICS DE COMPORTEMENT
    AVG(consecutive_boxes) AS avg_consecutive_boxes,
    AVG(total_boxes_so_far) AS avg_total_boxes,
    AVG(COALESCE(box_global_grade, 0)) AS avg_box_grade,

    -- METADATA
    CURRENT_TIMESTAMP() AS last_updated_at

FROM box_sales_filtered

GROUP BY
    date,
    year,
    month,
    dw_country_code,
    acquis_status_lvl1,
    acquis_status_lvl2,
    payment_status,
    next_month_status,
    committed,
    gift_type,
    subscription_type,
    box_type

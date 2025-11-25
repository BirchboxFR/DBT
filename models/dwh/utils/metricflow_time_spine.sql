{{ config(
    materialized='table',
    description='Time spine pour MetricFlow - table de dates de référence',
    tags=['utils', 'semantic_layer']
) }}

WITH date_spine AS (
    -- Génère une série de dates de 2015 à 2030
    -- Ajustez les dates selon vos besoins
    SELECT
        date_day
    FROM
        UNNEST(
            GENERATE_DATE_ARRAY(
                DATE('2015-01-01'),  -- Date de début
                DATE('2030-12-31'),  -- Date de fin
                INTERVAL 1 DAY
            )
        ) AS date_day
)

SELECT
    date_day AS date_day,
    -- Colonnes utiles pour le semantic layer
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    EXTRACT(DAYOFYEAR FROM date_day) AS day_of_year,
    FORMAT_DATE('%Y-%m', date_day) AS year_month,
    FORMAT_DATE('%Y-Q%Q', date_day) AS year_quarter,
    -- Première date du mois
    DATE_TRUNC(date_day, MONTH) AS first_day_of_month,
    -- Dernière date du mois
    LAST_DAY(date_day, MONTH) AS last_day_of_month,
    -- Première date du trimestre
    DATE_TRUNC(date_day, QUARTER) AS first_day_of_quarter,
    -- Dernière date du trimestre
    LAST_DAY(date_day, QUARTER) AS last_day_of_quarter,
    -- Première date de l'année
    DATE_TRUNC(date_day, YEAR) AS first_day_of_year,
    -- Dernière date de l'année
    LAST_DAY(date_day, YEAR) AS last_day_of_year
FROM date_spine
ORDER BY date_day

{{ config(
    materialized='table',
    schema='referentiel',
    persist_docs={"relation": true, "columns": true}
) }}

SELECT
  IRIS,                -- code IRIS (zone ~quartier)
  COM,                 -- code commune INSEE
  LAB_IRIS,            -- libellé du quartier
  C20_ACT1564,         -- nb d'actifs (15-64 ans)

  -- pourcentages de chaque CSP (sur les actifs 15-64 ans)
  ROUND(100 * C20_ACT1564_CS1 / NULLIF(C20_ACT1564, 0), 2) AS pct_agriculteurs,
  ROUND(100 * C20_ACT1564_CS2 / NULLIF(C20_ACT1564, 0), 2) AS pct_artisans_commercants,
  ROUND(100 * C20_ACT1564_CS3 / NULLIF(C20_ACT1564, 0), 2) AS pct_cadres,
  ROUND(100 * C20_ACT1564_CS4 / NULLIF(C20_ACT1564, 0), 2) AS pct_prof_intermediaires,
  ROUND(100 * C20_ACT1564_CS5 / NULLIF(C20_ACT1564, 0), 2) AS pct_employes,
  ROUND(100 * C20_ACT1564_CS6 / NULLIF(C20_ACT1564, 0), 2) AS pct_ouvriers,

  -- catégorie CSP dominante (= la plus représentée dans l'IRIS)
  CASE GREATEST(
    C20_ACT1564_CS1, C20_ACT1564_CS2, C20_ACT1564_CS3,
    C20_ACT1564_CS4, C20_ACT1564_CS5, C20_ACT1564_CS6
  )
    WHEN C20_ACT1564_CS1 THEN 'Agriculteurs exploitants'
    WHEN C20_ACT1564_CS2 THEN 'Artisans / commerçants / chefs entreprise'
    WHEN C20_ACT1564_CS3 THEN 'Cadres / professions intellectuelles supérieures'
    WHEN C20_ACT1564_CS4 THEN 'Professions intermédiaires'
    WHEN C20_ACT1564_CS5 THEN 'Employés'
    WHEN C20_ACT1564_CS6 THEN 'Ouvriers'
    ELSE 'Inconnu'
  END AS csp_dominante
FROM {{ ref('insee_iris_csp') }}

SELECT *, '1_BOX' as source FROM {{ ref('import_compta_box') }}
UNION ALL
SELECT *, '2_SHOP' as source FROM {{ ref('import_compta_shop') }}
UNION ALL
SELECT *, '3_GIFT' as source FROM {{ ref('import_compta_gift') }}
ORDER BY source
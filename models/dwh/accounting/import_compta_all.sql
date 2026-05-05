SELECT *,'BOX' as source FROM {{ ref('import_compta_box') }}
UNION ALL
SELECT *,'SHOP' FROM {{ ref('import_compta_shop') }}
UNION ALL
SELECT *,'GIFT' FROM {{ ref('import_compta_gift') }}
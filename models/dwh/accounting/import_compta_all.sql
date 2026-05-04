SELECT * FROM {{ ref('import_compta_box') }}
UNION ALL
SELECT * FROM {{ ref('import_compta_shop') }}
UNION ALL
SELECT * FROM {{ ref('import_compta_gift') }}
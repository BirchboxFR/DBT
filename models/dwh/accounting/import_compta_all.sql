SELECT date,	journal,	account	,numero_piece	,ecriture,	debit,	credit,	famille_de_categorie,	categorie,	analytic, '1_BOX' as source FROM {{ ref('import_compta_box') }}
UNION ALL
SELECT date,	journal,	account	,numero_piece	,ecriture,	debit,	credit,	famille_de_categorie,	categorie,	analytic, '2_SHOP' as source FROM {{ ref('import_compta_shop') }}
UNION ALL
SELECT date,	journal,	account	,numero_piece	,ecriture,	debit,	credit,	famille_de_categorie,	categorie,	analytic, '3_GIFT' as source FROM {{ ref('import_compta_gift') }}
ORDER BY source
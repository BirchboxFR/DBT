
SELECT 'FR' AS dw_country_code,u.*except(start_date), 
safe_cast(start_date as date) as start_date,
safe_cast(end_date as date) as end_date,
  FROM `bdd_prod_fr.wp_jb_open_comment_posts` u
UNION ALL
SELECT 'DE' AS dw_country_code,u.*except(start_date), 
safe_cast(start_date as date) as start_date,
safe_cast(end_date as date) as end_date, FROM `bdd_prod_de.wp_jb_open_comment_posts` u
UNION ALL
SELECT 'ES' AS dw_country_code,u.*except(start_date), 
safe_cast(start_date as date) as start_date,
safe_cast(end_date as date) as end_date, FROM `bdd_prod_es.wp_jb_open_comment_posts` u
UNION ALL
SELECT 'IT' AS dw_country_code,u.*except(start_date), 
safe_cast(start_date as date) as start_date,
safe_cast(end_date as date) as end_date, FROM `bdd_prod_it.wp_jb_open_comment_posts` u
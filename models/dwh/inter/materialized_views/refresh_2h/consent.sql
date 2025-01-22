SELECT 'FR' as dw_country_code,c.*except(_sdc_deleted_at),name FROM `teamdata-291012.bdd_prod_fr.wp_jb_user_consent` c
inner join `teamdata-291012.bdd_prod_fr.wp_jb_consent_topic` ct using(consent_topic_id)
where c._sdc_deleted_at is null

union all

SELECT 'DE' as dw_country_code,c.*except(_sdc_deleted_at),name FROM `teamdata-291012.bdd_prod_de.wp_jb_user_consent` c
inner join `teamdata-291012.bdd_prod_de.wp_jb_consent_topic` ct using(consent_topic_id)
where c._sdc_deleted_at is null

union all

SELECT 'ES' as dw_country_code,c.*except(_sdc_deleted_at),name FROM `teamdata-291012.bdd_prod_es.wp_jb_user_consent` c
inner join `teamdata-291012.bdd_prod_es.wp_jb_consent_topic` ct using(consent_topic_id)
where c._sdc_deleted_at is null


union all

SELECT 'IT' as dw_country_code,c.*except(_sdc_deleted_at),name FROM `teamdata-291012.bdd_prod_it.wp_jb_user_consent` c
inner join `teamdata-291012.bdd_prod_it.wp_jb_consent_topic` ct using(consent_topic_id)
where c._sdc_deleted_at is null

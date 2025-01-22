select 'FR'dw_country_code,country_code,category,max(taux) taux
from `teamdata-291012.bdd_prod_fr.wp_jb_tva_product`
where category='normal'
group by all
union all
select 'DE',country_code,category,max(taux) taux
from `teamdata-291012.bdd_prod_de.wp_jb_tva_product`
where category='normal'
group by all
union all
select 'ES',country_code,category,max(taux) taux
from `teamdata-291012.bdd_prod_es.wp_jb_tva_product`
where category='normal'
group by all
union all
select 'IT',country_code,category,max(taux) taux
from `teamdata-291012.bdd_prod_it.wp_jb_tva_product`
where category='normal'
group by all
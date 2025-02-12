with moyenne_marque as  ( 
select p.dw_country_code , p.raw_brand_name,
 count (distinct p.coupon)as nb_operation, round ( AVG(p.taux_capping) , 3) AS moyenne_capping , round ( AVG(p.nb_totale_use) , 3) AS moyenne_nb_utilisation_totale 
from forecast.analyse_offre_gws p

where p.Type_ope_market is not null 
and raw_brand_name is not null
and p.jour_ope < 35
and p.Type_ope_market in ("GWS No1", "GWS No2","GWS No3", "GWS No4")
AND p.to_consider_analysis = 1
group by p.dw_country_code ,p.raw_brand_name 
having   moyenne_capping  is not null
order by dw_country_code,   moyenne_capping  DESC
)
,
Classement_capping as (
select c.dw_country_code, raw_brand_name, nb_operation ,  moyenne_capping  , moyenne_nb_utilisation_totale , ROW_NUMBER() OVER (PARTITION BY c.dw_country_code ORDER BY    moyenne_capping  DESC) AS numero_ligne, safe_divide(nb_marque_classement_pays,4) as nb_marque_groupe , 
from moyenne_marque c
left join ( 
  select dw_country_code , count(*) as nb_marque_classement_pays
  from moyenne_marque
  group by dw_country_code
) s on s.dw_country_code = c.dw_country_code
order by dw_country_code ,  moyenne_capping   DESC
)
,
Classement_volume as (
select c.dw_country_code, raw_brand_name, nb_operation ,  moyenne_capping  , moyenne_nb_utilisation_totale , ROW_NUMBER() OVER (PARTITION BY c.dw_country_code ORDER BY    moyenne_nb_utilisation_totale  DESC) AS numero_ligne, safe_divide(nb_marque_classement_pays,4) as nb_marque_groupe , 
from moyenne_marque c
left join ( 
  select dw_country_code , count(*) as nb_marque_classement_pays
  from moyenne_marque
  group by dw_country_code
) s on s.dw_country_code = c.dw_country_code
order by dw_country_code ,  moyenne_nb_utilisation_totale  DESC
)

select cc.dw_country_code, cc.raw_brand_name, cc.nb_operation ,  cc.moyenne_capping  , cc.moyenne_nb_utilisation_totale  ,
case when (cc.numero_ligne <  cc.nb_marque_groupe ) then 1
 when (cc.numero_ligne < 2 *  cc.nb_marque_groupe) then 2
 when (cc.numero_ligne < 3*  cc.nb_marque_groupe ) then 3
else 4
end as classement_groupe_marque ,
case when (cv.numero_ligne <  cv.nb_marque_groupe ) then 1
 when (cv.numero_ligne < 2 *  cv.nb_marque_groupe) then 2
 when (cv.numero_ligne < 3*  cv.nb_marque_groupe ) then 3
else 4
end as classement_groupe_marque_volume 
from Classement_capping cc 
LEFT JOIN Classement_volume cv ON cc.dw_country_code = cv.dw_country_code and cc.raw_brand_name = cv.raw_brand_name

ORDER BY classement_groupe_marque ASC
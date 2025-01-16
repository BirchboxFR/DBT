
select b.*,s.box_id from (
select year,month,'FR' dw_country_code ,case when fr is not null then FR
end as obj  from `sales.obj_box`
union all
select year,month,'DE' dw_country_code ,case when DE is not null then DE
end as obj  from `sales.obj_box`
union all
select year,month,'ES' dw_country_code ,case when ES is not null then ES
end as obj  from `sales.obj_box`

) b

inner join 

(select distinct year,month,box_id,dw_country_code from `sales.box_sales` ) s on s.year=b.year and s.month=b.month and s.dw_country_code =b.dw_country_code
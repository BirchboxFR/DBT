with debut as (
  select  id, dw_country_code,
  date,
  min(shipping_date) as first,
  LEAD(min(shipping_date)-1) OVER(ORDER BY dw_country_code,id) as last
   from `teamdata-291012.inter.boxes` 
group by 1,2,3
order by id 
)


sELECT  *  FROM UNNEST(
  GENERATE_DATE_ARRAY('2011-01-01','2030-12-31')
  
  )as d

  inner join debut on d >=debut.first  and d<=debut.last
 -- where id=143 and dw_country_code='FR'
order by d asc

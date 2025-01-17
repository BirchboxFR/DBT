SELECT sub_id,dw_country_code,count(*) nb
FROM {{ ref('box_sales') }}
group by all
having nb=1


SELECT distinct 
concat(sq.dw_country_code,'_',cast(sr.user_id as string)) as ID,
user_id ,dw_country_code,
last_value(date) over ( partition by user_id,dw_country_code order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_order_date,
last_value(billing_country) over ( partition by user_id,dw_country_code order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_country,
last_value(billing_zipcode) over ( partition by user_id,dw_country_code order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_zipcode,
last_value(billing_phone) over ( partition by user_id,dw_country_code order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_phone,
last_value(billing_city) over ( partition by user_id,dw_country_code order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_city,
last_value(billing_adr1) over ( partition by user_id,dw_country_code order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_adress,
last_value(billing_civility) over ( partition by user_id,dw_country_code order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_civility,
case when last_value(billing_civility) over ( partition by user_id,dw_country_code order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 'MISTER' then 'M' else 'F' end  gender,
last_value(_rivery_last_update) over ( partition by user_id,dw_country_code order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_update,
 
 FROM {{ ref('orders') }}
  where billing_zipcode<>'DELETED'
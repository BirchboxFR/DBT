
SELECT distinct user_id ,dw_country_code,last_value(date) over ( partition by user_id order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_order_date ,
last_value(billing_country) over ( partition by user_id order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_country,
last_value(billing_zipcode) over ( partition by user_id order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_zipcode,
last_value(billing_phone) over ( partition by user_id order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_phone,
last_value(billing_city) over ( partition by user_id order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_city,
last_value(billing_adr1) over ( partition by user_id order by date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) billing_adress,
 FROM {{ ref('orders') }}
{{
  config(
    materialized='incremental',
    unique_key=['order_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

with histo as (
  select
    min(lastname) as lastname,
    billing_zipcode,
    count(distinct sub_id) as nb,
    lower(regexp_replace(billing_adress, r'[^a-zA-Z0-9]', '')) as billing_adress
  from {{ ref('box_sales') }} bs
  inner join user.customers using(user_key)
  where coupon = 'YEARLY' 
    and next_month_status = 'CHURN'
    and date_diff(current_date, payment_date, day) > 3
  group by billing_zipcode, billing_adress
),

main as (
  select 
    bs.dw_country_code,
    bs.order_id,
    histo.lastname as fraud_lastname,
    c.lastname,
    c.billing_zipcode,
    c.billing_city,
    bs.payment_date,
    count(distinct bs.sub_id) as nb,
    histo.nb as histo_fraude,
    histo.billing_adress as fraude_connu_adress,
    c.billing_adress,
    lower(regexp_replace(c.billing_adress, r'[^a-zA-Z0-9]', '')) as clean_adress,
    lower(regexp_replace(histo.billing_adress, r'[^a-zA-Z0-9]', '')) as clean_histo_adress
  from {{ ref('box_sales') }} bs
  inner join user.customers c using(user_key)
  left join histo using(billing_zipcode)
  where coupon = 'YEARLY'
    {% if is_incremental() %}
    -- Dans le mode incrémental, ne traiter que les nouvelles commandes depuis la dernière exécution
    and bs.payment_date > (select max(payment_date) from {{ this }})
    {% else %}
    -- Dans le mode full refresh, ne prendre que les données récentes
    and date_diff(current_date, payment_date, day) < 1
    {% endif %}
  group by 
    bs.dw_country_code,
    bs.order_id,
    histo.lastname,
    c.lastname,
    c.billing_zipcode,
    c.billing_city,
    bs.payment_date,
    histo.nb,
    histo.billing_adress,
    c.billing_adress
)

select 
  order_id,
  fraud_lastname,
  lastname,
  billing_city,
  payment_date,
  nb,
  histo_fraude,
  clean_adress,
  clean_histo_adress,
  case 
    when clean_adress like concat('%', clean_histo_adress, '%')
      or clean_histo_adress like concat('%', clean_adress, '%')
    then 'SUSPECT_ADRESS_SIMILAR'
    when histo_fraude is not null then 'SUSPECT_ZIP_ONLY'
    else 'OK'
  end as suspicion,
  functions.trigram_similarity(clean_adress, clean_histo_adress) as similarity_score,
  CONCAT('https://back.blissim.', LOWER(main.dw_country_code), '/wp-admin/admin.php?page=jb-orders&order_id=', order_id) as order_url
from main
where main.histo_fraude > 1 
group by all
having suspicion = 'SUSPECT_ADRESS_SIMILAR' 
   or functions.trigram_similarity(clean_adress, clean_histo_adress) > 0.6
order by payment_date desc
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
    concat (string_agg(lastname ),' _ ', string_agg(firstname)) as lastname,
    max(order_id) as fraud_order,
    billing_zipcode,
    string_agg(email)histo_email,
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
    fraud_order,
    histo.lastname as fraud_lastname,
    c.lastname,
    email,
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
    -- Dans le mode incrémental, traiter les commandes des 2 derniers jours
    and date_diff(current_date, bs.payment_date, day) <= 2
    and (bs.payment_date > (select max(payment_date) from {{ this }}) or bs.order_id not in (select order_id from {{ this }}))
    {% else %}
    -- Dans le mode full refresh, ne prendre que les données des 2 derniers jours
    and date_diff(current_date, payment_date, day) <= 2
    {% endif %}
  group by ALL
),

suspicious_orders as (
  select 
    order_id,
    max(fraud_order) as fraud_order,
    fraud_lastname,
    lastname,
    email,
    histo_email,
    billing_city,
    payment_date,
    sum(nb) as nb,
    sum(histo_fraude) as histo_fraude,
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
    CONCAT('https://back.blissim.', LOWER(main.dw_country_code), '/wp-admin/admin.php?page=jb-orders&order_id=', order_id) as order_url,
    -- Ajouter un classement par similarité pour chaque order_id
    row_number() over (partition by order_id order by functions.trigram_similarity(clean_adress, clean_histo_adress) desc) as similarity_rank
  from main
  where 1=1
  group by all
  having suspicion = 'SUSPECT_ADRESS_SIMILAR' or 
     functions.trigram_similarity(clean_adress, clean_histo_adress) > 0.6
)

-- Sélectionner uniquement la ligne avec le meilleur score de similarité pour chaque commande
select
  order_id,
  current_Datetime() date,
  fraud_order,
  fraud_lastname,
  lastname,
  billing_city,
  payment_date,
  max(similarity_rank) nb,
   histo_fraude,
  clean_adress,
  clean_histo_adress,
  suspicion,
  similarity_score,
  order_url
from suspicious_orders
where similarity_rank = 1
group by all
order by payment_date desc
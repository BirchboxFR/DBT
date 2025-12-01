-- snapshots/customers_history.sql
{% snapshot customers_history %}

{{
    config(
      unique_key='user_key',
      strategy='check',
      check_cols=[
        'email','firstname','lastname','gender','birth_date','billing_phone',
        'billing_region','billing_country','billing_zipcode','billing_city',
        'billing_adress','user_consent_optin_email','optin','optin_ctc',
        'optin_email','optin_partner','optin_sms','optin_whatsapp','skin_type',
        'skin_complexion','hair_type','hair_color','beauty_budget','skin_tone',
        'box_sub_status','current_sub_type','current_is_committed',
        'current_coupon_code','next_box_churn','is_admin','is_shopper',
        'is_shopper_fullsize','is_ever_gifted','is_raffer'
      ]
    )
}}

select
  -- exclude all float columns you want to override + other exceptions
  * except(box_net_revenue, box_average_discount, array_boxes),

  -- re-add them casted to NUMERIC (strict)
  cast(box_net_revenue as numeric) as box_net_revenue,
  cast(box_average_discount as numeric) as box_average_discount

from {{ ref('customers') }}

{% endsnapshot %}

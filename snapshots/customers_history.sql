-- snapshots/customers_history.sql
{% snapshot customers_history %}

{{
    config(
      unique_key='user_key',
      strategy='check',
      check_cols=[
        -- Identité
        'email',
        'firstname',
        'lastname',
        'gender',
        'birth_date',
        
        -- Coordonnées
        'billing_phone',
        'billing_region',
        'billing_country',
        'billing_zipcode',
        'billing_city',
        'billing_adress',
        
        -- Consentements (changements = important)
        'user_consent_optin_email',
        'optin',
        'optin_ctc',
        'optin_email',
        'optin_partner',
        'optin_sms',
        'optin_whatsapp',
        
        -- Profil beauté (change rarement, important pour la segmentation)
        'skin_type',
        'skin_complexion',
        'hair_type',
        'hair_color',
        'beauty_budget',
        'skin_tone',
        
        -- Statut abonnement (critique pour le business)
        'box_sub_status',
        'current_sub_type',
        'current_is_committed',
        'current_coupon_code',
        'next_box_churn',
        
        -- Flags importants
        'is_admin',
        'is_shopper',
        'is_shopper_fullsize',
        'is_ever_gifted',
        'is_raffer'
      ]
    )
}}

select * except(array_boxes) from {{ ref('customers') }}

{% endsnapshot %}
-- snapshots/customers_history.sql
{% snapshot customers_history %}

{{
    config(
      unique_key='user_key'
    )
}}

select * from {{ ref('customers') }}

{% endsnapshot %}
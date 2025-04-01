{% snapshot history_segments %}

    {{
        config(
          target_database='normalised-417010',  
          target_schema='user',
           strategy='check',
          check_cols=['status'], 
          invalidate_hard_deletes=True,
          unique_key='user_id',
        )
    }}

    SELECT 
    user_id,
    status,
    CURRENT_TIMESTAMP() AS snapshot_date
    FROM {{ref('today_segments')}}

{% endsnapshot %}
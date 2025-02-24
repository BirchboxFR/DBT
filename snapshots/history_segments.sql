{% snapshot history_segments %}

    {{
        config(
          target_schema='user',
          strategy='timestamp',
          updated_at='snapshot_date',
          invalidate_hard_deletes=True,
        )
    }}

    SELECT 
        user_id,
        status,
        CURRENT_date() AS snapshot_date
    FROM (
        SELECT user_id, status
        FROM {{ref('today_segments')}}
       
    )

{% endsnapshot %}
{% snapshot history_segments_full %}

    {{
        config(
          target_database='normalised-417010',  
          target_schema='user',
          strategy='timestamp',
          updated_at='snapshot_date',
          invalidate_hard_deletes=True,
          unique_key='user_id',
        )
    }}

    SELECT 
        user_id,
        status,
        CURRENT_TIMESTAMP() AS snapshot_date
    FROM (
        SELECT user_id, status
        FROM {{ref('today_segments')}}
    )

{% endsnapshot %}
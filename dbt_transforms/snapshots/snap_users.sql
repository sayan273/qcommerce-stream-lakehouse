{% snapshot snap_users %}

{{
    config(
      target_database='warehouse',
      target_schema='snapshots',
      unique_key='user_id',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

SELECT 
    user_id,
    membership_tier,
    preferred_city,
    updated_at
FROM {{ source('raw_data', 'raw_users') }}

{% endsnapshot %}
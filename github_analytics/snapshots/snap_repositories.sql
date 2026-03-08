{% snapshot snap_repositories %}

{{
    config(
      target_schema='snapshots',
      unique_key='full_name',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('bronze', 'raw_repositories') }}

{% endsnapshot %}
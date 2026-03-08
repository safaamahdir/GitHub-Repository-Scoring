{{ config(
    materialized= 'view'
) }}

with source as (
    select * from {{ source('bronze', 'raw_pull_requests') }}
),

cleaned as ( 
    select
        -- Repo_id and user_login
        repo_full_name as repo_id,
        coalesce(user_login, 'Unknown') as author_login,

        -- Rename and cast fields (dates to TIMESTAMP, pr_number to INTEGER).
        cast(pr_number as integer) as pr_number,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(closed_at as timestamp) as closed_at,
        cast(merged_at as timestamp) as merged_at,

        --Create a boolean is_merged = merged_at IS NOT NULL.
        case 
            when merged_at is not null then true 
            else false 
        end as is_merged,
        
        -- Create a boolean is_draft from the draft field.
        case 
            when draft = 'true' then true 
            else false 
        end as is_draft,

        -- Compute time_to_close_hours: difference in hours between created_at and merged_at (if merged) or closed_at (if closed), otherwise NULL.
        case 
            when merged_at is not null then datediff('hour', created_at, merged_at)
            when closed_at is not null then datediff('hour', created_at, closed_at)
            else null
        end as time_to_close_hours

    from source
    
    
    where pr_number is not null 
)

select * from cleaned
{{ config( 
    materialized= 'view'
) }}
with source as (
    select * from {{ source('bronze', 'raw_issues') }}
),

cleaned as ( 
    select
        --Rename and cast fields (same logic as other models).
        cast(issue_number as integer) as issue_number,
        cast(comments as integer) as comments,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(closed_at as timestamp) as closed_at,

        -- Cast is_pull_request to BOOLEAN.
        case 
            when is_pull_request = 'true' then true 
            else false 
        end as is_pull_request,

        -- Compute time_to_close_hours (same logic as for PRs).
        case 
            when closed_at is not null then datediff('hour', created_at, closed_at)
            else null
        end as time_to_close_hours

    -- Filter out pull requests: only keep rows where is_pull_request = false.
    from source
    where is_pull_request = 'false' and issue_number is not null
)

-- IMPORTANT: keep only REAL issues 
select * from cleaned
where is_pull_request = false

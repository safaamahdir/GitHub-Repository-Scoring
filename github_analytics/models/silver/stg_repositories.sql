-- models/silver/stg_repositories.sql
{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('bronze', 'raw_repositories') }}
),

cleaned as (
    
    select
        -- Rename full_name to repo_id (business key).
        full_name as repo_id,

        -- Cast dates (created_at, updated_at, pushed_at) to TIMESTAMP
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(pushed_at as timestamp) as pushed_at,

        -- Cast numeric metrics (stargazers_count, forks_count, etc.) to INTEGER.
        cast(stargazers_count as integer) as stargazers_count,
        cast(forks_count as integer) as forks_count,
        cast(open_issues_count as integer) as open_issues_count,

        -- Replace null values: description → ’No description’, language → ’Unknown’.
        coalesce(description, 'No description') as description,
        coalesce(language, 'Unknown') as language,

        --  Compute a derived column repo_age_days (difference between today and created_at).
        datediff('day', cast(created_at as date), current_date) as repo_age_days,
        
    -- Exclude archived repositories (archived = true).
    from source
    where not archived 
)

select * from cleaned
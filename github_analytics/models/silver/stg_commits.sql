-- models/silver/stg_commits.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}
with source as (
    select * from {{ source('bronze', 'raw_commits') }}
),
cleaned as (
    select 
        --Rename sha to commit_sha, repo_full_name to repo_id.
         sha as commit_sha,
         repo_full_name as repo_id,
         
         committer_login,
         
         -- Handle null author_login with COALESCE
        coalesce(author_login, 'Unknown') as author_login,
        
        -- Cast author_date and committer_date to TIMESTAMP.
        cast(author_date as timestamp) as author_date,
        cast(committer_date as timestamp) as committer_date,
    
        --  Extract day_of_week and hour_of_day from author_date (useful for analyzing development patterns).
        extract(dayofweek from author_date) as day_of_week,
        extract(hour from author_date) as hour_of_day,

        --  Truncate message to 200 characters.
        left(message, 200) as message


    from source
    where sha is not null-- Filter rows where sha IS NULL.

    {% if is_incremental() %}
      and author_date > (select max(author_date) from {{ this }})
    {% endif %}
)
select * from cleaned
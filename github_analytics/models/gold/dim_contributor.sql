{{ config(
    materialized= 'table'
) }} 

-- Expected structure:
-- • contributor_id / login: unique contributor identifier.
-- • first_contribution_at: date of first activity.
-- • repos_contributed_to: number of distinct repos.
-- • total_activities: total number of activities (commits + PRs).
-- Hint: Combine authors from stg_commits and stg_pull_requests with a UNION, then aggregate by login. Exclude the ’unknown’ login.

with all_activities as (
    -- Commit activity
    select 
        author_login as login,
        author_date as activity_at,
        repo_id
    from {{ ref('stg_commits') }}
    where author_login != 'Unknown'

    union all

    -- Pull request activity
    select 
        author_login as login,
        created_at as activity_at,
        repo_id
    from {{ ref('stg_pull_requests') }}
    where author_login != 'Unknown'
),
final as (
    select
        login as contributor_id,
        min(activity_at) as first_contribution_at,
        count(distinct repo_id) as repos_contributed_to,
        count(*) as total_activities
    from all_activities
    group by contributor_id
)

select * from final
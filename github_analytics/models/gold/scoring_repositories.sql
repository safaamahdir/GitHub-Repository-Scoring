{{ config(materialized='table') }}

--1. CTE recent_activity: aggregate metrics from the last 30 days from fact_repo_activity (commits, merged PRs, contributors, average close time). Also compute total_prs, merged_prs, total_issues, closed_issues over the full history for community ratios.
with recent_activity as (
    select
        repo_id,
        count(distinct case when activity_type = 'commit' then activity_id end) as recent_commits,
        count(distinct case when activity_type = 'merged_pr' then activity_id end) as recent_merged_prs,
        count(distinct case when activity_type = 'pr_contributor' then contributor_id end) as recent_contributors,
        avg(case when activity_type = 'closed_pr' then close_time end) as avg_close_time,
        count(distinct case when activity_type in ('pr', 'merged_pr') then activity_id end) as total_prs,
        count(distinct case when activity_type = 'merged_pr' then activity_id end) as merged_prs,
        count(distinct case when activity_type in ('issue', 'closed_issue') then activity_id end) as total_issues,
        count(distinct case when activity_type = 'closed_issue' then activity_id end) as closed_issues
    from {{ ref('fact_repo_activity') }}
    where activity_date >= current_date - interval '30 days'
    group by repo_id
    
)

-- 2. CTE base_metrics: join dim_repository (for descriptive attributes like stars, forks) with recent_activity to have all metrics in a single CTE.

, base_metrics as (
    select
        r.repo_id,
        r.repo_name,
        r.stars,
        r.forks,
        r.language,
        r.description,
        ra.recent_commits,
        ra.recent_merged_prs,
        ra.recent_contributors,
        ra.avg_close_time,
        ra.total_prs,
        ra.merged_prs,
        ra.total_issues,
        ra.closed_issues
    from {{ ref('dim_repository') }} r
    left join recent_activity ra on r.repo_id = ra.repo_id
)

-- 3. CTE ranked: apply NTILE(10) on each metric to obtain a relative rank. Warning: for reaction times (where “lower = better”), use ORDER BY ... DESC in the NTILE.
, ranked as (
    select
        *,
        ntile(10) over (order by recent_commits desc) as recent_commits_rank,
        ntile(10) over (order by recent_merged_prs desc) as recent_merged_prs_rank,
        ntile(10) over (order by recent_contributors desc) as recent_contributors_rank,
        ntile(10) over (order by avg_close_time asc) as avg_close_time_rank
    from base_metrics
)

-- 4. CTE scored: compute each sub-score by normalizing ranks to 100. Formula: (sum_of_ranks) * 100.0 / max_possible.
, scored as (
    select
        *,
        (recent_commits_rank + recent_merged_prs_rank + recent_contributors_rank + avg_close_time_rank) * 100.0 / 40.0 as score
    from ranked
)

-- 5. Final SELECT: compute score_global = weighted average of the 4 sub-scores. Add a RANK() OVER (ORDER BY score_global DESC) for ranking.
select
    repo_id,
    repo_name,
    stars,
    forks,
    language,
    description,
    recent_commits,
    recent_merged_prs,
    recent_contributors,
    avg_close_time,
    total_prs,
    merged_prs,
    total_issues,
    closed_issues,
    score as score_global,

    rank() over (order by score desc) as global_rank
from scored

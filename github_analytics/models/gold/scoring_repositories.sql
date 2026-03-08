{{ config(materialized='table') }}

/* 1. CTE recent_activity: aggregate metrics from the last 30 days from
fact_repo_activity (commits, merged PRs, contributors, average close time).
Also compute total_prs_lifetime, total_merged_lifetime, total_issues, closed_issues over the
full history for community ratios.
*/
with recent_activity as (
    select
        repo_id,
        sum(case when activity_date >= current_date - interval '30 days' then commits_count else 0 end) as commits_30d,
        sum(case when activity_date >= current_date - interval '30 days' then prs_merged else 0 end) as prs_merged_30d,
        sum(case when activity_date >= current_date - interval '30 days' then unique_committers else 0 end) as unique_committers_30d,
        avg(case when activity_date >= current_date - interval '30 days' then avg_pr_close_hours end) as avg_pr_close_hours_30d,
        sum(prs_opened) as total_prs_lifetime_lifetime,
        sum(prs_merged) as total_merged_lifetime,
        sum(issues_opened) as total_issues_lifetime,
        sum(issues_closed) as total_issues_closed_lifetime
    from {{ ref('fact_repo_activity') }}
    group by 1
),

/* 2. CTE base_metrics: join dim_repository (for descriptive attributes like stars,
forks) with recent_activity to have all metrics in a single CTE.*/

base_metrics as (
    select
        r.*,
        a.*,
        coalesce(a.total_merged_lifetime * 1.0 / nullif(a.total_prs_lifetime_lifetime, 0), 0) as pr_merge_ratio,
        coalesce(a.total_issues_closed_lifetime * 1.0 / nullif(a.total_issues_lifetime, 0), 0) as issue_resolution_ratio
    from {{ ref('dim_repository') }} r
    inner join recent_activity a on r.repo_id = a.repo_id
),

/* 3. CTE ranked: apply NTILE(10) on each metric to obtain a relative rank. Warn
ing: for reaction times (where “lower = better”), use ORDER BY ... DESC in the
NTILE. */
ranked as (
    select
        *,
        ntile(10) over (order by commits_30d asc) as rank_commits,
        ntile(10) over (order by prs_merged_30d asc) as rank_prs,
        ntile(10) over (order by unique_committers_30d asc) as rank_contributors,
        ntile(10) over (order by avg_pr_close_hours_30d desc) as rank_reaction_time
    from base_metrics
),

/* 4. CTE scored: compute each sub-score by normalizing ranks to 100. Formula:
(sum_of_ranks) * 100.0 / max_possible */
scored as (
    select
        *,
        (rank_commits - 1) * 100.0 / 9.0 as score_commits,
        (rank_prs - 1) * 100.0 / 9.0 as score_prs,
        (rank_contributors - 1) * 100.0 / 9.0 as score_contributors,
        (rank_reaction_time - 1) * 100.0 / 9.0 as score_reaction_time
    from ranked
)

/* 5. Final SELECT: compute score_global = weighted average of the 4 sub-scores.
Add a RANK() OVER (ORDER BY score_global DESC) for repo_rank. */
select
    *,
    (0.25 * score_commits + 0.25 * score_prs + 0.25 * score_contributors + 0.25 * score_reaction_time) as score_global,
    rank() over (order by (0.25 * score_commits + 0.25 * score_prs + 0.25 * score_contributors + 0.25 * score_reaction_time) desc) as repo_rank
from scored
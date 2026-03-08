{{ config(materialized='table') }}

-- For each (repo_id, activity_date) pair, compute:

--  commits_count, unique_committers (from stg_commits).
with daily_commits as (
        repo_id,
        cast(commit_date as date) as activity_date,
        count(*) as commits_count,
        count(distinct committer_id) as unique_committers
    from {{ ref('stg_commits') }}
    where commit_date is not null
    group by repo_id, cast(commit_date as date)
),

-- prs_opened, prs_merged, avg_pr_close_hours (from stg_pull_requests).
daily_prs as (
    select
        repo_id,
        cast(created_at as date) as activity_date,
        count(case when status = 'opened' then 1 end) as prs_opened,
        count(case when status = 'merged' then 1 end) as prs_merged,
        avg(case when closed_at is not null then datediff(hour, created_at, closed_at) end) as avg_pr_close_hours
    from {{ ref('stg_pull_requests') }}
    where created_at is not null
    group by repo_id, cast(created_at as date)
),

-- issues_opened, issues_closed, avg_issue_close_hours (from stg_issues).
daily_issues as (
    select
        repo_id,
        cast(created_at as date) as activity_date,
        count(case when status = 'opened' then 1 end) as issues_opened,
        count(case when status = 'closed' then 1 end) as issues_closed,
        avg(case when closed_at is not null then datediff(hour, created_at, closed_at) end) as avg_issue_close_hours
    from {{ ref('stg_issues') }}
    where created_at is not null
    group by repo_id, cast(created_at as date)
),


all_dates as (
    select distinct repo_id, activity_date from daily_commits
    union
    select distinct repo_id, activity_date from daily_prs
    union
    select distinct repo_id, activity_date from daily_issues
)

select
    ad.repo_id,
    ad.activity_date,
    --   date_id: foreign key to dim_date in YYYYMMDD format.
    cast(to_char(ad.activity_date, 'YYYYMMDD') as integer) as date_id,
    coalesce(dc.commits_count, 0) as commits_count,
    coalesce(dc.unique_committers, 0) as unique_committers,
    coalesce(dp.prs_opened, 0) as prs_opened,
    coalesce(dp.prs_merged, 0) as prs_merged,
    dp.avg_pr_close_hours,
    coalesce(di.issues_opened, 0) as issues_opened,
    coalesce(di.issues_closed, 0) as issues_closed,
    di.avg_issue_close_hours
from all_dates ad
left join daily_commits dc on ad.repo_id = dc.repo_id and ad.activity_date = dc.activity_date
left join daily_prs dp on ad.repo_id = dp.repo_id and ad.activity_date = dp.activity_date
left join daily_issues di on ad.repo_id = di.repo_id and ad.activity_date = di.activity_date
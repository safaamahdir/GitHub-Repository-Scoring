{{ config(materialized='table') }}

with repo as (
    select * from {{ ref('stg_repositories') }}
)

select
    -- Identity
    repo_id,
    repo_name,
    owner_login,

    -- Descriptive attributes
    description,
    language,
    license_name,

    -- Snapshot counters (from GitHub API, not aggregated)
    stargazers_count    as stars_count,
    forks_count,
    watchers_count,

    -- Temporal
    created_at,
    repo_age_days,

    -- Configuration
    default_branch,
    has_wiki,
    has_pages

from repo
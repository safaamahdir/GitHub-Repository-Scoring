select
    repo_id,
    total_prs_lifetime,
    total_merged_lifetime
from {{ ref('scoring_repositories') }}
where total_merged_lifetime > total_prs_lifetime

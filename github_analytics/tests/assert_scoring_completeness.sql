-- Fails if a repo with activity data is missing from scoring
select
    d.repo_id
from {{ ref('dim_repository') }} d
inner join {{ ref('fact_repo_activity') }} f on d.repo_id = f.repo_id
left join {{ ref('scoring_repositories') }} s on d.repo_id = s.repo_id
where s.repo_id is null

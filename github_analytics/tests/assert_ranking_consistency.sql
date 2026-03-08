-- Fails if the repo with repo_rank=1 doesn't have the highest score_global
select
    repo_id,
    score_global,
    repo_rank
from {{ ref('scoring_repositories') }}
where repo_rank = 1
  and score_global < (
      select max(score_global)
      from {{ ref('scoring_repositories') }}
  )

select
    pr_number as entity_id,
    'pull_request' as entity_type,
    created_at,
    closed_at
from {{ ref('stg_pull_requests') }}
where closed_at is not null
  and closed_at <= created_at

union all

select
    issue_number as entity_id,
    'issue' as entity_type,
    created_at,
    closed_at
from {{ ref('stg_issues') }}
where closed_at is not null
  and closed_at <= created_at

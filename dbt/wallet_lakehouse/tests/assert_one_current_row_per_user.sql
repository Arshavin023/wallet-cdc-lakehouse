-- Singular test: the SCD2 history must have exactly one "current" row per
-- user_id — if dbt snapshot ever produces two open-ended rows for the same
-- user (a bug, or a botched manual backfill), this catches it.

select user_id, count(*) as current_row_count
from {{ ref('dim_users_history') }}
where is_current
group by 1
having count(*) > 1

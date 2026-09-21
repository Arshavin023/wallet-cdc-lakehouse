-- Singular test: fails if any transaction has a negative USD value.
-- Dependency-free stand-in for dbt_utils.accepted_range (see packages.yml —
-- that package needs network access to hub.getdbt.com to install, which
-- this sandboxed environment doesn't have; both are valid, this one just
-- has zero external dependencies).

select transaction_id, usd_value_at_tx
from {{ ref('fact_transactions') }}
where usd_value_at_tx is not null
  and usd_value_at_tx < 0

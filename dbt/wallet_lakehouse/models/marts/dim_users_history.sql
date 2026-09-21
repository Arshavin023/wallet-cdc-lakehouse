-- Grain: one row per (user_id, valid_from) — full Type-2 change history.
--
-- Use this instead of dim_users when you need a point-in-time join, e.g.:
--
--   select f.transaction_id, h.kyc_status
--   from fact_transactions f
--   join dim_users_history h
--     on f.user_id = h.user_id
--    and f.submitted_at >= h.valid_from
--    and f.submitted_at <  coalesce(h.valid_to, '9999-12-31')
--
-- to get the KYC status *as of the transaction*, not today's value.

select
    user_id,
    lower(trim(email))   as email,
    upper(country_code)  as country_code,
    signup_channel,
    kyc_status,
    dbt_valid_from        as valid_from,
    dbt_valid_to           as valid_to,
    dbt_valid_to is null    as is_current
from {{ ref('users_snapshot') }}

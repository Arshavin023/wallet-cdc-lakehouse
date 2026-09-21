-- Grain: one row per user_id — the CURRENT version only.
--
-- Backed by snapshots/users_snapshot.sql, dbt's native Type-2 SCD
-- mechanism, rather than a plain "select * from stg_users": filtering to
-- dbt_valid_to IS NULL gives the latest version per user, and
-- dim_users_history.sql exposes the full change history for anyone who
-- needs a point-in-time join instead (e.g. "what was this user's KYC
-- status when transaction X happened").

with current_users as (
    select
        user_id,
        lower(trim(email))   as email,
        upper(country_code)  as country_code,
        signup_channel,
        kyc_status,
        created_at           as user_created_at,
        updated_at           as user_updated_at
    from {{ ref('users_snapshot') }}
    where dbt_valid_to is null
),

wallet_counts as (
    select
        user_id,
        count(*)                                  as wallet_count,
        count(*) filter (where chain = 'ethereum') as ethereum_wallet_count
    from {{ ref('stg_wallets') }}
    group by 1
)

select
    u.user_id,
    u.email,
    u.country_code,
    u.signup_channel,
    u.kyc_status,
    coalesce(w.wallet_count, 0)          as wallet_count,
    coalesce(w.ethereum_wallet_count, 0) as ethereum_wallet_count,
    u.user_created_at,
    u.user_updated_at
from current_users u
left join wallet_counts w on u.user_id = w.user_id

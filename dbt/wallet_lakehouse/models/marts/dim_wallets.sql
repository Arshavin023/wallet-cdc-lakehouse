-- Grain: one row per wallet_id.

select
    w.wallet_id,
    w.user_id,
    w.chain,
    w.address,
    w.wallet_type,
    w.is_primary,
    w.created_at as wallet_created_at,
    w.updated_at as wallet_updated_at
from {{ ref('stg_wallets') }} w

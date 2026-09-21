-- Grain: one row per transaction_id (the finest grain the OLTP source
-- emits). Joins to dim_wallets/dim_users are 1-to-1 on wallet_id/user_id,
-- so no fan-out — a SUM(usd_value_at_tx) here is always a true transaction
-- total, never inflated by a join.

with tx as (
    select * from {{ ref('stg_transactions') }}
),

wallets as (
    select wallet_id, user_id, chain from {{ ref('dim_wallets') }}
)

select
    tx.transaction_id,
    tx.wallet_id,
    w.user_id,
    w.chain,
    tx.tx_hash,
    tx.direction,
    tx.asset_symbol,
    tx.amount,
    tx.usd_value_at_tx,
    tx.fee_native,
    tx.status,
    tx.confirmation_seconds,
    tx.submitted_at,
    tx.confirmed_at,
    date_trunc('day', tx.submitted_at) as submitted_date
from tx
left join wallets w on tx.wallet_id = w.wallet_id

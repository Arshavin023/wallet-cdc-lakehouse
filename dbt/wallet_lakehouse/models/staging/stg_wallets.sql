with source as (
    select * from {{ source('bronze', 'wallets') }}
),

deduped as (
    select
        *,
        row_number() over (partition by wallet_id order by updated_at desc) as _rn
    from source
),

final as (
    select
        wallet_id,
        user_id,
        lower(chain)      as chain,
        lower(address)    as address,
        wallet_type,
        is_primary,
        created_at,
        updated_at
    from deduped
    where _rn = 1
)

select * from final

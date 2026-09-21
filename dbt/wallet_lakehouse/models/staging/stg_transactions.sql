with source as (
    select * from {{ source('bronze', 'transactions') }}
),

deduped as (
    select
        *,
        row_number() over (partition by transaction_id order by updated_at desc) as _rn
    from source
),

final as (
    select
        transaction_id,
        wallet_id,
        nullif(trim(tx_hash), '')      as tx_hash,
        direction,
        upper(asset_symbol)            as asset_symbol,
        amount,
        usd_value_at_tx,
        fee_native,
        status,
        submitted_at,
        confirmed_at,
        updated_at,
        case
            when confirmed_at is not null
                then datediff('second', submitted_at, confirmed_at)
        end as confirmation_seconds
    from deduped
    where _rn = 1
)

select * from final

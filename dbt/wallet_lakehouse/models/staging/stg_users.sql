-- Deduplicates the bronze CDC feed down to one current row per user_id.
-- In production this source is a Delta table that's already been MERGEd to
-- "current state" by spark/bronze_cdc_to_delta.py; the ROW_NUMBER guard
-- here is defensive so this model is correct even if it's ever pointed at
-- a raw, non-deduplicated CDC stream.

with source as (
    select * from {{ source('bronze', 'users') }}
),

deduped as (
    select
        *,
        row_number() over (partition by user_id order by updated_at desc) as _rn
    from source
),

final as (
    select
        user_id,
        lower(trim(email))              as email,
        upper(country_code)             as country_code,
        signup_channel,
        kyc_status,
        created_at,
        updated_at
    from deduped
    where _rn = 1
)

select * from final

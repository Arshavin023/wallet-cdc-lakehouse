{#
    Type-2 SCD on the bronze users feed, using dbt's native snapshot
    mechanism (timestamp strategy, keyed on the CDC `updated_at` column
    Debezium/Postgres already maintains — see oltp/schema.sql's
    set_updated_at() trigger).

    Every dbt snapshot run compares the current bronze row per user_id
    against the last snapshotted version: unchanged rows are left alone,
    changed rows get their old version closed out (dbt_valid_to set) and a
    new version opened (dbt_valid_from = this run, dbt_valid_to = null).

    This answers exactly the "what was this user's KYC status when
    transaction X happened" question dim_users.sql's docstring raised —
    dim_users_history.sql below joins on it to do exactly that.
#}
{% snapshot users_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('bronze', 'users') }}

{% endsnapshot %}

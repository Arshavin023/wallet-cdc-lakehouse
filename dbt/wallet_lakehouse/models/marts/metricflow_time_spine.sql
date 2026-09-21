-- Required by dbt's Semantic Layer (MetricFlow) for any time-based metric
-- (e.g. transaction_volume_usd by day/week/month). One row per calendar
-- day; MetricFlow joins against this to fill in dates with no transactions
-- rather than silently skipping them. Range covers the synthetic dataset's
-- window (2026-01-01 to 2026-09-01, see scripts/generate_synthetic_data.py)
-- with headroom on both sides.
-- https://docs.getdbt.com/docs/build/metricflow-time-spine

with days as (
    select unnest(
        generate_series(
            date '2025-01-01',
            date '2027-01-01',
            interval 1 day
        )
    ) as date_day
)

select date_day from days

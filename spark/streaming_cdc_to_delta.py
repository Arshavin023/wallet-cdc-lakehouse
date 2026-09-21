"""
Structured Streaming job: incrementally reads newly-landed CDC JSON files
from the raw landing zone and appends them into a bronze Delta table using
Auto Loader (`cloudFiles`), demonstrating the streaming half of "batch and
streaming" from the JD — as distinct from the batch MERGE job in
bronze_cdc_to_delta.py.

Trigger choice matters here: Databricks Free Edition is serverless-only,
and serverless compute only supports `Trigger.AvailableNow()` (recommended)
or the deprecated `Trigger.Once()` — NOT `Trigger.ProcessingTime(...)` or
`Trigger.Continuous(...)` (see Databricks "Serverless compute limitations",
Streaming limitations section). So this job runs as a scheduled
micro-batch: `AvailableNow()` processes everything that has landed since
the last run, then stops — which is also the pattern Databricks recommends
for cost control on any tier, not just Free Edition. For a genuinely
always-on stream you'd move this to a Lakeflow Declarative Pipeline in
continuous mode, noted in the README as the production-scale alternative.

Usage: schedule this as a Databricks Job task (every N minutes), or run
interactively for a single AvailableNow batch:
    python spark/streaming_cdc_to_delta.py --table transactions
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG = "wallet_lakehouse"
BRONZE_SCHEMA = "bronze"
RAW_CDC_PATH = "s3://wallet-lakehouse-raw/raw/cdc"
CHECKPOINT_ROOT = "s3://wallet-lakehouse-raw/checkpoints/bronze_stream"


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("wallet-lakehouse-bronze-stream").getOrCreate()


def stream_table(spark: SparkSession, table: str) -> None:
    target_name = f"{CATALOG}.{BRONZE_SCHEMA}.cdc_{table}_stream"
    source_path = f"{RAW_CDC_PATH}/{table}/"
    checkpoint_path = f"{CHECKPOINT_ROOT}/{table}"

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .select(
            "after.*",
            F.col("op").alias("_cdc_op"),
            F.col("ts_ms").alias("_cdc_ts_ms"),
            F.current_timestamp().alias("_stream_ingested_at"),
        )
        .where("_cdc_op != 'd'")  # deletes handled by the batch MERGE job's reconciliation pass
    )

    query = (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)  # the only non-deprecated trigger Free Edition serverless supports
        .toTable(target_name)
    )
    query.awaitTermination()
    print(f"AvailableNow batch complete for {target_name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", choices=["users", "wallets", "transactions"], default="transactions")
    args = parser.parse_args()

    spark = get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
    stream_table(spark, args.table)


if __name__ == "__main__":
    main()

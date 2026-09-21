"""
Batch job: reads landed CDC JSON (Debezium envelopes) and on-chain JSON from
the raw landing zone / Unity Catalog external location, and MERGEs them into
bronze Delta tables — one per source entity, keyed on the OLTP primary key,
always keeping the latest `after` image.

Run as a Databricks notebook/job on **serverless** compute (Free Edition is
serverless-only — see README "Databricks Free Edition notes"). This script
targets Unity Catalog tables directly (`catalog.schema.table`), since
serverless compute requires Unity Catalog for external storage access —
there's no DBFS mount / instance-profile path here.

Usage (as a Databricks job or from a notebook):
    %run ./bronze_cdc_to_delta

Or via the Databricks CLI:
    databricks bundle run bronze_cdc_batch
"""
from __future__ import annotations

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "wallet_lakehouse"
BRONZE_SCHEMA = "bronze"
RAW_CDC_PATH = "s3://wallet-lakehouse-raw/raw/cdc"  # backed by a UC external location
RAW_ONCHAIN_PATH = "s3://wallet-lakehouse-raw/raw/onchain"

CDC_TABLES = ["users", "wallets", "transactions"]
CDC_PRIMARY_KEY = {
    "users": "user_id",
    "wallets": "wallet_id",
    "transactions": "transaction_id",
}


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("wallet-lakehouse-bronze-batch").getOrCreate()


def merge_cdc_table(spark: SparkSession, table: str) -> None:
    pk = CDC_PRIMARY_KEY[table]
    target_name = f"{CATALOG}.{BRONZE_SCHEMA}.cdc_{table}"

    raw = spark.read.json(f"{RAW_CDC_PATH}/{table}/")
    # Debezium envelope: {table, op, ts_ms, before, after, source, consumed_at}.
    # 'd' (delete) has after=null; everything else carries the current row in `after`.
    deletes = raw.filter(F.col("op") == "d")
    upserts = raw.filter(F.col("op") != "d").select(
        "after.*", F.col("ts_ms").alias("_cdc_ts_ms"), F.col("op").alias("_cdc_op")
    )
    # Keep only the latest event per primary key within this batch before merging.
    from pyspark.sql.window import Window

    w = Window.partitionBy(pk).orderBy(F.col("_cdc_ts_ms").desc())
    upserts_latest = (
        upserts.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    )

    if not spark.catalog.tableExists(target_name):
        upserts_latest.write.format("delta").mode("overwrite").saveAsTable(target_name)
        print(f"created {target_name} with {upserts_latest.count()} rows")
        return

    target = DeltaTable.forName(spark, target_name)
    (
        target.alias("t")
        .merge(upserts_latest.alias("s"), f"t.{pk} = s.{pk}")
        .whenMatchedUpdateAll(condition="s._cdc_ts_ms >= t._cdc_ts_ms")
        .whenNotMatchedInsertAll()
        .execute()
    )

    delete_ids = deletes.select(F.col("before")[pk].alias(pk)).distinct()
    if delete_ids.count() > 0:
        (
            target.alias("t")
            .merge(delete_ids.alias("d"), f"t.{pk} = d.{pk}")
            .whenMatchedDelete()
            .execute()
        )

    print(f"merged batch into {target_name}")


def load_onchain_bronze(spark: SparkSession, chain: str) -> None:
    target_name = f"{CATALOG}.{BRONZE_SCHEMA}.onchain_transactions"
    raw = spark.read.json(f"{RAW_ONCHAIN_PATH}/{chain}/")
    if raw.rdd.isEmpty():
        print(f"no on-chain rows found for {chain}, skipping")
        return

    raw = raw.withColumn("_loaded_at", F.current_timestamp())

    if not spark.catalog.tableExists(target_name):
        raw.write.format("delta").mode("overwrite").saveAsTable(target_name)
        print(f"created {target_name} with {raw.count()} rows")
        return

    target = DeltaTable.forName(spark, target_name)
    (
        target.alias("t")
        .merge(raw.alias("s"), "t.hash = s.hash AND t._chain = s._chain")
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"merged on-chain batch for {chain} into {target_name}")


def main():
    spark = get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

    for table in CDC_TABLES:
        merge_cdc_table(spark, table)

    for chain in ("ethereum", "base"):
        load_onchain_bronze(spark, chain)


if __name__ == "__main__":
    sys.exit(main() or 0)

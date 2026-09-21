#!/usr/bin/env python3
"""
Tails the Debezium/Kafka CDC topics (wallet.public.users, wallet.public.wallets,
wallet.public.transactions) and lands each change event as a newline-delimited
JSON file in the raw landing zone, partitioned by table and ingestion date —
the same layout you'd write to S3 (s3://<bucket>/raw/cdc/<table>/dt=YYYY-MM-DD/).

Local mode (default): writes to ./landing_zone/raw/cdc/...
S3 mode: set LANDING_ZONE_S3_BUCKET and this will upload via boto3 instead.

This script is intentionally simple (no Kafka Connect S3 sink, no Flink) so
the CDC mechanics stay visible end to end: Debezium -> Kafka -> here -> Delta
bronze table (see spark/bronze_cdc_to_delta.py). In a production Trust
Wallet-style setup you'd more likely use the Confluent S3 Sink connector or
Databricks' native Kafka source in a Structured Streaming job — both are
noted in the README as the "next step" once this is proven out.
"""
from __future__ import annotations

import json
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_PREFIX = os.environ.get("CDC_TOPIC_PREFIX", "wallet")
TABLES = ["users", "wallets", "transactions"]
TOPICS = [f"{TOPIC_PREFIX}.public.{t}" for t in TABLES]

LANDING_ZONE_ROOT = Path(os.environ.get("LANDING_ZONE_ROOT", "./landing_zone"))
S3_BUCKET = os.environ.get("LANDING_ZONE_S3_BUCKET")  # optional


def landing_path(table: str) -> Path:
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    d = LANDING_ZONE_ROOT / "raw" / "cdc" / table / f"dt={dt}"
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{table}_{datetime.now(timezone.utc).strftime('%H%M%S')}.jsonl"


def maybe_upload_to_s3(local_path: Path, table: str) -> None:
    if not S3_BUCKET:
        return
    import boto3  # imported lazily so local-only runs don't need it

    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"raw/cdc/{table}/dt={dt}/{local_path.name}"
    boto3.client("s3").upload_file(str(local_path), S3_BUCKET, key)
    print(f"  uploaded -> s3://{S3_BUCKET}/{key}")


def run() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "wallet-lakehouse-cdc-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe(TOPICS)
    print(f"Subscribed to {TOPICS} on {BOOTSTRAP_SERVERS}")

    running = True

    def _stop(*_args):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    open_files: dict[str, tuple[Path, object]] = {}
    events_written = 0

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {msg.error()}", file=sys.stderr)
                continue

            table = msg.topic().split(".")[-1]
            payload = msg.value()
            if payload is None:
                continue  # tombstone record from a delete, skipped for now

            event = json.loads(payload)
            # Debezium's envelope: {before, after, source, op, ts_ms}. We keep
            # it whole in bronze — the dbt staging layer is where op/before/after
            # get flattened into a clean "current row" view per entity.
            envelope = {
                "table": table,
                "op": event.get("op"),
                "ts_ms": event.get("ts_ms"),
                "before": event.get("before"),
                "after": event.get("after"),
                "source": event.get("source"),
                "consumed_at": datetime.now(timezone.utc).isoformat(),
            }

            if table not in open_files:
                path = landing_path(table)
                open_files[table] = (path, open(path, "a", encoding="utf-8"))

            path, fh = open_files[table]
            fh.write(json.dumps(envelope) + "\n")
            fh.flush()
            events_written += 1

            if events_written % 50 == 0:
                print(f"  ...{events_written} events landed so far")

    finally:
        for table, (path, fh) in open_files.items():
            fh.close()
            maybe_upload_to_s3(path, table)
        consumer.close()
        print(f"Stopped. {events_written} events landed under {LANDING_ZONE_ROOT}/raw/cdc/")


if __name__ == "__main__":
    run()

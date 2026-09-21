#!/usr/bin/env python3
"""
Generates a Kafka KRaft cluster ID in the same format the real Kafka CLI
tool produces (`kafka-storage.sh random-uuid`): a base64 URL-safe encoding
of a random 16-byte UUID, no padding — 22 characters.

RUN THIS ONCE per cluster, not once per `docker compose up`. Kafka writes
this value into the persisted kafka_data/ volume the first time the broker
starts; every startup after that checks the configured CLUSTER_ID still
matches. Generating a fresh one later either fails to start against the
existing volume or (worse) gets treated as an unrelated cluster.

Usage:
    python3 scripts/generate_kafka_cluster_id.py >> .env
    # or, to fill in an existing .env by hand:
    python3 scripts/generate_kafka_cluster_id.py
"""
from __future__ import annotations

import base64
import sys
import uuid


def generate() -> str:
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")


def main() -> None:
    cluster_id = generate()
    if sys.stdout.isatty():
        print(f"KAFKA_CLUSTER_ID={cluster_id}")
        print("\nCopy the line above into .env (or redirect this script's output into it).", file=sys.stderr)
        print("Generate this once and keep it — see the script's docstring for why.", file=sys.stderr)
    else:
        # piped/redirected — print just the KEY=VALUE line, clean for `>> .env`
        print(f"KAFKA_CLUSTER_ID={cluster_id}")


if __name__ == "__main__":
    main()

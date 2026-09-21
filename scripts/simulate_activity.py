#!/usr/bin/env python3
"""
Continuously inserts new transactions and mutates existing rows (status
transitions, KYC updates) against the live Postgres OLTP database, so that
once Debezium is watching it there's a steady stream of INSERT/UPDATE
events to demonstrate CDC end to end — not just a one-time bulk load.

Run this *after* seed_data/seed_inserts.sql has been loaded and the
Debezium connector is registered, so you can watch events land in
landing_zone/raw/cdc/ in near real time.

Usage:
    python scripts/simulate_activity.py --interval 2 --duration 120
"""
from __future__ import annotations

import argparse
import random
import time

import psycopg2

ASSET_BY_CHAIN = {
    "ethereum": ["ETH", "USDT", "USDC"],
    "bsc": ["BNB", "USDT"],
    "polygon": ["MATIC", "USDC"],
    "arbitrum": ["ETH", "ARB"],
    "base": ["ETH", "USDC"],
}


def connect(dsn: str):
    return psycopg2.connect(dsn)


def insert_transaction(cur) -> None:
    cur.execute("SELECT wallet_id, chain FROM wallets ORDER BY random() LIMIT 1;")
    row = cur.fetchone()
    if not row:
        return
    wallet_id, chain = row
    asset = random.choice(ASSET_BY_CHAIN.get(chain, ["ETH"]))
    amount = round(random.lognormvariate(-2, 2), 8)
    cur.execute(
        """
        INSERT INTO transactions
            (wallet_id, direction, asset_symbol, amount, usd_value_at_tx, fee_native, status, submitted_at)
        VALUES (%s, %s, %s, %s, %s, %s, 'pending', now())
        RETURNING transaction_id;
        """,
        (
            wallet_id,
            random.choice(["send", "receive", "swap"]),
            asset,
            amount,
            round(amount * random.uniform(0.9, 3500), 2),
            round(random.uniform(0.0001, 0.01), 8),
        ),
    )
    return cur.fetchone()[0]


def maybe_confirm_pending(cur) -> None:
    cur.execute(
        "SELECT transaction_id FROM transactions WHERE status = 'pending' ORDER BY random() LIMIT 3;"
    )
    for (tx_id,) in cur.fetchall():
        new_status = random.choices(["confirmed", "failed"], weights=[0.85, 0.15])[0]
        cur.execute(
            "UPDATE transactions SET status = %s, confirmed_at = CASE WHEN %s = 'confirmed' THEN now() ELSE confirmed_at END, "
            "tx_hash = COALESCE(tx_hash, md5(random()::text)) WHERE transaction_id = %s;",
            (new_status, new_status, tx_id),
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dsn",
        default="postgresql://lamisplus_etl:QUWeIQvD27BYei1@localhost:5432/wallets",
        help="Connects to your HOST Postgres directly (this script runs outside Docker, "
        "so plain 'localhost' is correct here — unlike register_connector.py's PG_HOST, "
        "which runs *inside* a container and needs host.docker.internal instead). "
        "Swap the user/password for whatever role you actually created the `wallets` "
        "database with.",
    )
    parser.add_argument("--interval", type=float, default=2.0, help="seconds between batches")
    parser.add_argument("--duration", type=float, default=120.0, help="total seconds to run")
    args = parser.parse_args()

    conn = connect(args.dsn)
    conn.autocommit = True
    cur = conn.cursor()

    start = time.time()
    n_inserts = n_updates = 0
    try:
        while time.time() - start < args.duration:
            with conn.cursor() as c:
                for _ in range(random.randint(1, 4)):
                    insert_transaction(c)
                    n_inserts += 1
                maybe_confirm_pending(c)
                n_updates += 3
            print(f"  inserted={n_inserts} update_attempts={n_updates}", end="\r")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        cur.close()
        conn.close()
        print(f"\nDone. ~{n_inserts} inserts, ~{n_updates} update attempts over {int(time.time()-start)}s.")


if __name__ == "__main__":
    main()

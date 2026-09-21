#!/usr/bin/env python3
"""
Generates a synthetic OLTP dataset for the wallet app: users, wallets, and
transactions, with realistic-ish distributions (a handful of power users,
mixed KYC states, some failed/pending transactions).

Two output modes:
  --to csv   -> writes CSVs to ./seed_data/ (used as dbt seeds for local
                testing with dbt-duckdb, and as the fixture for `load_to_postgres.py`)
  --to sql   -> writes an INSERT script to ./seed_data/seed_inserts.sql
                (run this against Postgres to both seed the DB *and* generate
                the initial batch of CDC events once Debezium is watching it)

Usage:
    python scripts/generate_synthetic_data.py --users 500 --to csv
"""
from __future__ import annotations

import argparse
import csv
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

random.seed(42)  # reproducible dataset

COUNTRIES = ["NG", "US", "GB", "IN", "PH", "KE", "VN", "BR", "ID", "ZA"]
CHANNELS = ["organic", "referral", "paid_ad"]
KYC_STATES = ["pending", "verified", "verified", "verified", "rejected"]  # skew verified
CHAINS = ["ethereum", "bsc", "polygon", "arbitrum", "base"]
ASSETS = {
    "ethereum": ["ETH", "USDT", "USDC"],
    "bsc": ["BNB", "USDT"],
    "polygon": ["MATIC", "USDC"],
    "arbitrum": ["ETH", "ARB"],
    "base": ["ETH", "USDC"],
}
DIRECTIONS = ["send", "receive", "swap"]
STATUSES = ["confirmed", "confirmed", "confirmed", "pending", "failed"]  # skew confirmed

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 9, 1, tzinfo=timezone.utc)


def random_ts(start: datetime = START, end: datetime = END) -> datetime:
    if start >= end:
        return end
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def fake_address(chain: str) -> str:
    if chain == "ethereum" or chain in ("arbitrum", "base", "polygon"):
        return "0x" + uuid.uuid4().hex[:40]
    return "0x" + uuid.uuid4().hex[:40]  # EVM chains only in this dataset


def gen_users(n: int) -> list[dict]:
    users = []
    for uid in range(1, n + 1):
        created = random_ts()
        users.append(
            {
                "user_id": uid,
                "email": f"user{uid}@example.com",
                "country_code": random.choice(COUNTRIES),
                "signup_channel": random.choice(CHANNELS),
                "kyc_status": random.choice(KYC_STATES),
                "created_at": created.isoformat(),
                "updated_at": created.isoformat(),
            }
        )
    return users


def gen_wallets(users: list[dict], avg_per_user: float = 1.6) -> list[dict]:
    wallets = []
    wid = 1
    for u in users:
        n_wallets = max(1, round(random.gauss(avg_per_user, 0.7)))
        used_chains = random.sample(CHAINS, k=min(n_wallets, len(CHAINS)))
        for i, chain in enumerate(used_chains):
            created = datetime.fromisoformat(u["created_at"]) + timedelta(
                minutes=random.randint(0, 4320)
            )
            wallets.append(
                {
                    "wallet_id": wid,
                    "user_id": u["user_id"],
                    "chain": chain,
                    "address": fake_address(chain),
                    "wallet_type": "non_custodial",
                    "is_primary": i == 0,
                    "created_at": created.isoformat(),
                    "updated_at": created.isoformat(),
                }
            )
            wid += 1
    return wallets


# A handful of "power users" transact far more than everyone else — this
# gives the fact table a realistic skew instead of a flat distribution.
def gen_transactions(wallets: list[dict], avg_per_wallet: float = 12.0) -> list[dict]:
    transactions = []
    tid = 1
    power_wallets = set(random.sample([w["wallet_id"] for w in wallets], k=max(1, len(wallets) // 20)))

    for w in wallets:
        n_tx = int(random.expovariate(1 / avg_per_wallet))
        if w["wallet_id"] in power_wallets:
            n_tx *= 8
        n_tx = min(n_tx, 400)

        chain_assets = ASSETS[w["chain"]]
        created_floor = datetime.fromisoformat(w["created_at"])

        for _ in range(n_tx):
            submitted = random_ts(start=max(created_floor, START), end=END)
            status = random.choice(STATUSES)
            confirmed = (
                (submitted + timedelta(seconds=random.randint(5, 600))).isoformat()
                if status == "confirmed"
                else None
            )
            asset = random.choice(chain_assets)
            amount = round(random.lognormvariate(mu=-2, sigma=2), 8)
            usd_price = {"ETH": 3200, "USDT": 1, "USDC": 1, "BNB": 580, "MATIC": 0.6, "ARB": 0.9}.get(
                asset, 1.0
            )
            transactions.append(
                {
                    "transaction_id": tid,
                    "wallet_id": w["wallet_id"],
                    "tx_hash": ("0x" + uuid.uuid4().hex + uuid.uuid4().hex[:24]) if status != "pending" else "",
                    "direction": random.choice(DIRECTIONS),
                    "asset_symbol": asset,
                    "amount": amount,
                    "usd_value_at_tx": round(amount * usd_price, 2),
                    "fee_native": round(random.uniform(0.0001, 0.01), 8),
                    "status": status,
                    "submitted_at": submitted.isoformat(),
                    "confirmed_at": confirmed,
                    "updated_at": (confirmed or submitted.isoformat()),
                }
            )
            tid += 1
    return transactions


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows):>6} rows -> {path}")


def write_sql_inserts(users, wallets, transactions, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("BEGIN;\n\n")
        for u in users:
            f.write(
                "INSERT INTO users (user_id, email, country_code, signup_channel, kyc_status, created_at, updated_at) "
                f"VALUES ({u['user_id']}, '{u['email']}', '{u['country_code']}', '{u['signup_channel']}', "
                f"'{u['kyc_status']}', '{u['created_at']}', '{u['updated_at']}');\n"
            )
        f.write("SELECT setval('users_user_id_seq', (SELECT max(user_id) FROM users));\n\n")

        for w in wallets:
            f.write(
                "INSERT INTO wallets (wallet_id, user_id, chain, address, wallet_type, is_primary, created_at, updated_at) "
                f"VALUES ({w['wallet_id']}, {w['user_id']}, '{w['chain']}', '{w['address']}', "
                f"'{w['wallet_type']}', {str(w['is_primary']).lower()}, '{w['created_at']}', '{w['updated_at']}');\n"
            )
        f.write("SELECT setval('wallets_wallet_id_seq', (SELECT max(wallet_id) FROM wallets));\n\n")

        for t in transactions:
            tx_hash = f"'{t['tx_hash']}'" if t["tx_hash"] else "NULL"
            confirmed = f"'{t['confirmed_at']}'" if t["confirmed_at"] else "NULL"
            f.write(
                "INSERT INTO transactions (transaction_id, wallet_id, tx_hash, direction, asset_symbol, amount, "
                "usd_value_at_tx, fee_native, status, submitted_at, confirmed_at, updated_at) "
                f"VALUES ({t['transaction_id']}, {t['wallet_id']}, {tx_hash}, '{t['direction']}', "
                f"'{t['asset_symbol']}', {t['amount']}, {t['usd_value_at_tx']}, {t['fee_native']}, "
                f"'{t['status']}', '{t['submitted_at']}', {confirmed}, '{t['updated_at']}');\n"
            )
        f.write(
            "SELECT setval('transactions_transaction_id_seq', (SELECT max(transaction_id) FROM transactions));\n\n"
        )
        f.write("COMMIT;\n")
    print(f"wrote SQL insert script -> {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--users", type=int, default=500)
    parser.add_argument("--to", choices=["csv", "sql", "both"], default="both")
    parser.add_argument("--out", default="seed_data")
    args = parser.parse_args()

    out = Path(args.out)
    users = gen_users(args.users)
    wallets = gen_wallets(users)
    transactions = gen_transactions(wallets)

    print(f"generated {len(users)} users, {len(wallets)} wallets, {len(transactions)} transactions")

    if args.to in ("csv", "both"):
        write_csv(users, out / "users.csv")
        write_csv(wallets, out / "wallets.csv")
        write_csv(transactions, out / "transactions.csv")

    if args.to in ("sql", "both"):
        write_sql_inserts(users, wallets, transactions, out / "seed_inserts.sql")


if __name__ == "__main__":
    main()

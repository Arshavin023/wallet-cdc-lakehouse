#!/usr/bin/env python3
"""
Pulls real on-chain transactions from the Etherscan V2 API for a set of
addresses and lands them as raw JSON in the same landing-zone layout the CDC
consumer uses, so the bronze layer treats "internal" and "on-chain" data
consistently.

Etherscan V2 (2024+) unifies all EVM chains behind one API key via the
`chainid` query param — no per-chain key needed. Free tier: 5 req/sec,
100,000 req/day, shared across chains. Get a free key at etherscan.io/apis.

This targets a small, fixed list of well-known public addresses (exchange
hot wallets, popular contracts) rather than scraping arbitrary users, which
keeps the demo data both realistic and legally uncomplicated.

Two ways to run this:

  Manual / local (writes to local disk only):
    export ETHERSCAN_API_KEY=your_key_here
    python ingestion/fetch_onchain_transactions.py --chain ethereum --limit 200

  Scheduled (uploads straight to S3 — see .github/workflows/onchain-ingestion.yml,
  which runs this daily via GitHub Actions using short-lived OIDC-assumed AWS
  credentials, no long-lived access key stored anywhere):
    export ETHERSCAN_API_KEY=your_key_here
    export LANDING_ZONE_S3_BUCKET=your-bucket-name   # from `terraform output raw_bucket_name`
    python ingestion/fetch_onchain_transactions.py --chain ethereum --limit 200

  Local disk writes still happen either way — S3 upload is additive, not a
  replacement — but a GitHub Actions runner's filesystem is thrown away at
  the end of every run, so LANDING_ZONE_S3_BUCKET is what actually makes
  scheduled runs durable.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"

# chainid per https://docs.etherscan.io/etherscan-v2/supported-chains
CHAIN_IDS = {
    "ethereum": 1,
    "bsc": 56,
    "polygon": 137,
    "arbitrum": 42161,
    "base": 8453,
}

# Well-known, high-activity public contract addresses per chain — verified
# against Etherscan/Basescan directly before being hardcoded here. Using
# widely-known token contracts (rather than guessed "exchange wallet"
# addresses) keeps this both verifiable and legally uncomplicated: no
# private user data, no unverified attribution claims.
SAMPLE_ADDRESSES = {
    "ethereum": [
        "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT (Tether USD) contract
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH (Wrapped Ether) contract
    ],
    "base": [
        "0x4200000000000000000000000000000000000006",  # WETH on Base (predeploy address)
    ],
}
# Note: --address lets you point this at any address, including your own
# testnet/mainnet wallets, without editing this file.

LANDING_ZONE_ROOT = Path(os.environ.get("LANDING_ZONE_ROOT", "./landing_zone"))
S3_BUCKET = os.environ.get("LANDING_ZONE_S3_BUCKET")  # optional — see module docstring


def fetch_txlist(address: str, chain: str, api_key: str, limit: int) -> list[dict]:
    params = {
        "chainid": CHAIN_IDS[chain],
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "page": 1,
        "offset": limit,
        "sort": "desc",
        "apikey": api_key,
    }
    resp = requests.get(ETHERSCAN_V2_BASE, params=params, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") not in ("1", 1) and body.get("message") != "No transactions found":
        raise RuntimeError(f"Etherscan error for {address}: {body}")
    return body.get("result", []) if isinstance(body.get("result"), list) else []


def land_events(chain: str, events: list[dict]) -> Path:
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = LANDING_ZONE_ROOT / "raw" / "onchain" / chain / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"onchain_{chain}_{datetime.now(timezone.utc).strftime('%H%M%S')}.jsonl"
    with open(out_path, "w", encoding="utf-8") as f:
        for ev in events:
            ev["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            ev["_chain"] = chain
            f.write(json.dumps(ev) + "\n")
    return out_path


def maybe_upload_to_s3(local_path: Path, chain: str) -> None:
    if not S3_BUCKET:
        return
    import boto3  # imported lazily so local-only runs don't need it installed

    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"raw/onchain/{chain}/dt={dt}/{local_path.name}"
    boto3.client("s3").upload_file(str(local_path), S3_BUCKET, key)
    print(f"  uploaded -> s3://{S3_BUCKET}/{key}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chain", choices=list(CHAIN_IDS), default="ethereum")
    parser.add_argument("--limit", type=int, default=200, help="max tx per address")
    args = parser.parse_args()

    api_key = os.environ.get("ETHERSCAN_API_KEY")
    if not api_key:
        raise SystemExit(
            "Set ETHERSCAN_API_KEY (free key at https://etherscan.io/apis) before running this."
        )

    addresses = SAMPLE_ADDRESSES.get(args.chain, [])
    if not addresses:
        raise SystemExit(f"No sample addresses configured for chain={args.chain}")

    all_events: list[dict] = []
    for addr in addresses:
        print(f"fetching {args.limit} tx for {addr} on {args.chain} ...")
        events = fetch_txlist(addr, args.chain, api_key, args.limit)
        print(f"  got {len(events)} transactions")
        all_events.extend(events)
        time.sleep(0.25)  # stay comfortably under 5 req/sec free-tier limit

    if not all_events:
        print("No on-chain events fetched — nothing written.")
        return

    out_path = land_events(args.chain, all_events)
    print(f"landed {len(all_events)} on-chain events -> {out_path}")
    maybe_upload_to_s3(out_path, args.chain)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Reproduces the SCD2 demonstration used to verify dim_users_history:
mutates 25 users' kyc_status from 'pending' to 'verified' (bumping
updated_at), so a second `dbt snapshot` run has real change history to
capture instead of a no-op.

Run this between two `dbt snapshot` runs to see it work:

    cd dbt/wallet_lakehouse
    dbt seed --target dev
    dbt snapshot --target dev                    # first version, dbt_valid_to = null for everyone
    python ../../scripts/demo_scd2.py             # mutates 25 users' kyc_status
    dbt seed --target dev --full-refresh          # reload the mutated seed
    dbt snapshot --target dev                    # second version — closes out the old row, opens a new one
    dbt build --target dev                        # rebuilds dim_users / dim_users_history on top

Then query snapshots.users_snapshot (or main_marts.dim_users_history) for
any of the 25 affected user_ids to see both versions with dbt_valid_from/
dbt_valid_to (valid_from/valid_to) populated correctly.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

SEED_PATH = Path(__file__).resolve().parent.parent / "dbt" / "wallet_lakehouse" / "seeds" / "users.csv"


def main(n_users: int = 25) -> None:
    with open(SEED_PATH) as f:
        rows = list(csv.DictReader(f))

    now = datetime.now(timezone.utc).isoformat()
    changed = 0
    for r in rows:
        if r["kyc_status"] == "pending" and changed < n_users:
            r["kyc_status"] = "verified"
            r["updated_at"] = now
            changed += 1

    with open(SEED_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"mutated {changed} users' kyc_status pending -> verified (updated_at={now})")
    print(f"wrote back to {SEED_PATH}")
    print("Now run: dbt seed --target dev --full-refresh && dbt snapshot --target dev")


if __name__ == "__main__":
    main()

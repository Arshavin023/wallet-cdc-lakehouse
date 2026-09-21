#!/usr/bin/env python3
"""
Parses dbt's run_results.json after a `dbt build`/`dbt test` run and posts a
summary of any failed or errored tests to Slack (via an incoming webhook).
With no webhook configured, it just prints the summary and exits non-zero
on failure — which is exactly what CI needs (see .github/workflows/dbt.yml).

This is the "monitoring and alerting that surfaces issues early" the JD
asks for applied to the one thing dbt already tells you in detail: which
tests failed, on which model, and why. It deliberately doesn't try to also
monitor infrastructure (cluster health, job duration, etc.) — that's a
Databricks Jobs / Lakehouse Monitoring concern, out of scope for what a
single dbt project can observe about itself.

Usage:
    dbt build --target dev
    python monitoring/alert_on_dbt_failures.py --run-results target/run_results.json

Env:
    SLACK_WEBHOOK_URL   optional; if unset, alerts just print to stdout/stderr.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests

FAILURE_STATUSES = {"fail", "error", "runtime error"}


def load_results(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def summarize(results: dict) -> tuple[list[dict], dict]:
    failures = []
    counts: dict[str, int] = {}
    for r in results.get("results", []):
        status = (r.get("status") or "").lower()
        counts[status] = counts.get(status, 0) + 1
        if status in FAILURE_STATUSES:
            failures.append(
                {
                    "unique_id": r.get("unique_id"),
                    "status": status,
                    "message": (r.get("message") or "").strip()[:500],
                    "execution_time": round(r.get("execution_time", 0), 2),
                }
            )
    return failures, counts


def format_slack_message(failures: list[dict], counts: dict, invocation_id: str) -> dict:
    lines = [f"*dbt run failed* ({invocation_id})", ""]
    lines.append(
        "Status counts: "
        + ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    )
    lines.append("")
    for f in failures[:10]:
        lines.append(f"• `{f['unique_id']}` — *{f['status']}*: {f['message']}")
    if len(failures) > 10:
        lines.append(f"… and {len(failures) - 10} more")
    return {"text": "\n".join(lines)}


def send_slack_alert(payload: dict, webhook_url: str) -> None:
    resp = requests.post(webhook_url, json=payload, timeout=10)
    resp.raise_for_status()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-results", default="target/run_results.json")
    args = parser.parse_args()

    path = Path(args.run_results)
    if not path.exists():
        print(f"No run_results.json found at {path} — nothing to check.", file=sys.stderr)
        return 0

    results = load_results(path)
    invocation_id = results.get("metadata", {}).get("invocation_id", "unknown")
    failures, counts = summarize(results)

    print(f"dbt run {invocation_id}: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))

    if not failures:
        print("All checks passed — nothing to alert on.")
        return 0

    print(f"\n{len(failures)} failure(s):")
    for f in failures:
        print(f"  [{f['status'].upper()}] {f['unique_id']}: {f['message']}")

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if webhook_url:
        send_slack_alert(format_slack_message(failures, counts, invocation_id), webhook_url)
        print("\nAlert posted to Slack.")
    else:
        print("\nSLACK_WEBHOOK_URL not set — printed above instead of alerting.")

    return 1  # non-zero exit so CI fails the build on real test failures


if __name__ == "__main__":
    sys.exit(main())

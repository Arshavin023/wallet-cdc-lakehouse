#!/usr/bin/env python3
"""
Renders cdc/connector-config.template.json with your host Postgres
connection details and registers it against the Kafka Connect REST API.

Run this once the docker-compose stack is up (`docker compose up -d`) and
Connect is healthy (`curl http://localhost:8083/connectors` should return
`[]` once it's ready — it can take ~30s after `up` to start listening).

Env vars (all optional, shown with their defaults):
    PG_HOST      host.docker.internal   # how the Connect *container* reaches
                                         # your HOST Postgres — not "localhost"
    PG_PORT      5432
    PG_USER      wallet_app
    PG_PASSWORD  wallet_app_pw
    PG_DATABASE  wallets

Usage:
    PG_USER=myuser PG_PASSWORD=mypassword python scripts/register_connector.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from string import Template

import requests

TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "cdc" / "connector-config.template.json"
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
CONNECTOR_NAME = "wallet-postgres-connector"

DEFAULTS = {
    "PG_HOST": "host.docker.internal",
    "PG_PORT": "5432",
    "PG_USER": "wallet_app",
    "PG_PASSWORD": "wallet_app_pw",
    "PG_DATABASE": "wallets",
}


def render_config() -> dict:
    values = {key: os.environ.get(key, default) for key, default in DEFAULTS.items()}
    rendered = Template(TEMPLATE_PATH.read_text()).safe_substitute(values)
    return json.loads(rendered)


def main() -> int:
    config = render_config()
    print(f"Registering connector against a Postgres database at "
          f"{config['config']['database.hostname']}:{config['config']['database.port']}"
          f"/{config['config']['database.dbname']} ...")

    resp = requests.post(
        f"{CONNECT_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(config),
        timeout=30,
    )

    if resp.status_code == 409:
        print("Connector already exists — checking its status instead.")
    elif not resp.ok:
        print(f"Registration failed ({resp.status_code}): {resp.text}", file=sys.stderr)
        return 1
    else:
        print(json.dumps(resp.json(), indent=2))

    status = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=10)
    print("\nConnector status:")
    print(json.dumps(status.json(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

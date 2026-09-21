-- =============================================================================
-- Wallet Activity Lakehouse — OLTP schema (PostgreSQL)
-- Simulates the operational database behind a crypto-wallet app: users,
-- wallets, and the transactions they submit. This is the "source system"
-- that CDC (Debezium) captures and streams into the lakehouse.
--
-- This file has no CREATE DATABASE statement on purpose — psql requires
-- connecting to an existing database before running DDL, so create the
-- database first, then run this against it, both on your HOST machine's
-- own Postgres (not a container):
--
--   createdb wallets
--   psql -d wallets -f oltp/schema.sql
--
-- (swap `wallets` for whatever database name you prefer, and update
-- PG_DATABASE / scripts/simulate_activity.py's --dsn to match)
-- =============================================================================
DO $$
BEGIN
    CREATE ROLE wallet_app WITH PASSWORD 'QUWeIQvD27BYei1';
EXCEPTION WHEN duplicate_object THEN
    NULL;
END
$$;
ALTER ROLE wallet_app WITH REPLICATION;

CREATE TABLE IF NOT EXISTS users (
    user_id         BIGSERIAL PRIMARY KEY,
    email           TEXT NOT NULL UNIQUE,
    country_code    CHAR(2) NOT NULL,
    signup_channel  TEXT NOT NULL,               -- 'organic', 'referral', 'paid_ad'
    kyc_status      TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'verified', 'rejected'
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS wallets (
    wallet_id       BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES users(user_id),
    chain           TEXT NOT NULL,                -- 'ethereum', 'bsc', 'polygon', ...
    address         TEXT NOT NULL,
    wallet_type     TEXT NOT NULL DEFAULT 'non_custodial', -- 'non_custodial', 'custodial'
    is_primary      BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chain, address)
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      BIGSERIAL PRIMARY KEY,
    wallet_id            BIGINT NOT NULL REFERENCES wallets(wallet_id),
    tx_hash               TEXT UNIQUE,             -- null until broadcast/confirmed
    direction              TEXT NOT NULL,            -- 'send', 'receive', 'swap'
    asset_symbol           TEXT NOT NULL,            -- 'ETH', 'USDT', 'BNB', ...
    amount                 NUMERIC(38, 18) NOT NULL,
    usd_value_at_tx        NUMERIC(18, 2),
    fee_native              NUMERIC(38, 18),
    status                  TEXT NOT NULL DEFAULT 'pending', -- 'pending','confirmed','failed'
    submitted_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at             TIMESTAMPTZ,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wallets_user_id ON wallets(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_wallet_id ON transactions(wallet_id);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_transactions_submitted_at ON transactions(submitted_at);

-- Keep updated_at current on every UPDATE — also gives CDC consumers (and the
-- dbt SCD models downstream) a reliable "last changed" column to key off of.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_users_updated_at ON users;
CREATE TRIGGER trg_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_wallets_updated_at ON wallets;
CREATE TRIGGER trg_wallets_updated_at BEFORE UPDATE ON wallets
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_transactions_updated_at ON transactions;
CREATE TRIGGER trg_transactions_updated_at BEFORE UPDATE ON transactions
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- =============================================================================
-- Logical replication setup — this is what Debezium's PostgreSQL connector
-- reads from. REPLICA IDENTITY FULL ensures UPDATE/DELETE events carry the
-- full "before" image, not just the primary key, which matters once you
-- start reconciling wallet balances downstream.
-- =============================================================================
ALTER TABLE users        REPLICA IDENTITY FULL;
ALTER TABLE wallets       REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

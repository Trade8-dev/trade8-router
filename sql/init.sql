-- SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
-- SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
-- SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Router
-- SPDX-License-Identifier: Apache-2.0

-- Trade8:Router - broker sessions, destination links, templates, fees.
--
-- Design: ../router/DESIGN.md §4.2,
-- data-model §2.1 (broker/gateway/destination/msg_template/fee).
-- Credentials are secret references - never values (platform rule).
-- Note: broker/gateway/destination/msg_template live in the Desk schema
-- (shared catalogs, single source of truth); Router owns session runtime
-- state + the journal.

CREATE SCHEMA IF NOT EXISTS router;

-- Outbound message journal (resend/recovery; retention-purged via Cron).
CREATE TABLE IF NOT EXISTS router.message (
 id BIGSERIAL PRIMARY KEY,
 env_id UUID NOT NULL,
 destination_id UUID NOT NULL,
 direction TEXT NOT NULL CHECK (direction IN ('out','in')),
 seq BIGINT NOT NULL,
 msg_type TEXT,
 payload BYTEA NOT NULL,
 at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_t8_routermsg_dest ON router.message (env_id, destination_id, seq);
CREATE INDEX IF NOT EXISTS idx_t8_routermsg_at ON router.message (env_id, at);

-- Sequence lease counters for outbound order ids per destination.
CREATE TABLE IF NOT EXISTS router.session (
 env_id UUID NOT NULL,
 destination_id UUID NOT NULL,
 outgoing_seq BIGINT NOT NULL DEFAULT 0,
 incoming_seq BIGINT NOT NULL DEFAULT 0,
 state TEXT NOT NULL DEFAULT 'down'
 CHECK (state IN ('down','connecting','logged_in')),
 last_heartbeat TIMESTAMPTZ,
 PRIMARY KEY (env_id, destination_id)
);

-- ---------------------------------------------------------------------------
-- Fills (data-model §2.3 - ORDER_TRANSACTION + ORDER_TRANSACTION_FIX_TAG).
-- Router is the SOLE writer (execution normalization); no other service
-- reads these at runtime - Desk's blotter rides the fill events.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS router.order_transaction (
 transaction_id UUID PRIMARY KEY, -- exec id
 env_id UUID NOT NULL,
 order_id UUID NOT NULL REFERENCES desk."order",
 state TEXT NOT NULL CHECK (state IN ('fill','cancel','reject','correction')),
 qty BIGINT NOT NULL,
 price_px BIGINT NOT NULL,
 sec_fee BIGINT, sec_fee_ccy TEXT,
 activity_fee BIGINT, activity_fee_ccy TEXT,
 gateway_fee BIGINT, gateway_fee_ccy TEXT,
 execution_fee BIGINT, execution_fee_ccy TEXT,
 clearing_fee BIGINT, clearing_fee_ccy TEXT,
 liquidity_flag TEXT,
 gateway_order_id TEXT,
 seq_number BIGINT,
 market_date DATE,
 at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_t8_ordertx_order ON router.order_transaction (env_id, order_id, at);
CREATE INDEX IF NOT EXISTS idx_t8_ordertx_mktdate ON router.order_transaction (env_id, market_date);

-- Broker-dialect FIX tags preserved per fill for audit (ORDER_TRANSACTION_FIX_TAG).
CREATE TABLE IF NOT EXISTS router.fill_fix_tags (
 env_id UUID NOT NULL,
 transaction_id UUID NOT NULL REFERENCES router.order_transaction,
 fix_tag INT NOT NULL,
 tag_value TEXT,
 PRIMARY KEY (env_id, transaction_id, fix_tag)
);

-- ---------------------------------------------------------------------------
-- RLS
-- ---------------------------------------------------------------------------
DO $$
DECLARE tbl TEXT;
DECLARE pol TEXT;
BEGIN
 FOREACH tbl IN ARRAY ARRAY[
 'router.message', 'router.session',
 'router.order_transaction', 'router.fill_fix_tags'
 ] LOOP
 BEGIN
 pol := replace(split_part(tbl, '.', 2), '"', '');
 EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
 EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
 -- Suffix BEFORE %I quoting: `limit` alone is fine but the concatenation
 -- `"limit"_select` is invalid SQL - build 'limit_select' unquoted
 -- (desk-store DO-block memory: reserved-word table names are the
 -- classic failure class).
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR SELECT USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_select', tbl);
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR INSERT WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_insert', tbl);
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR UPDATE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_update', tbl);
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR DELETE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_delete', tbl);
 EXCEPTION WHEN OTHERS THEN
 RAISE NOTICE 'RLS setup failed for %: %', tbl, SQLERRM;
 END;
 END LOOP;
END $$;

-- Local outbox + audit journal (DBX §6.7 / §6.8 - shell-generated DDL).
CREATE TABLE IF NOT EXISTS router.outbox (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 event_type TEXT NOT NULL,
 stream_family TEXT NOT NULL,
 payload JSONB NOT NULL,
 publisher_id_token TEXT,
 publisher_subject TEXT,
 relayed BOOLEAN NOT NULL DEFAULT false,
 relayed_at TIMESTAMPTZ,
 relay_attempts INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_outbox_unrelayed
 ON router.outbox (created_at) WHERE relayed = false;
ALTER TABLE router.outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE router.outbox FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS outbox_select ON router.outbox;
CREATE POLICY outbox_select ON router.outbox FOR SELECT
 USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
DROP POLICY IF EXISTS outbox_insert ON router.outbox;
CREATE POLICY outbox_insert ON router.outbox FOR INSERT
 WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
DROP POLICY IF EXISTS outbox_update ON router.outbox;
CREATE POLICY outbox_update ON router.outbox FOR UPDATE
 USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
CREATE OR REPLACE FUNCTION router.router_notify_outbox() RETURNS TRIGGER
LANGUAGE plpgsql AS $fn$
BEGIN
 PERFORM pg_notify('router_outbox_changed', '');
 RETURN NEW;
END;
$fn$;
DROP TRIGGER IF EXISTS outbox_notify_trigger ON router.outbox;
CREATE TRIGGER outbox_notify_trigger
 AFTER INSERT ON router.outbox
 FOR EACH ROW
 EXECUTE FUNCTION router.router_notify_outbox();

CREATE TABLE IF NOT EXISTS router.audit_journal (
 id UUID PRIMARY KEY,
 env_id UUID NOT NULL,
 action TEXT NOT NULL,
 target_resource_type TEXT,
 target_resource_id TEXT,
 actor_user_id TEXT,
 actor_subject_type TEXT,
 severity TEXT NOT NULL DEFAULT 'info',
 correlation_id TEXT,
 before_state JSONB,
 after_state JSONB,
 details JSONB,
 pushed BOOLEAN NOT NULL DEFAULT false,
 pushed_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_router_audit_journal_unpushed
 ON router.audit_journal (created_at) WHERE pushed = false;
ALTER TABLE router.audit_journal ENABLE ROW LEVEL SECURITY;
ALTER TABLE router.audit_journal FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS audit_journal_dml ON router.audit_journal;
CREATE POLICY audit_journal_dml ON router.audit_journal
 USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())
 WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id());

-- ---------------------------------------------------------------------------
-- Per-service role grants + search_path (bank8-ledger pattern; the CLI
-- creates the role BEFORE this SQL - grant-before-role incident rule).
-- Grants live in db trade8 (this database); the scope→role map row is
-- upserted by `vertical enroll` from --section=pg_roles.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trade8_router_service') THEN
  ALTER ROLE trade8_router_service SET search_path = router, public;
  GRANT USAGE ON SCHEMA router TO trade8_router_service;
  GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA router TO trade8_router_service;
  GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA router TO trade8_router_service;
  ALTER DEFAULT PRIVILEGES FOR ROLE sentin8 IN SCHEMA router
   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO trade8_router_service;
  ALTER DEFAULT PRIVILEGES FOR ROLE sentin8 IN SCHEMA router
   GRANT USAGE, SELECT ON SEQUENCES TO trade8_router_service;
 END IF;
END $$;
-- Declared cross-schema reads (SELECT-only) - mirrors registration.rs
-- PgRole.cross_schema_grants; `vertical enroll` owns their application,
-- this block only re-asserts them idempotently after a role recreation.
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trade8_router_service') THEN
  GRANT USAGE ON SCHEMA desk TO trade8_router_service;
  GRANT SELECT ON desk.destination, desk.gateway, desk.market_gateway_destination,
       desk.office_broker, desk.msg_template TO trade8_router_service;
 END IF;
END $$;

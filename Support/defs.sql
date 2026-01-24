-- Batcher V2: assets + requests
-- Assumes PostgreSQL.
-- We use pgcrypto for gen_random_uuid(); switch to uuid-ossp if that’s your standard.

CREATE SCHEMA IF NOT EXISTS batcher;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Each object in S3 is addressed by a UUID "locator".
-- This table is the metadata index for those locators.
CREATE TABLE IF NOT EXISTS batcher.s3_objects (
  locator       uuid PRIMARY KEY,
  kind          text NOT NULL,              -- e.g. 'template', 'source', 'raw_result', 'final_result'
  bytes         bigint NOT NULL,
  sha256        bytea,
  content_type  text,
  created_at    timestamptz NOT NULL DEFAULT now()
);

-- Template versions (multiple locators per template_name allowed).
CREATE TABLE IF NOT EXISTS batcher.asset_templates (
  template_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  template_name  text NOT NULL,
  s3_locator     uuid NOT NULL REFERENCES batcher.s3_objects(locator),
  created_at     timestamptz NOT NULL DEFAULT now(),
  UNIQUE (template_name, s3_locator)
);

-- Source versions (multiple locators per source_name allowed).
CREATE TABLE IF NOT EXISTS batcher.asset_sources (
  source_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_name  text NOT NULL,
  s3_locator   uuid NOT NULL REFERENCES batcher.s3_objects(locator),
  created_at   timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_name, s3_locator)
);

-- A "production" groups the requests created from (template, source).
CREATE TABLE IF NOT EXISTS batcher.productions (
  production_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  production_name  text NOT NULL UNIQUE,
  template_id      uuid NOT NULL REFERENCES batcher.asset_templates(template_id),
  source_id        uuid NOT NULL REFERENCES batcher.asset_sources(source_id),
  created_at       timestamptz NOT NULL DEFAULT now()
);

-- The request state machine for V2 (as requested).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'batcher' AND t.typname = 'request_state'
  ) THEN
    CREATE TYPE batcher.request_state AS ENUM ('entered', 'submitted', 'cancelled', 'completed');
  END IF;
END
$$;

-- One row per "request" that will later be submitted to the AI batch service.
CREATE TABLE IF NOT EXISTS batcher.requests (
  request_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  production_id       uuid NOT NULL REFERENCES batcher.productions(production_id) ON DELETE CASCADE,
  item_index          integer NOT NULL,                    -- 1..N within a production
  item_meta           jsonb NOT NULL DEFAULT '{}'::jsonb,  -- parsed TOML header as JSON
  request_text        text NOT NULL,                       -- rendered template output
  -- state               batcher.request_state NOT NULL DEFAULT 'entered',

  -- Fields useful for later pipeline steps (optional but usually needed):
  provider_batch_id   text,
  provider_request_id text,
  raw_result_locator  uuid REFERENCES batcher.s3_objects(locator),
  final_result_locator uuid REFERENCES batcher.s3_objects(locator),

  submit_claimed_until timestamptz,
  submit_claimed_by text,
  submit_claim_token uuid,

  provider_batch_uuid uuid,

  poll_claimed_until timestamptz,
  poll_claimed_by text,
  poll_claim_token uuid,

  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),

  UNIQUE (production_id, item_index)
);

CREATE INDEX IF NOT EXISTS requests_by_prod_state ON batcher.requests (production_id, state);
CREATE INDEX IF NOT EXISTS requests_submit_claim_idx ON batcher.requests (state, submit_claimed_until);
CREATE INDEX IF NOT EXISTS requests_by_provider_batch_uuid ON batcher.requests (provider_batch_uuid);
CREATE INDEX IF NOT EXISTS requests_poll_claim_idx ON batcher.requests (state, poll_claimed_until);

-- Helpful index for batch->requests updates
CREATE INDEX IF NOT EXISTS requests_by_batch_completed ON batcher.requests (provider_batch_uuid, state);


-- “Transaction for requests”: append-only state/event history.
CREATE TABLE IF NOT EXISTS batcher.request_events (
  event_id     bigserial PRIMARY KEY,
  request_id   uuid NOT NULL REFERENCES batcher.requests(request_id) ON DELETE CASCADE,
  state        batcher.request_state NOT NULL,
  details      jsonb NOT NULL DEFAULT '{}'::jsonb,
  occurred_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS request_events_by_req_time ON batcher.request_events (request_id, occurred_at DESC);


create table if not exists batcher.request_results (
  request_fk uuid not null references batcher.requests(request_id) on delete cascade
  , batch_fk uuid REFERENCES batcher.batches(uid) ON DELETE CASCADE
  , created_at timestamptz NOT NULL DEFAULT now()
  , metadata jsonb not null default '{}'::jsonb
  , content text
  , primary key (batch_fk, request_fk)
);


-- Track the batches of requests:
create table if not exists batcher.batches (
  uid uuid not null primary key
  , provider_batch_id text not null
  , poll_claimed_until timestamptz
  , poll_claimed_by text
  , poll_claim_token uuid
  , created_at timestamptz not null default now()
);

CREATE INDEX IF NOT EXISTS batches_poll_claim_idx ON batcher.batches (poll_claimed_until);

create table if not exists batcher.batch_events (
  event_id bigserial primary key
  , batch_fk uuid not null references batcher.batches(uid) on delete cascade
  , event_type text not null
  , details jsonb not null default '{}'::jsonb
  , occurred_at timestamptz not null default now()
);

CREATE INDEX IF NOT EXISTS batch_events_by_batch_time ON batcher.batch_events (batch_fk, occurred_at DESC);


create table if not exists batcher.batch_requests (
  request_fk uuid not null references batcher.requests(request_id) on delete cascade
  , batch_fk uuid not null references batcher.batches(uid) on delete cascade
  , created_at timestamptz not null default now()
  , primary key (request_fk, batch_fk)
);

CREATE INDEX IF NOT EXISTS batch_requests_by_batch ON batcher.batch_requests (batch_fk);


-- Durable outbox for Engine.Fetch (one row per ready batch).
CREATE TABLE IF NOT EXISTS batcher.fetch_outbox (
  batch_fk uuid not null references batcher.batches(uid) on delete cascade
  , metadata jsonb not null default '{}'::jsonb
  , fetch_claimed_until timestamptz
  , fetch_claimed_by text
  , fetch_claim_token uuid
  , created_at timestamptz NOT NULL DEFAULT now()
  , primary key (batch_fk)
);

CREATE INDEX IF NOT EXISTS fetch_outbox_claim_idx ON batcher.fetch_outbox (fetch_claimed_until);


CREATE INDEX IF NOT EXISTS fetch_outbox_claim_idx ON batcher.fetch_outbox (fetch_claimed_until);

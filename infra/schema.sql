CREATE TABLE IF NOT EXISTS events (
  event_id text PRIMARY KEY,
  payload jsonb NOT NULL,
  status text NOT NULL CHECK (status IN ('received','sync_crm','queue_human_review','dead_letter')),
  created_at double precision NOT NULL DEFAULT extract(epoch from now())
);
CREATE TABLE IF NOT EXISTS decisions (
  event_id text PRIMARY KEY REFERENCES events(event_id),
  body jsonb NOT NULL,
  created_at double precision NOT NULL DEFAULT extract(epoch from now())
);
CREATE TABLE IF NOT EXISTS approvals (
  id bigserial PRIMARY KEY,
  event_id text NOT NULL REFERENCES events(event_id),
  state text NOT NULL CHECK (state IN ('pending','approved','rejected')),
  reviewer text,
  created_at double precision NOT NULL DEFAULT extract(epoch from now())
);
CREATE TABLE IF NOT EXISTS dead_letters (
  id bigserial PRIMARY KEY,
  event_id text NOT NULL,
  error text NOT NULL,
  created_at double precision NOT NULL DEFAULT extract(epoch from now())
);
CREATE TABLE IF NOT EXISTS audit (
  id bigserial PRIMARY KEY,
  event_id text NOT NULL,
  action text NOT NULL,
  detail jsonb NOT NULL,
  created_at double precision NOT NULL DEFAULT extract(epoch from now())
);
CREATE INDEX IF NOT EXISTS approvals_pending_idx ON approvals(state) WHERE state='pending';

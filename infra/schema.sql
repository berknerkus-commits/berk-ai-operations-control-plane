CREATE TABLE operations_events (
  event_id text PRIMARY KEY,
  account_id text NOT NULL,
  payload jsonb NOT NULL,
  status text NOT NULL CHECK (status IN ('received','sync_crm','queue_human_review','dead_letter')),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE operation_decisions (
  event_id text PRIMARY KEY REFERENCES operations_events(event_id),
  intent text NOT NULL,
  action text NOT NULL,
  requires_approval boolean NOT NULL,
  model_version text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE approval_queue (
  id bigserial PRIMARY KEY,
  event_id text NOT NULL REFERENCES operations_events(event_id),
  state text NOT NULL CHECK (state IN ('pending','approved','rejected')),
  reviewer text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE dead_letters (
  id bigserial PRIMARY KEY,
  event_id text NOT NULL,
  error_code text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX operations_events_account_idx ON operations_events(account_id);
CREATE INDEX approval_queue_pending_idx ON approval_queue(state) WHERE state='pending';

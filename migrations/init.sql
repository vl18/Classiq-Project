CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS tasks (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  qc TEXT NOT NULL,
  shots INTEGER NOT NULL DEFAULT 1024,
  status TEXT NOT NULL CHECK (status IN ('pending','processing','completed','failed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ NULL,
  completed_at TIMESTAMPTZ NULL,

  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,

  locked_by TEXT NULL,
  lease_expires_at TIMESTAMPTZ NULL,

  next_run_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  result JSONB NULL,
  last_error TEXT NULL,
  metrics JSONB NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_next_run ON tasks (status, next_run_at);
CREATE INDEX IF NOT EXISTS idx_tasks_processing_lease ON tasks (status, lease_expires_at);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS trg_set_updated_at ON tasks;
CREATE TRIGGER trg_set_updated_at
BEFORE UPDATE ON tasks
FOR EACH ROW
EXECUTE PROCEDURE set_updated_at();

CREATE TYPE task_status AS ENUM ('queued', 'running', 'failed', 'succeeded');

CREATE TABLE tasks (
    id bigserial PRIMARY KEY,
    name text NOT NULL,
    type text NOT NULL,
    status task_status NOT NULL DEFAULT 'queued',
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    attempts integer NOT NULL DEFAULT 0,
    locked_at timestamptz,
    last_error text
);

CREATE INDEX tasks_dequeue_idx
ON tasks (created_at, id)
WHERE status = 'queued' AND locked_at IS NULL;

CREATE INDEX tasks_reaper_idx
ON tasks (locked_at, id)
WHERE status = 'running';

CREATE TABLE task_events (
    id bigserial PRIMARY KEY,
    task_id bigint NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    status task_status NOT NULL DEFAULT 'queued',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX task_events_task_id_created_at_idx
ON task_events (task_id, created_at)
INCLUDE (status);
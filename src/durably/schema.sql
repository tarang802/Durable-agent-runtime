-- durably schema. Applied by `durably init-db`; safe to run more than once.

CREATE TABLE IF NOT EXISTS workflows (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    type            text        NOT NULL,
    input           jsonb,      -- NULL when started without input
    status          text        NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    -- Fencing token: bumped on every workflow-task claim. Every history write is
    -- conditioned on it, so a stale worker's write matches zero rows (G2).
    epoch           bigint      NOT NULL DEFAULT 0,
    lease_owner     text,
    lease_until     timestamptz,
    -- Next history sequence number, handed out by the append transaction.
    next_seq        bigint      NOT NULL DEFAULT 1,
    -- A duplicate start with the same key returns the existing workflow.
    idempotency_key text        UNIQUE,
    result          jsonb,
    error           text,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
    workflow_id uuid        NOT NULL REFERENCES workflows (id),
    seq         bigint      NOT NULL,
    type        text        NOT NULL CHECK (type IN (
                    'WorkflowStarted', 'ActivityScheduled', 'ActivityCompleted',
                    'ActivityFailed', 'TimerStarted', 'TimerFired', 'SignalReceived',
                    'MarkerRecorded', 'WorkflowCompleted', 'WorkflowFailed')),
    payload     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    -- G1: one event per position in a workflow's history.
    PRIMARY KEY (workflow_id, seq)
);

-- G1: history is append-only. The database itself refuses edits and deletes, so no
-- code path (or bug) can rewrite what replay depends on.
CREATE OR REPLACE FUNCTION events_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'events is append-only (% rejected)', TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS events_append_only ON events;
CREATE TRIGGER events_append_only
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_append_only();

CREATE TABLE IF NOT EXISTS tasks (
    id          bigserial   PRIMARY KEY,
    workflow_id uuid        NOT NULL REFERENCES workflows (id),
    kind        text        NOT NULL
                CHECK (kind IN ('workflow_task', 'activity_task', 'timer')),
    visible_at  timestamptz NOT NULL DEFAULT now(),
    attempt     integer     NOT NULL DEFAULT 0,
    claimed_by  text,
    lease_until timestamptz,
    payload     jsonb       NOT NULL DEFAULT '{}'::jsonb
);

-- Claim scans due tasks of one kind in visibility order.
CREATE INDEX IF NOT EXISTS tasks_due ON tasks (kind, visible_at);

-- At most one unclaimed workflow task per workflow: every new event "wakes" the
-- workflow with INSERT ... ON CONFLICT DO NOTHING, so wake-ups coalesce.
CREATE UNIQUE INDEX IF NOT EXISTS tasks_one_pending_workflow_task
    ON tasks (workflow_id) WHERE kind = 'workflow_task' AND claimed_by IS NULL;

-- G4: at most one terminal event per workflow, enforced by the database.
CREATE UNIQUE INDEX IF NOT EXISTS events_one_terminal
    ON events (workflow_id) WHERE type IN ('WorkflowCompleted', 'WorkflowFailed');

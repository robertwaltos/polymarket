CREATE TABLE IF NOT EXISTS strategy_orchestrator_snapshot_history (
    id BIGSERIAL PRIMARY KEY,
    org_id UUID NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
    snapshot_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orchestrator_snapshot_history_org_created
    ON strategy_orchestrator_snapshot_history(org_id, created_at DESC);


CREATE TABLE IF NOT EXISTS research_runs (
    id UUID PRIMARY KEY,
    org_id UUID NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
    strategy_id UUID NULL,
    status TEXT NOT NULL CHECK (status IN ('pass', 'fail', 'manual_review')),
    dataset_snapshot_id TEXT NOT NULL,
    feature_set_hash TEXT NOT NULL,
    code_version TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    baseline_run_id UUID NULL,
    metrics_json JSONB NOT NULL,
    acceptance_json JSONB NOT NULL,
    artifact_uri TEXT NULL,
    notes TEXT NULL,
    record_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_runs_org_created_at
    ON research_runs(org_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_runs_org_strategy_created_at
    ON research_runs(org_id, strategy_id, created_at DESC);

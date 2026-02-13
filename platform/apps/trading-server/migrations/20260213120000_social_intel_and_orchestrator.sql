CREATE TABLE IF NOT EXISTS social_intel_claims (
    id UUID PRIMARY KEY,
    org_id UUID NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
    source TEXT NOT NULL CHECK (source IN ('x', 'reddit', 'manual')),
    query TEXT NULL,
    author TEXT NULL,
    community TEXT NULL,
    posted_at TIMESTAMPTZ NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    url TEXT NULL,
    text_body TEXT NOT NULL,
    brag_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    hype_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    evidence_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    strategy_tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    dedup_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(org_id, dedup_key)
);

CREATE INDEX IF NOT EXISTS idx_social_intel_claims_org_captured
    ON social_intel_claims(org_id, captured_at DESC);

CREATE TABLE IF NOT EXISTS strategy_orchestrator_snapshots (
    org_id UUID PRIMARY KEY REFERENCES orgs(id) ON DELETE CASCADE,
    snapshot_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


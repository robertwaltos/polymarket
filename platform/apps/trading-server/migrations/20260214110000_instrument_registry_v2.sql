CREATE TABLE IF NOT EXISTS canonical_instrument_registry (
    id UUID PRIMARY KEY,
    org_id UUID NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
    canonical_event_id TEXT NOT NULL,
    canonical_outcome_id TEXT NOT NULL,
    canonical_event_title TEXT NULL,
    outcome_label TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    lifecycle TEXT NOT NULL CHECK (lifecycle IN ('new', 'tradable', 'watchlist_only', 'deprecated')),
    manual_override BOOLEAN NOT NULL DEFAULT FALSE,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(org_id, venue, symbol)
);

CREATE INDEX IF NOT EXISTS idx_canonical_registry_org_event
    ON canonical_instrument_registry(org_id, canonical_event_id);

CREATE INDEX IF NOT EXISTS idx_canonical_registry_org_lifecycle
    ON canonical_instrument_registry(org_id, lifecycle, updated_at DESC);


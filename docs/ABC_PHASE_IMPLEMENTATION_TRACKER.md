# Phase A/B/C Implementation Tracker

Source plan: `docs/AUTONOMOUS_TRADING_RECOMMENDATIONS.md`

## Working Rules

1. This file is the continuity source of truth for Phase A/B/C implementation.
2. Every completed engineering milestone must update:
   - milestone status in the checklist tables
   - the `Progress Log` section (append-only, newest first)
3. Status values:
   - `not_started`
   - `in_progress`
   - `done`
   - `blocked`

## Phase A (0-4 weeks): Production-grade autonomous safety + observability

| ID | Milestone | Status | Scope |
|---|---|---|---|
| A1 | Hierarchical risk controls | done | Org/strategy/instrument guardrails, rate limits, spread/adverse selection caps, cooldowns |
| A2 | Live guard + SLO monitoring | done | Quote freshness, rejection-rate and latency SLO checks, automated live blocking on SLO breach |
| A3 | Event-sourced telemetry for autonomous loops | done | Decision trace persistence + telemetry metrics endpoints |
| A4 | Agent audit explainability | done | Rich metadata (`agent_id`, `policy_version`, `feature_hash`, `confidence`) + post-trade explain endpoint |

## Phase B (4-8 weeks): Micro-execution + multi-instrument alpha plumbing

| ID | Milestone | Status | Scope |
|---|---|---|---|
| B1 | Tiny-increment execution algorithms | done | TWAP/VWAP/POV/Iceberg/Passive-maker slicing with adaptive child sizing |
| B2 | Cross-market signal layer | done | Event ontology scaffolding + cross-venue dislocation scanners |
| B3 | Instrument normalization + registry v2 | done | Canonical event/outcome registry with lifecycle states and overrides |
| B4 | Continuous 24/7 opportunity scheduler | done | EV-prioritized queueing with compute budget and settlement-time weighting |

## Phase C (8-12 weeks): Autonomous research + controlled self-improvement

| ID | Milestone | Status | Scope |
|---|---|---|---|
| C1 | Research pipeline automation | done | Reproducible experiment artifacts, dataset snapshots, acceptance gates |
| C2 | Policy ensemble + rollout governance | done | Champion/challenger + ensemble policy wiring |
| C3 | Federated/distributed learning | done | Signed parameter exchange, version conflict handling, secure aggregation |
| C4 | Automated capital allocator | done | Risk-adjusted dynamic budget assignment with non-bypassable risk constraints |

## Progress Log

### 2026-02-14

- Implemented `C2` ensemble-policy wiring on top of champion/challenger governance:
  - `POST /v1/orchestrator/agents/:agent_id/ensemble-policy`
  - strategy-level microstructure/event/risk policy component scores + normalized weights
  - ensemble score folded into orchestrator ranking and allocation decision inputs
- Validation pass completed after C2 implementation:
  - `cargo check -p trading-server`
  - `cargo test -p trading-server`
  - `cargo test -p oms`
  - `cargo test -p risk`
  - all passing
- Implemented `C4` automated capital allocator:
  - `POST /v1/orchestrator/capital-allocate`
  - regime-aware risk-adjusted edge scoring (`normal`, `thin_liquidity`, `news_shock`, `trending`)
  - enforced hard caps via orchestrator capital budget + per-strategy maximum allocation constraints
- Implemented `B2` cross-market signal layer:
  - `GET /v1/signals/ontology` for normalized event/outcome ontology views (with domain + settlement windows)
  - `GET /v1/signals/dislocations` for cross-venue dislocation scans over canonical event/outcome pairs
- Implemented `C1` research pipeline automation:
  - `POST /v1/research/runs`, `GET /v1/research/runs`, `GET /v1/research/runs/:run_id`
  - acceptance gates for sharpe/drawdown/trades/return and baseline sharpe delta
  - migration `20260214130000_research_runs.sql` with persistent research run storage
- Validation pass completed after B2/C4/C1 implementation:
  - `cargo check -p trading-server`
  - `cargo test -p trading-server`
  - `cargo test -p oms`
  - `cargo test -p risk`
  - all passing
- Validation pass completed after A1-A4 implementation:
  - `cargo test -p trading-server`
  - `cargo test -p oms`
  - `cargo test -p risk`
  - all passing
- Implemented `A4` explainability workflow:
  - `GET /v1/orders/:request_id/explain` for post-trade signal/risk/execution narrative
  - API order intake now accepts arbitrary metadata map to carry agent/policy/feature/confidence context into audit traces
  - postgres lookup by `request_id` added for explain endpoint fidelity
- Implemented `A3` telemetry summary endpoint:
  - `GET /v1/telemetry/summary` exposes fill-efficiency, signal-confidence, slippage proxy, and edge-decay proxy over configurable lookback windows
  - metrics derive from persisted order audit + decision-trace records (DB or memory fallback)
- Implemented `A2` live guard SLO monitoring and enforcement:
  - configurable rolling SLO window + sample minimums
  - live rejection-rate threshold checks
  - p95 health-latency threshold checks
  - SLO breach propagation into live authorization blocks and guard snapshot visibility
- Implemented `A1` hierarchical pre-trade guardrails in trading-server:
  - org/strategy/instrument order-rate limits
  - strategy cooldown escalation on repeated rate-limit or microstructure guard trips
  - spread-cross and adverse-selection threshold checks from order metadata
  - enforcement wired into both direct order submissions and sliced child-order submissions
- Initialized A/B/C tracking document and normalized milestone statuses against the current codebase.
- Carried forward completed baseline milestones from earlier work:
  - `B1` slicing engine
  - `B3` instrument registry v2
  - `B4` EV scheduler
  - `C3` federated protocol
- Opened active work for this implementation batch: `A1`, `A2`, `A3`, `A4`.

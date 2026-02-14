# Autonomous Trading Development Recommendations (24/7 Multi-Instrument Focus)

## Objective

This document proposes a concrete next-phase build plan for full automation of research and trading with autonomous agents, emphasizing:

- 24/7 operation
- very small incremental orders
- multi-instrument and emerging prediction markets
- safety-first live execution controls

## Current Strengths to Build On

1. **Clear execution-mode model (`paper` / `shadow` / `live`)** already exists and is enforced through the OMS pipeline.
2. **Unified venue adapter interface** (`health`, `quote`, `balances`, `place_live_order`) supports multi-venue expansion.
3. **Per-org OMS isolation + auditing** provides a strong base for autonomous operation in multi-tenant contexts.
4. **Initial strategy orchestration and social-intel ingestion** are present and can be extended into fully agentic loops.
5. **Backtest crate exists** and can be upgraded into realistic simulation parity for live readiness.

## Highest-Priority Gaps (for autonomous, high-frequency micro-trading)

### 1) Execution quality and market microstructure modeling
- Current simulator/backtest reports only basic counts and lacks slippage/latency/order-book dynamics.
- There is no explicit queue-position model or micro-fill logic for tiny-increment execution.

### 2) Risk engine depth for always-on automation
- Risk limits are currently static and simple (max order notional/position notional/live toggle).
- Missing volatility-aware sizing, per-strategy drawdown brakes, and venue-specific dynamic throttles.

### 3) Portfolio and accounting realism
- Portfolio state updates fills/cash but lacks full realized/unrealized PnL decomposition and fee-funding granularity by venue/instrument.
- Needed for continuous capital allocation among autonomous agents.

### 4) Research-to-production model lifecycle
- RL currently uses a compact tabular state/action design and offline simulation stub, useful for scaffolding but insufficient for production policy learning.
- Need feature store + offline/online evaluation + safe rollout gates.

### 5) Coverage of emerging prediction-market venues and contracts
- Domain supports prediction markets and includes `Venue::Polymarket`, but adapter depth and instrument normalization need expansion for broader market coverage and niche contracts.

## Recommended Feature Development Roadmap

## Phase A (0-4 weeks): Production-grade autonomous safety + observability

1. **Introduce hierarchical risk controls**
   - Add account/org/strategy/instrument guardrails.
   - Add max order-rate, max adverse selection, and max spread-cross controls for micro-ordering.
   - Add automated cooldown/re-enable policies per strategy and per venue.

2. **Strengthen live guard + SLO monitoring**
   - Add explicit health SLOs (quote freshness, rejection-rate, latency percentiles).
   - Promote live guard to block orders when stale market data or adapter instability is detected.

3. **Event-sourced telemetry for autonomous loops**
   - Persist decision traces: signal snapshot -> risk decisions -> order route -> fill outcomes.
   - Add metrics for per-strategy edge decay, fill efficiency, and slippage drift.

4. **Agent audit explainability**
   - Extend order metadata to include agent id, policy version, feature hash, and confidence.
   - Build post-trade explain endpoint for compliance/debugging.

## Phase B (4-8 weeks): Micro-execution + multi-instrument alpha plumbing

1. **Execution algorithms for tiny-increment trading**
   - Implement TWAP/VWAP/POV/iceberg/queue-aware passive-maker logic.
   - Add adaptive child-order sizing based on spread, depth proxy, and fill probability.

2. **Cross-market signal layer**
   - Build event-level normalized ontology across prediction markets (politics, sports, macro, weather, crypto events).
   - Add price-dislocation and implied-probability spread scanners across venues.

3. **Instrument normalization and registry v2**
   - Canonical event-id + outcome mapping with confidence scores and manual override workflow.
   - Add instrument lifecycle states (new, tradable, watchlist-only, deprecated).

4. **Continuous opportunity scheduler (24/7)**
   - Prioritize scans by expected value / compute budget.
   - Add timezone-aware event clocks and settlement-risk windows.

## Phase C (8-12 weeks): Autonomous research + controlled self-improvement

1. **Research pipeline automation**
   - Feature store + dataset snapshots + reproducible experiment runs.
   - Agent-generated strategy variants with auto-backtest + acceptance gates.

2. **Policy ensemble architecture**
   - Replace single tabular RL policy with ensemble:
     - short-horizon microstructure policy
     - medium-horizon event probability policy
     - risk/meta-policy allocator
   - Use champion/challenger rollout with shadow deployment first.

3. **Federated / distributed learning implementation**
   - Replace sync stub with signed parameter exchange and version conflict handling.
   - Support opt-in peer clusters and secure aggregation.

4. **Automated capital allocator**
   - Dynamic budget assignment by risk-adjusted edge, drawdown state, and market regime.
   - Hard constraints from central risk engine remain non-bypassable.

## Advanced Algorithm Recommendations

1. **Prediction-market specific market-making**
   - Inventory-skewed quoting on yes/no ladders.
   - Outcome-correlation hedging across related contracts.

2. **Meta-order slicing for low-impact entry/exit**
   - Dynamic urgency model with volatility and spread sensitivity.
   - Opportunistic passive posting before aggressive crossing.

3. **Regime detection + policy switching**
   - Detect liquidity regimes (normal/thin/news shock).
   - Route to different execution policies by regime.

4. **Multi-agent architecture**
   - Specialized agents by domain (politics, macro, sports, weather, crypto).
   - Portfolio-level supervisor agent for risk/capital arbitration.

5. **Online learning with strict guardrails**
   - Bounded parameter update windows.
   - Automatic rollback when live KPIs breach thresholds.

## Platform Improvements (Engineering)

1. **State durability and recovery**
   - Move in-memory orchestrator/rate-limiter state into durable storage/redis for crash-safe continuity.
   - Add deterministic replay from audit/event logs.

2. **Concurrency and throughput**
   - Introduce bounded queues and backpressure between scan/signal/risk/execution stages.
   - Add per-venue actor/task isolation to prevent cascading failures.

3. **Data quality controls**
   - Market-data freshness checks, duplicate suppression, and schema/version validation.
   - Confidence scoring for external/social intelligence inputs.

4. **Testing hardening**
   - Property tests for risk and portfolio invariants.
   - Chaos tests for adapter downtime, stale quotes, and partial-fill storms.

## Suggested KPI Framework (for autonomous operations)

- **Alpha quality**: expected edge vs realized edge decay
- **Execution quality**: slippage bps, passive fill ratio, rejection/timeout rate
- **Risk health**: limit breaches prevented, drawdown per strategy, kill-switch activations
- **Automation reliability**: mean time to recover, stale-data incidents, missed opportunities
- **Capital efficiency**: return on allocated capital by agent and by venue

## Immediate Next 10 Tickets

1. Dynamic risk limits (volatility + liquidity aware)
2. Order-slicing engine for micro-increment execution
3. Canonical event/outcome registry for cross-venue prediction contracts
4. Quote freshness and staleness gate in pre-trade checks
5. Realistic backtest simulator (latency/slippage/partial fills)
6. Decision-trace schema (signal->risk->execution) with query endpoints
7. Strategy champion/challenger shadow rollout pipeline
8. Durable orchestrator state with replayable snapshots
9. Federated learning protocol implementation (replace stub)
10. Autonomous scanner scheduler with EV-prioritized queueing

## Guiding Principle

Autonomy should scale only behind deterministic safety layers. Keep the architecture **agentic at the edge, conservative at the core**:

- aggressive in opportunity discovery,
- disciplined in execution,
- uncompromising in risk and auditability.

# Multi-Asset Trading Platform Roadmap

## 1) Product Scope

Build a cross-platform, multi-user, API-first trading platform with:

- Multi-venue connectivity for prediction markets, crypto, stocks, options, futures, metals.
- Three execution modes:
  - `paper`: fully simulated broker + wallet.
  - `shadow`: real market/account reads + simulated execution (no live order placement).
  - `live`: real execution.
- Strategy automation, risk controls, observability, and self-healing operations.
- Desktop UI (light/dark) and external API for third-party integrations.

## 2) Technology Direction

- Core runtime: Rust (`tokio`, `axum`, `serde`, `sqlx`, `tracing`).
- Data/API: Postgres + Redis.
- Messaging: NATS (event bus) for low-latency internal pub/sub.
- UI: Tauri + web frontend (shared design system).
- Packaging: native desktop builds for Windows and macOS.
- Deployment: local workstation first, then dedicated host.

## 3) Milestones

## M0 - Foundation + Repo Bootstrap (Week 1)
Goal: create clean architecture boundaries and compile-safe skeleton.

- `TKT-001` Create new Rust workspace for greenfield platform.
- `TKT-002` Define canonical domain model (Instrument, Quote, Order, Fill, Position, Account).
- `TKT-003` Define execution mode semantics (`paper`, `shadow`, `live`).
- `TKT-004` Add environment/config loader with strict validation.
- `TKT-005` Add structured logging and trace IDs.

Exit criteria:
- Workspace compiles; modules wired with trait-based interfaces.
- Mode toggles are globally enforced.

## M1 - Connectors v1 (Week 2-3)
Goal: first production connector path and normalized order interface.

- `TKT-101` Implement connector trait contracts (`health`, `quotes`, `balances`, `place_order`).
- `TKT-102` Kalshi adapter skeleton (auth + market data + place order path).
- `TKT-103` IBKR adapter skeleton (auth/session + market data + place order path).
- `TKT-104` Coinbase adapter skeleton (auth + market data + place order path).
- `TKT-105` Unified symbol mapping + instrument registry.
- `TKT-106` Connector capability matrix and fallback behavior.

Exit criteria:
- One end-to-end flow from normalized order to connector API (paper + shadow + live gate).

## M2 - OMS + Portfolio Ledger (Week 4)
Goal: robust state model for orders, fills, positions, and PnL.

- `TKT-201` OMS service with idempotency keys and retry-safe commands.
- `TKT-202` Fill ingestion and position updates.
- `TKT-203` Portfolio service (realized/unrealized PnL, fees, exposure).
- `TKT-204` Reconciliation jobs (broker state vs internal ledger).
- `TKT-205` Persistent audit trail schema.

Exit criteria:
- Deterministic order lifecycle with full event history.

## M3 - Risk Engine v1 (Week 5)
Goal: central pre-trade and portfolio risk controls.

- `TKT-301` Pre-trade checks (max notional, max position, max leverage).
- `TKT-302` Portfolio risk checks (concentration, asset-class caps, daily DD limits).
- `TKT-303` Kill-switches (global, venue, strategy, user).
- `TKT-304` Margin-aware checks for options/futures.
- `TKT-305` Risk policy configuration with runtime reload.

Exit criteria:
- No order can bypass risk gate in any mode.

## M4 - Sim/Shadow Trading + Backtesting (Week 6-7)
Goal: realistic validation before live capital deployment.

- `TKT-401` Paper execution simulator with slippage/latency model.
- `TKT-402` Shadow mode pipeline (real reads, simulated writes).
- `TKT-403` Backtesting service reusing OMS/risk logic.
- `TKT-404` Historical data import adapters.
- `TKT-405` Performance report generator (Sharpe, DD, turnover, hit-rate).

Exit criteria:
- Identical strategy code runs in backtest, paper, shadow, and live.

## M5 - Multi-User + Auth + API (Week 8)
Goal: secure multi-tenant operation.

- `TKT-501` User/org model and RBAC.
- `TKT-502` Authentication (email/password + OAuth + API keys).
- `TKT-503` Tenant-aware secrets and connector credentials.
- `TKT-504` Public API v1 (orders, positions, balances, market data, metrics).
- `TKT-505` API rate limiting and audit logging.

Exit criteria:
- Multiple users can run isolated strategies/connectors safely.

## M6 - UI Platform (Week 9-10)
Goal: desktop-grade control plane comparable to broker terminals.

- `TKT-601` Tauri app shell with light/dark theme system.
- `TKT-602` Portfolio + positions + PnL dashboards.
- `TKT-603` Order ticket + depth + execution monitor.
- `TKT-604` Mode controls (paper/shadow/live) with explicit confirmations.
- `TKT-605` Risk and alert center.
- `TKT-606` Strategy runtime controls and logs viewer.

Exit criteria:
- Operators can manage full lifecycle from UI without terminal dependency.

## M7 - AI + Automation + Self-Healing (Week 11-12)
Goal: operational resilience and AI-assisted decision support.

- `TKT-701` Strategy orchestration workflows (schedules, dependencies, retries).
- `TKT-702` Auto-recovery supervisor (crash restart, heartbeat, circuit breaker).
- `TKT-703` AI signal plugin interface (offline + online feature paths).
- `TKT-704` AI safety policy (never bypass risk/limits).
- `TKT-705` Incident auto-diagnostics and remediation playbooks.

Exit criteria:
- Platform can self-diagnose common failures and degrade gracefully.

## M8 - Production Hardening (Week 13+)
Goal: safe live rollout with strict acceptance gates.

- `TKT-801` Staging environment parity and deployment pipelines.
- `TKT-802` Chaos testing for market-data disconnects and order failures.
- `TKT-803` Security hardening (secret rotation, key vault, signed builds).
- `TKT-804` Compliance exports and retention policies.
- `TKT-805` Capital ramp policy with automated stop conditions.

Exit criteria:
- Live trading allowed only after shadow and paper KPIs pass.

## 4) Mode Semantics (Must-Have)

- `paper`: no external write calls. Simulated fills only.
- `shadow`: live reads for market/account; writes are blocked and simulated.
- `live`: real order placement allowed if risk + auth + kill-switch checks pass.

Every order request records:
- requested mode,
- effective mode after policy checks,
- reason for block/allow,
- connector response or simulation result.

## 5) Non-Negotiable Safety Requirements

- No strategy can place live orders without:
  - explicit per-user, per-connector `live_enabled=true`,
  - risk policy loaded,
  - kill-switch healthy.
- Deterministic order IDs and idempotency keys on every submission.
- Full audit log for config changes, orders, fills, and mode transitions.
- Shadow and paper accounts must not share funding state with live.

## 6) Ticket Prioritization (First 20)

- P0: `TKT-001..005`, `TKT-101..106`, `TKT-201..203`, `TKT-301..303`, `TKT-401..402`
- P1: `TKT-204..205`, `TKT-304..305`, `TKT-403..405`, `TKT-501..505`
- P2: `TKT-601..606`, `TKT-701..705`, `TKT-801..805`

## 7) Immediate Next Sprint (5-Day)

- Day 1: workspace scaffold + domain types + mode toggles.
- Day 2: connector traits + Kalshi/IBKR/Coinbase health checks.
- Day 3: OMS command bus + simulated execution engine.
- Day 4: risk pre-check pipeline + persistence schemas.
- Day 5: shadow-mode end-to-end dry run with logs + metrics.

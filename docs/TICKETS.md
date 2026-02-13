# Ticket Backlog (Concrete)

## Milestone M0 - Foundation

| ID | Title | Owner | Priority | Acceptance Criteria |
|---|---|---|---|---|
| TKT-001 | Bootstrap `platform/` Rust workspace | Core | P0 | `cargo check` succeeds for workspace with module crates. |
| TKT-002 | Define canonical domain models | Core | P0 | `domain` crate exposes order, fill, balance, position, mode types. |
| TKT-003 | Enforce paper/shadow/live semantics | Core | P0 | OMS downgrades/blocks live mode when policy disallows writes. |
| TKT-004 | Strict env config loader | Core | P0 | Adapter initialization logs missing keys and skips unsafe startup paths. |
| TKT-005 | Structured logging baseline | Core | P0 | `tracing` + env filter enabled in server app. |

## Milestone M1 - Connectors

| ID | Title | Owner | Priority | Acceptance Criteria |
|---|---|---|---|---|
| TKT-101 | Connector trait and registry | Integrations | P0 | Trait supports `health`, `quote`, `balances`, `place_live_order`; registry routes by venue. |
| TKT-102 | Kalshi connector path | Integrations | P0 | RSA-signed requests for health, balances, quote, order endpoints. |
| TKT-103 | IBKR connector path | Integrations | P0 | Auth/session path with quote, balances, order submission format. |
| TKT-104 | Coinbase connector path | Integrations | P0 | HMAC-signed Advanced Trade requests for health, quote, balances, orders. |
| TKT-105 | Cross-venue symbol normalization | Integrations | P1 | Venue-specific symbol mapping helper used by OMS intake. |
| TKT-106 | Capability matrix + fallback behavior | Integrations | P1 | Unsupported operations return explicit typed errors (no silent failures). |

## Milestone M2 - OMS / Portfolio / Risk

| ID | Title | Owner | Priority | Acceptance Criteria |
|---|---|---|---|---|
| TKT-201 | OMS submit pipeline | Trading Core | P0 | Pre-trade risk + adapter routing + ack generation in one flow. |
| TKT-202 | Simulated execution path | Trading Core | P0 | Paper/shadow mode creates fills and updates portfolio cash/positions. |
| TKT-203 | Portfolio state transitions | Trading Core | P0 | Buy/sell fills update weighted average and position closeout correctly. |
| TKT-204 | Risk policy checks | Trading Core | P0 | Max order/position limits and live-mode policy enforced. |
| TKT-205 | Deterministic order auditing | Trading Core | P1 | Ack includes request id, effective mode, and decision reason. |

## Milestone M3 - Backtesting / Validation

| ID | Title | Owner | Priority | Acceptance Criteria |
|---|---|---|---|---|
| TKT-301 | Reuse OMS in backtest harness | Research | P1 | Backtest runner can replay order sequences through OMS. |
| TKT-302 | Paper-vs-shadow parity checks | Research | P1 | Same strategy inputs produce deterministic execution decisions. |
| TKT-303 | KPI reporting | Research | P1 | Report includes simulated/rejected counts and ending portfolio snapshots. |

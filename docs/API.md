# Trading Server API (v1)

## Auth

- Use `Authorization: Bearer <token>`.
- Tokens are loaded from:
  - `API_KEYS=token:user_id:org_id:role[:label],...`
  - or single-key fallback: `API_KEY` + optional `API_USER_ID`, `API_ROLE`.
- Optional JWT HS256: set `JWT_SECRET` and pass JWT bearer token with claims:
  - `sub` (user UUID), optional `org_id`, optional `role`, optional `label`.
- Password login uses Argon2 password hashes (DB-backed users).
- Org-level protected-route rate limiting is controlled by `ORG_RATE_LIMIT_PER_MIN`.

Roles:
- `admin`: can request `live` mode.
- `trader`: can place paper/shadow orders.
- `viewer`: read-only.

## Endpoints

- `GET /v1/health`
- `GET /v1/stream?token=<access_or_api_token>` (SSE)
- `POST /v1/auth/login`
- `POST /v1/auth/refresh`
- `POST /v1/auth/reset-password`
- `POST /v1/orders`
- `GET /v1/portfolio`
- `GET /v1/portfolio/history?limit=100`
- `GET /v1/orders/recent?limit=50`
- `GET /v1/execution/guards` (protected)
- `POST /v1/execution/guards/kill-switch` (admin only)
- `POST /v1/execution/guards/venues/:venue` (admin only)
- `POST /v1/auth/change-password` (protected)
- `POST /v1/auth/revoke` (protected)
- `POST /v1/api-keys` (admin only, DB required)
- `POST /v1/orgs` (admin only, DB required)
- `GET /v1/users` (admin only, DB required)
- `POST /v1/users` (admin only, DB required)
- `POST /v1/users/:user_id/issue-reset` (admin only, DB required)

SSE event types currently emitted:
- `system` (connection/auth metadata)
- `order_ack` (new order acknowledgment)
- `portfolio_snapshot` (post-trade portfolio update)
- `execution_guard` (kill-switch/venue-guard/health updates)

## Example Request

```json
{
  "venue": "coinbase",
  "symbol": "BTC-USD",
  "side": "buy",
  "order_type": "limit",
  "quantity": 0.1,
  "limit_price": 100.0,
  "mode": "paper"
}
```

## Quick Run

```powershell
cd D:\PythonProjects\polymarket\platform
Copy-Item .env.example .env
cargo run -p trading-server -- serve
```

GUI companion:

```powershell
cd D:\PythonProjects\polymarket\platform\apps\trading-ui
Copy-Item .env.example .env -Force
npm install
npm run dev
```

## Smoke Auth Script

```powershell
cd D:\PythonProjects\polymarket\platform
.\scripts\smoke_auth.ps1
```

Optional arguments:

```powershell
.\scripts\smoke_auth.ps1 -BaseUrl "http://127.0.0.1:8080" -AdminEmail "admin@localhost" -AdminPassword "your-password"
```

## CI Smoke (Disposable Postgres)

```bash
cd /path/to/polymarket/platform
bash scripts/ci_auth_smoke.sh
```

This script starts a disposable Postgres container, launches `trading-server`, runs the auth smoke flow, and tears everything down.

Windows PowerShell equivalent:

```powershell
cd D:\PythonProjects\polymarket\platform
.\scripts\ci_auth_smoke.ps1
```

## Database

- Migrations are in `platform/apps/trading-server/migrations`.
- Set `DATABASE_URL` and run:

```powershell
cd D:\PythonProjects\polymarket\platform
cargo run -p trading-server -- db-migrate
```

- Optional bootstrap on startup:
  - `DB_BOOTSTRAP_ORG_NAME`
  - `DB_BOOTSTRAP_ADMIN_EMAIL`
  - `DB_BOOTSTRAP_ADMIN_PASSWORD` (for login flow)
  - `DB_BOOTSTRAP_API_KEY` (for API-key auth flow)
- Auth/rate-limit tuning:
  - `ORG_RATE_LIMIT_PER_MIN`
  - `PASSWORD_RESET_TOKEN_TTL_SECS`
  - `STREAM_EVENT_BUFFER`
- Live guard hardening:
  - `LIVE_GUARD_REQUIRE_HEALTHY`
  - `LIVE_GUARD_HEALTH_MAX_AGE_SECS`
  - `LIVE_GUARD_HEALTH_POLL_SECS`
  - `LIVE_GUARD_FAILURE_THRESHOLD`
  - `LIVE_GUARD_COOLDOWN_SECS`
  - `LIVE_GUARD_DISABLED_VENUES`

## Password Reset Flow

1. Admin calls `POST /v1/users/:user_id/issue-reset` to mint a one-time reset token.
2. User calls `POST /v1/auth/reset-password` with that token and a new password.
3. User logs in with `POST /v1/auth/login`.

## Isolation

- OMS state is isolated per `org_id` (separate in-memory engine per org).
- Audit logs are split by org under `OMS_AUDIT_DIR` as `org_<org_id>.jsonl`.

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for ci_auth_smoke.sh" >&2
  exit 1
fi

CARGO_BIN=""
if command -v cargo >/dev/null 2>&1; then
  CARGO_BIN="cargo"
elif command -v cargo.exe >/dev/null 2>&1; then
  CARGO_BIN="cargo.exe"
elif [[ -n "${USERPROFILE:-}" ]] && [[ -x "${USERPROFILE}/.cargo/bin/cargo.exe" ]]; then
  CARGO_BIN="${USERPROFILE}/.cargo/bin/cargo.exe"
else
  echo "cargo is required for ci_auth_smoke.sh" >&2
  exit 1
fi

PS_BIN=""
if command -v pwsh >/dev/null 2>&1; then
  PS_BIN="pwsh"
elif command -v powershell.exe >/dev/null 2>&1; then
  PS_BIN="powershell.exe"
elif command -v powershell >/dev/null 2>&1; then
  PS_BIN="powershell"
else
  echo "PowerShell is required for ci_auth_smoke.sh (pwsh or powershell.exe)." >&2
  exit 1
fi

check_health() {
  local url="$1"
  "${PS_BIN}" -NoProfile -Command "try { \$resp = Invoke-WebRequest -Uri '${url}' -Method GET -TimeoutSec 3 -UseBasicParsing; if (\$resp.StatusCode -ge 200 -and \$resp.StatusCode -lt 300) { exit 0 } else { exit 1 } } catch { exit 1 }" >/dev/null 2>&1
}

PG_IMAGE="${PG_IMAGE:-postgres:16-alpine}"
PG_PORT="${PG_PORT:-55432}"
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-postgres}"
PG_DB="${PG_DB:-trading_platform}"
PG_CONTAINER="${PG_CONTAINER:-trading-smoke-pg-$(date +%s)}"

SERVER_ADDR="${SERVER_ADDR:-127.0.0.1:18080}"
BASE_URL="${BASE_URL:-http://${SERVER_ADDR}}"
DATABASE_URL="${DATABASE_URL:-postgres://${PG_USER}:${PG_PASSWORD}@127.0.0.1:${PG_PORT}/${PG_DB}}"

SERVER_LOG="${SERVER_LOG:-${ROOT_DIR}/var/ci_trading_server.log}"
mkdir -p "$(dirname "$SERVER_LOG")"

SERVER_PID=""
ENV_FILE="${ROOT_DIR}/.env"
ENV_BACKUP=""
CI_ENV_WRITTEN=0

cleanup() {
  set +e
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  if [[ "${CI_ENV_WRITTEN}" -eq 1 ]]; then
    if [[ -n "${ENV_BACKUP}" ]] && [[ -f "${ENV_BACKUP}" ]]; then
      mv "${ENV_BACKUP}" "${ENV_FILE}" >/dev/null 2>&1 || true
    else
      rm -f "${ENV_FILE}" >/dev/null 2>&1 || true
    fi
  fi
  docker rm -f "${PG_CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[STEP] Starting disposable Postgres container ${PG_CONTAINER}"
docker run -d --rm \
  --name "${PG_CONTAINER}" \
  -e POSTGRES_USER="${PG_USER}" \
  -e POSTGRES_PASSWORD="${PG_PASSWORD}" \
  -e POSTGRES_DB="${PG_DB}" \
  -p "${PG_PORT}:5432" \
  "${PG_IMAGE}" >/dev/null

echo "[STEP] Waiting for Postgres readiness"
attempt=0
pg_ready=0
while [[ "${attempt}" -lt 120 ]]; do
  if docker exec "${PG_CONTAINER}" pg_isready -U "${PG_USER}" -d "${PG_DB}" >/dev/null 2>&1; then
    # During init, postgres can briefly report ready and then restart.
    sleep 1
    if docker exec "${PG_CONTAINER}" pg_isready -U "${PG_USER}" -d "${PG_DB}" >/dev/null 2>&1; then
      pg_ready=1
      break
    fi
  fi
  sleep 1
  attempt=$((attempt + 1))
done

if [[ "${pg_ready}" -ne 1 ]]; then
  echo "Postgres did not become ready in time" >&2
  docker ps --filter "name=${PG_CONTAINER}" >&2 || true
  docker logs "${PG_CONTAINER}" --tail 120 >&2 || true
  exit 1
fi

export DATABASE_URL
export JWT_SECRET="${JWT_SECRET:-ci_jwt_secret_change_me}"
export DB_MAX_CONNECTIONS="${DB_MAX_CONNECTIONS:-5}"
export DB_BOOTSTRAP_ORG_NAME="${DB_BOOTSTRAP_ORG_NAME:-ci-org}"
export DB_BOOTSTRAP_ADMIN_EMAIL="${DB_BOOTSTRAP_ADMIN_EMAIL:-admin@localhost}"
export DB_BOOTSTRAP_ADMIN_PASSWORD="${DB_BOOTSTRAP_ADMIN_PASSWORD:-Admin123!Smoke}"
export DB_BOOTSTRAP_API_KEY="${DB_BOOTSTRAP_API_KEY:-}"
export DB_BOOTSTRAP_API_KEY_LABEL="${DB_BOOTSTRAP_API_KEY_LABEL:-bootstrap-admin}"
export EXECUTION_MODE="${EXECUTION_MODE:-paper}"
export LIVE_TRADING_ENABLED="${LIVE_TRADING_ENABLED:-false}"
export SHADOW_READS_ENABLED="${SHADOW_READS_ENABLED:-true}"
export SIM_STARTING_CASH_USD="${SIM_STARTING_CASH_USD:-50000}"
export SIM_FEE_BPS="${SIM_FEE_BPS:-2.0}"
export OMS_AUDIT_DIR="${OMS_AUDIT_DIR:-var/oms_audit_ci}"
export ORG_RATE_LIMIT_PER_MIN="${ORG_RATE_LIMIT_PER_MIN:-600}"
export PASSWORD_RESET_TOKEN_TTL_SECS="${PASSWORD_RESET_TOKEN_TTL_SECS:-3600}"
export SERVER_ADDR

# Ensure at least one adapter is configured for server startup.
export COINBASE_API_KEY="${COINBASE_API_KEY:-ci-dummy-key}"
export COINBASE_API_SECRET="${COINBASE_API_SECRET:-ci-dummy-secret}"
export COINBASE_API_PASSPHRASE="${COINBASE_API_PASSPHRASE:-ci-dummy-passphrase}"
export COINBASE_BASE_URL="${COINBASE_BASE_URL:-https://api.coinbase.com}"

if [[ -f "${ENV_FILE}" ]]; then
  ENV_BACKUP="${ENV_FILE}.ci_auth_smoke.bak.$(date +%s)"
  cp "${ENV_FILE}" "${ENV_BACKUP}"
fi
cat >"${ENV_FILE}" <<EOF
DATABASE_URL=${DATABASE_URL}
JWT_SECRET=${JWT_SECRET}
DB_MAX_CONNECTIONS=${DB_MAX_CONNECTIONS}
DB_BOOTSTRAP_ORG_NAME=${DB_BOOTSTRAP_ORG_NAME}
DB_BOOTSTRAP_ADMIN_EMAIL=${DB_BOOTSTRAP_ADMIN_EMAIL}
DB_BOOTSTRAP_ADMIN_PASSWORD=${DB_BOOTSTRAP_ADMIN_PASSWORD}
DB_BOOTSTRAP_API_KEY=${DB_BOOTSTRAP_API_KEY}
DB_BOOTSTRAP_API_KEY_LABEL=${DB_BOOTSTRAP_API_KEY_LABEL}
EXECUTION_MODE=${EXECUTION_MODE}
LIVE_TRADING_ENABLED=${LIVE_TRADING_ENABLED}
SHADOW_READS_ENABLED=${SHADOW_READS_ENABLED}
SIM_STARTING_CASH_USD=${SIM_STARTING_CASH_USD}
SIM_FEE_BPS=${SIM_FEE_BPS}
OMS_AUDIT_DIR=${OMS_AUDIT_DIR}
ORG_RATE_LIMIT_PER_MIN=${ORG_RATE_LIMIT_PER_MIN}
PASSWORD_RESET_TOKEN_TTL_SECS=${PASSWORD_RESET_TOKEN_TTL_SECS}
SERVER_ADDR=${SERVER_ADDR}
COINBASE_API_KEY=${COINBASE_API_KEY}
COINBASE_API_SECRET=${COINBASE_API_SECRET}
COINBASE_API_PASSPHRASE=${COINBASE_API_PASSPHRASE}
COINBASE_BASE_URL=${COINBASE_BASE_URL}
EOF
CI_ENV_WRITTEN=1

echo "[STEP] Starting trading server"
env \
  DATABASE_URL="${DATABASE_URL}" \
  JWT_SECRET="${JWT_SECRET}" \
  DB_MAX_CONNECTIONS="${DB_MAX_CONNECTIONS}" \
  DB_BOOTSTRAP_ORG_NAME="${DB_BOOTSTRAP_ORG_NAME}" \
  DB_BOOTSTRAP_ADMIN_EMAIL="${DB_BOOTSTRAP_ADMIN_EMAIL}" \
  DB_BOOTSTRAP_ADMIN_PASSWORD="${DB_BOOTSTRAP_ADMIN_PASSWORD}" \
  DB_BOOTSTRAP_API_KEY="${DB_BOOTSTRAP_API_KEY}" \
  DB_BOOTSTRAP_API_KEY_LABEL="${DB_BOOTSTRAP_API_KEY_LABEL}" \
  EXECUTION_MODE="${EXECUTION_MODE}" \
  LIVE_TRADING_ENABLED="${LIVE_TRADING_ENABLED}" \
  SHADOW_READS_ENABLED="${SHADOW_READS_ENABLED}" \
  SIM_STARTING_CASH_USD="${SIM_STARTING_CASH_USD}" \
  SIM_FEE_BPS="${SIM_FEE_BPS}" \
  OMS_AUDIT_DIR="${OMS_AUDIT_DIR}" \
  ORG_RATE_LIMIT_PER_MIN="${ORG_RATE_LIMIT_PER_MIN}" \
  PASSWORD_RESET_TOKEN_TTL_SECS="${PASSWORD_RESET_TOKEN_TTL_SECS}" \
  SERVER_ADDR="${SERVER_ADDR}" \
  COINBASE_API_KEY="${COINBASE_API_KEY}" \
  COINBASE_API_SECRET="${COINBASE_API_SECRET}" \
  COINBASE_API_PASSPHRASE="${COINBASE_API_PASSPHRASE}" \
  COINBASE_BASE_URL="${COINBASE_BASE_URL}" \
  "${CARGO_BIN}" run -p trading-server -- serve >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

echo "[STEP] Waiting for trading server readiness at ${BASE_URL}/v1/health"
attempt=0
while [[ "${attempt}" -lt 90 ]]; do
  if check_health "${BASE_URL}/v1/health"; then
    break
  fi
  if ! kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    echo "trading-server exited before becoming ready" >&2
    cat "${SERVER_LOG}" >&2 || true
    exit 1
  fi
  sleep 1
  attempt=$((attempt + 1))
done

if ! check_health "${BASE_URL}/v1/health"; then
  echo "trading-server did not become ready in time" >&2
  cat "${SERVER_LOG}" >&2 || true
  exit 1
fi

echo "[STEP] Running auth smoke flow (${PS_BIN})"
SMOKE_SCRIPT_PATH="${ROOT_DIR}/scripts/smoke_auth.ps1"
if [[ "${PS_BIN}" == "powershell.exe" ]] && command -v wslpath >/dev/null 2>&1; then
  SMOKE_SCRIPT_PATH="$(wslpath -w "${SMOKE_SCRIPT_PATH}")"
fi

"${PS_BIN}" -NoProfile -ExecutionPolicy Bypass -File "${SMOKE_SCRIPT_PATH}" \
  -BaseUrl "${BASE_URL}" \
  -AdminEmail "${DB_BOOTSTRAP_ADMIN_EMAIL}" \
  -AdminPassword "${DB_BOOTSTRAP_ADMIN_PASSWORD}"

echo "[OK] CI auth smoke completed successfully"

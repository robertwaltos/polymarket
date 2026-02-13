export type ExecutionMode = "paper" | "shadow" | "live";

export interface HealthResponse {
  ok: boolean;
  timestamp: string;
  execution_mode_default: ExecutionMode;
  audit_recent_count: number;
  db_enabled: boolean;
}

export interface LiveGuardConfig {
  require_healthy: boolean;
  max_health_age_secs: number;
  failure_threshold: number;
  cooldown_secs: number;
  health_poll_interval_secs: number;
}

export interface LiveGuardGlobalState {
  kill_switch_enabled: boolean;
  kill_switch_reason: string | null;
  kill_switch_updated_at: string;
}

export interface LiveGuardVenueState {
  venue: PlaceOrderInput["venue"];
  live_enabled: boolean;
  live_allowed_now: boolean;
  block_reason: string | null;
  consecutive_live_failures: number;
  breaker_open: boolean;
  breaker_open_until: string | null;
  last_health_ok: boolean | null;
  last_health_detail: string | null;
  last_health_at: string | null;
  updated_at: string;
}

export interface LiveGuardSnapshot {
  config: LiveGuardConfig;
  global: LiveGuardGlobalState;
  venues: LiveGuardVenueState[];
}

export interface AuthLoginResponse {
  access_token: string;
  refresh_token: string;
  token_type: "Bearer";
  expires_in: number;
}

export interface PositionSnapshot {
  symbol: string;
  quantity: number;
  average_price: number;
  mark_price: number;
  unrealized_pnl: number;
}

export interface PortfolioResponse {
  cash_usd: number;
  positions: PositionSnapshot[];
  source: "postgres" | "memory";
  created_at?: string;
}

export interface PortfolioHistorySnapshot {
  cash_usd: number;
  positions_json: PositionSnapshot[];
  created_at: string;
}

export interface PortfolioHistoryResponse {
  snapshots: PortfolioHistorySnapshot[];
}

export interface OrderAck {
  request_id: string;
  mode: ExecutionMode;
  status: "accepted" | "simulated" | "rejected";
  venue_order_id: string | null;
  message: string;
}

export interface RecentOrderRecord {
  order: {
    request_id: string;
    submitted_at: string;
    mode: ExecutionMode;
    side: "buy" | "sell";
    quantity: number;
    limit_price: number | null;
    instrument: {
      symbol: string;
      venue: string;
      asset_class: string;
      quote_currency: string;
    };
  };
  ack: {
    status: "accepted" | "simulated" | "rejected";
    venue_order_id: string | null;
    message: string;
  };
}

export interface RecentOrdersResponse {
  records: RecentOrderRecord[];
}

export interface ApiErrorBody {
  error: string;
}

export interface PlaceOrderInput {
  venue: "coinbase" | "ibkr" | "kalshi";
  symbol: string;
  side: "buy" | "sell";
  order_type: "limit" | "market";
  quantity: number;
  limit_price?: number;
  mode?: ExecutionMode;
}

export interface PlaceOrderResponse {
  ack: OrderAck;
  actor: {
    user_id: string;
    org_id: string;
    label: string;
    role: string;
  };
}

export interface LiveKillSwitchInput {
  enabled: boolean;
  reason?: string;
}

export interface VenueGuardControlInput {
  live_enabled?: boolean;
  reset_breaker?: boolean;
}

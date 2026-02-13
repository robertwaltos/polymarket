export type ExecutionMode = "paper" | "shadow" | "live";

export interface HealthResponse {
  ok: boolean;
  timestamp: string;
  execution_mode_default: ExecutionMode;
  audit_recent_count: number;
  db_enabled: boolean;
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

export type ExecutionMode = "paper" | "shadow" | "live";

export interface HealthResponse {
  ok: boolean;
  timestamp: string;
  execution_mode_default: ExecutionMode;
  audit_recent_count: number;
  db_enabled: boolean;
  intel_claims?: number;
  orchestrator_agents?: number;
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
  instrument?: {
    venue: string;
    symbol: string;
    asset_class: string;
    quote_currency: string;
  };
  symbol?: string;
  quantity: number;
  average_price: number;
  mark_price?: number;
  market_price?: number | null;
  unrealized_pnl?: number | null;
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

export interface ClosePositionInput {
  venue: "coinbase" | "ibkr" | "kalshi";
  symbol: string;
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

export type SocialSource = "x" | "reddit" | "manual";

export interface SocialClaim {
  id: string;
  source: SocialSource;
  query: string | null;
  author: string | null;
  community: string | null;
  posted_at: string | null;
  captured_at: string;
  url: string | null;
  text: string;
  brag_score: number;
  hype_score: number;
  evidence_flags: string[];
  strategy_tags: string[];
}

export interface SocialClaimListResponse {
  claims: SocialClaim[];
}

export interface SocialTagSignal {
  tag: string;
  mentions: number;
  weight: number;
}

export interface SocialTagsResponse {
  window_hours: number;
  tags: SocialTagSignal[];
}

export interface SourceScanStatus {
  source: SocialSource;
  ok: boolean;
  inserted: number;
  detail: string | null;
}

export interface SocialScanResponse {
  started_at: string;
  finished_at: string;
  inserted: number;
  total_claims: number;
  source_status: SourceScanStatus[];
  top_tags: SocialTagSignal[];
}

export interface ManualSocialClaimInput {
  source?: SocialSource;
  text: string;
  url?: string;
  author?: string;
  community?: string;
  query?: string;
}

export interface SocialScanInput {
  sources?: SocialSource[];
  queries?: string[];
  lookback_days?: number;
  max_per_query?: number;
}

export type StrategyLifecycle = "active" | "paused" | "killed";

export interface StrategyPerformance {
  total_return_pct: number;
  sharpe: number;
  max_drawdown_pct: number;
  win_rate: number;
  trades: number;
  updated_at: string;
}

export interface StrategyAgent {
  id: string;
  name: string;
  venue: string | null;
  description: string | null;
  tags: string[];
  lifecycle: StrategyLifecycle;
  allocation_usd: number;
  score: number;
  rank: number;
  last_score_note: string | null;
  performance: StrategyPerformance;
  created_at: string;
  updated_at: string;
}

export interface OrchestratorAction {
  at: string;
  agent_id: string;
  agent_name: string;
  action: string;
  reason: string;
  allocation_before_usd: number;
  allocation_after_usd: number;
}

export interface OrchestratorTagBias {
  tag: string;
  weight: number;
}

export interface OrchestratorConfig {
  total_capital_usd: number;
  starting_allocation_usd: number;
  max_allocation_per_strategy_usd: number;
  promote_multiplier: number;
  promote_top_n: number;
  kill_bottom_n: number;
  min_trades_before_ranking: number;
  kill_drawdown_pct: number;
  kill_score_threshold: number;
}

export interface OrchestratorSnapshot {
  config: OrchestratorConfig;
  evaluated_at: string | null;
  total_allocated_usd: number;
  agents: StrategyAgent[];
  last_actions: OrchestratorAction[];
  active_tag_bias: OrchestratorTagBias[];
}

export interface UpsertStrategyAgentInput {
  id?: string;
  name: string;
  venue?: string;
  description?: string;
  tags?: string[];
  active?: boolean;
  allocation_usd?: number;
}

export interface StrategyPerformanceInput {
  total_return_pct: number;
  sharpe: number;
  max_drawdown_pct: number;
  win_rate: number;
  trades: number;
}

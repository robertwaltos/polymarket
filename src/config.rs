use crate::error::AgentError;
use ethers_signers::LocalWallet;
use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub private_key: String,
    pub anthropic_key: String,
    pub clob_api_key: Option<String>,
    pub polygon_rpc_url: String,
    pub shadow_mode: bool,
    pub loop_interval_secs: u64,
    pub market_fetch_limit: usize,
    pub anthropic_model: String,
    pub anthropic_version: String,
    pub anthropic_max_tokens: u32,
    pub usdc_contract: String,
    pub usdc_decimals: u32,
    pub starting_balance_usdc: f64,
    pub limit_price_offset_bps: i64,
    pub dune_api_key: Option<String>,
    pub dune_query_ids: Vec<u64>,
    pub moralis_api_key: Option<String>,
    pub x_bearer_token: Option<String>,
    pub news_api_key: Option<String>,
    pub noaa_endpoint: Option<String>,
    pub rotowire_rss: Option<String>,
    pub espn_injuries_url: Option<String>,
    pub claude_cost_per_million_input: f64,
    pub claude_cost_per_million_output: f64,
    pub betfair_app_key: Option<String>,
    pub betfair_session: Option<String>,
    pub betfair_event_type_ids: Vec<String>,
    pub kalshi_api_key: Option<String>,
    pub smarkets_session: Option<String>,
    pub ibkr_api_key: Option<String>,
    pub odds_api_key: Option<String>,
    pub odds_api_sport: String,
    pub cftc_url: Option<String>,
    pub robinhood_predicts_url: Option<String>,
    pub enable_gui: bool,
    pub exploration_epsilon: f64,
    pub exploration_fraction: f64,
    pub enable_rl: bool,
    pub rl_epsilon: f64,
    pub rl_alpha: f64,
    pub rl_gamma: f64,
    pub enable_pq_signing: bool,
    pub enable_federated: bool,
    pub enable_arb_bridge: bool,
    pub arbitrum_rpc_url: Option<String>,
    pub arbitrum_chain_id: u64,
    pub local_polls_url: Option<String>,
    pub enable_self_audit: bool,
    pub self_audit_threshold: f64,
    pub self_audit_path: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self, AgentError> {
        let private_key = env::var("PRIVATE_KEY")
            .or_else(|_| env::var("POLYGON_PRIVATE_KEY"))
            .map_err(AgentError::Env)?;

        let anthropic_key = env::var("ANTHROPIC_KEY")
            .or_else(|_| env::var("ANTHROPIC_API_KEY"))
            .map_err(AgentError::Env)?;

        let clob_api_key = env::var("CLOB_API_KEY").ok();
        let polygon_rpc_url = env::var("POLYGON_RPC_URL")?;

        let shadow_mode = env_bool("SHADOW_MODE", true);
        let loop_interval_secs = env_u64("LOOP_INTERVAL_SECS", 600);
        let market_fetch_limit = env_usize("MARKET_FETCH_LIMIT", 1000);

        let anthropic_model = env::var("ANTHROPIC_MODEL")
            .unwrap_or_else(|_| "claude-3-5-sonnet-20240620".to_string());
        let anthropic_version = env::var("ANTHROPIC_VERSION")
            .unwrap_or_else(|_| "2023-06-01".to_string());
        let anthropic_max_tokens = env_u32("ANTHROPIC_MAX_TOKENS", 512);

        let usdc_contract = env::var("USDC_CONTRACT")
            .unwrap_or_else(|_| "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".to_string());
        let usdc_decimals = env_u32("USDC_DECIMALS", 6);

        let starting_balance_usdc = env_f64("STARTING_BALANCE_USDC", 50.0);
        let limit_price_offset_bps = env_i64("LIMIT_PRICE_OFFSET_BPS", 0);

        let dune_api_key = env::var("DUNE_API_KEY").ok();
        let dune_query_ids = env::var("DUNE_QUERY_IDS")
            .ok()
            .map(parse_csv_u64)
            .unwrap_or_default();
        let moralis_api_key = env::var("MORALIS_API_KEY").ok();
        let x_bearer_token = env::var("X_BEARER_TOKEN").ok();
        let news_api_key = env::var("NEWS_API_KEY").ok();
        let noaa_endpoint = env::var("NOAA_ENDPOINT").ok();
        let rotowire_rss = env::var("ROTOWIRE_RSS").ok();
        let espn_injuries_url = env::var("ESPN_INJURIES_URL").ok();

        let claude_cost_per_million_input = env_f64("CLAUDE_COST_PER_MILLION_INPUT", 0.0);
        let claude_cost_per_million_output = env_f64("CLAUDE_COST_PER_MILLION_OUTPUT", 0.0);

        let betfair_app_key = env::var("BETFAIR_APP_KEY")
            .or_else(|_| env::var("BETFAIR_API_KEY"))
            .ok();
        let betfair_session = env::var("BETFAIR_SESSION")
            .or_else(|_| env::var("BETFAIR_SESSION_TOKEN"))
            .ok();
        let betfair_event_type_ids = env::var("BETFAIR_EVENT_TYPE_IDS")
            .ok()
            .map(parse_csv_string)
            .unwrap_or_else(|| vec!["1".to_string()]);

        let kalshi_api_key = env::var("KALSHI_API_KEY").ok();
        let smarkets_session = env::var("SMARKETS_SESSION").ok();
        let ibkr_api_key = env::var("IBKR_API_KEY").ok();
        let odds_api_key = env::var("ODDS_API_KEY")
            .or_else(|_| env::var("ODDSAPI_KEY"))
            .ok();
        let odds_api_sport = env::var("ODDS_API_SPORT")
            .unwrap_or_else(|_| "basketball_nba".to_string());
        let cftc_url = env::var("CFTC_URL").ok();
        let robinhood_predicts_url = env::var("ROBINHOOD_PREDICTS_URL").ok();

        let enable_gui = env_bool("ENABLE_GUI", false);
        let exploration_epsilon = env_f64("EXPLORATION_EPSILON", 0.0);
        let exploration_fraction = env_f64("EXPLORATION_FRACTION", 0.01);
        let enable_rl = env_bool("ENABLE_RL", false);
        let rl_epsilon = env_f64("RL_EPSILON", 0.2);
        let rl_alpha = env_f64("RL_ALPHA", 0.1);
        let rl_gamma = env_f64("RL_GAMMA", 0.9);
        let enable_pq_signing = env_bool("ENABLE_PQ_SIGNING", false);
        let enable_federated = env_bool("ENABLE_FEDERATED", false);
        let enable_arb_bridge = env_bool("ENABLE_ARB_BRIDGE", false);
        let arbitrum_rpc_url = env::var("ARBITRUM_RPC_URL").ok();
        let arbitrum_chain_id = env_u64("ARBITRUM_CHAIN_ID", 42161);
        let local_polls_url = env::var("LOCAL_POLLS_URL")
            .or_else(|_| env::var("TEXAS_TRIBUNE_POLLS_URL"))
            .ok();
        let enable_self_audit = env_bool("ENABLE_SELF_AUDIT", false);
        let self_audit_threshold = env_f64("SELF_AUDIT_THRESHOLD", 500.0);
        let self_audit_path = env::var("SELF_AUDIT_PATH").ok();

        Ok(Self {
            private_key,
            anthropic_key,
            clob_api_key,
            polygon_rpc_url,
            shadow_mode,
            loop_interval_secs,
            market_fetch_limit,
            anthropic_model,
            anthropic_version,
            anthropic_max_tokens,
            usdc_contract,
            usdc_decimals,
            starting_balance_usdc,
            limit_price_offset_bps,
            dune_api_key,
            dune_query_ids,
            moralis_api_key,
            x_bearer_token,
            news_api_key,
            noaa_endpoint,
            rotowire_rss,
            espn_injuries_url,
            claude_cost_per_million_input,
            claude_cost_per_million_output,
            betfair_app_key,
            betfair_session,
            betfair_event_type_ids,
            kalshi_api_key,
            smarkets_session,
            ibkr_api_key,
            odds_api_key,
            odds_api_sport,
            cftc_url,
            robinhood_predicts_url,
            enable_gui,
            exploration_epsilon,
            exploration_fraction,
            enable_rl,
            rl_epsilon,
            rl_alpha,
            rl_gamma,
            enable_pq_signing,
            enable_federated,
            enable_arb_bridge,
            arbitrum_rpc_url,
            arbitrum_chain_id,
            local_polls_url,
            enable_self_audit,
            self_audit_threshold,
            self_audit_path,
        })
    }

    pub fn wallet(&self) -> Result<LocalWallet, AgentError> {
        self.private_key
            .parse::<LocalWallet>()
            .map_err(|e| AgentError::Wallet(e.to_string()))
            .map(|wallet| wallet.with_chain_id(137u64))
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key).ok().as_deref() {
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES") => true,
        Some("0") | Some("false") | Some("FALSE") | Some("no") | Some("NO") => false,
        _ => default,
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_u32(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_i64(key: &str, default: i64) -> i64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn parse_csv_u64(input: String) -> Vec<u64> {
    input
        .split(',')
        .filter_map(|s| s.trim().parse::<u64>().ok())
        .collect()
}

fn parse_csv_string(input: String) -> Vec<String> {
    input
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

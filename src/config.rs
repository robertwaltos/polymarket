use crate::error::AgentError;
use crate::execution::ExecutionVenue;
use ethers_signers::{LocalWallet, Signer};
use std::env;
use std::fs;
use std::path::Path;

#[derive(Clone, Debug)]
pub struct Config {
    pub execution_venue: ExecutionVenue,
    pub private_key: String,
    pub anthropic_key: String,
    pub anthropic_base_url: String,
    pub clob_api_key: Option<String>,
    pub clob_api_secret: Option<String>,
    pub clob_api_passphrase: Option<String>,
    pub polygon_rpc_url: String,
    pub shadow_mode: bool,
    pub shadow_balance_override: Option<f64>,
    pub loop_interval_secs: u64,
    pub market_fetch_limit: usize,
    pub anthropic_model: String,
    pub anthropic_version: String,
    pub anthropic_max_tokens: u32,
    pub anthropic_prompt_max_chars: usize,
    pub anthropic_max_markets_per_cycle: usize,
    pub anthropic_rate_limit_cooldown_secs: u64,
    pub anthropic_input_tokens_per_minute: u64,
    pub weather_fast_path: bool,
    pub weather_station_weight: f64,
    pub weather_station_scale_f: f64,
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
    pub clob_exchange_address: String,
    pub clob_taker_address: String,
    pub clob_fee_rate_bps: u64,
    pub clob_nonce: u64,
    pub clob_expiration_secs: u64,
    pub clob_order_type: String,
    pub clob_post_only: bool,
    pub clob_tick_size: f64,
    pub clob_signature_type: u8,
    pub clob_neg_risk: bool,
    pub clob_funder: Option<String>,
    pub betfair_app_key: Option<String>,
    pub betfair_session: Option<String>,
    pub betfair_event_type_ids: Vec<String>,
    pub kalshi_api_key: Option<String>,
    pub kalshi_key_id: Option<String>,
    pub kalshi_private_key_b64: Option<String>,
    pub kalshi_base_url: String,
    pub kalshi_post_only: bool,
    pub kalshi_time_in_force: String,
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
    pub federated_cluster_id: String,
    pub federated_peer_id: String,
    pub federated_shared_key: Option<String>,
    pub federated_sync_dir: String,
    pub federated_min_peer_updates: usize,
    pub federated_max_snapshot_age_secs: u64,
    pub federated_secure_aggregation: bool,
    pub federated_local_blend: f64,
    pub enable_ev_scheduler: bool,
    pub scheduler_cycle_token_budget: u64,
    pub scheduler_ev_floor: f64,
    pub scheduler_max_candidates: usize,
    pub scheduler_urgent_window_hours: i64,
    pub scheduler_timezone_offset_hours: i32,
    pub enable_arb_bridge: bool,
    pub arbitrum_rpc_url: Option<String>,
    pub arbitrum_chain_id: u64,
    pub local_polls_url: Option<String>,
    pub enable_self_audit: bool,
    pub self_audit_threshold: f64,
    pub self_audit_path: Option<String>,
    pub rl_offline_train: bool,
    pub rl_offline_market_limit: usize,
    pub rl_offline_episodes: u64,
    pub rl_offline_epsilon_start: f64,
    pub rl_offline_epsilon_decay: f64,
    pub rl_replay_buffer: usize,
    pub rl_finetune_interval: u64,
    pub rl_lr_slow: f64,
    pub rl_lr_fast: f64,
    pub augur_rpc_url: Option<String>,
    pub augur_contract: Option<String>,
    pub augur_markets_url: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self, AgentError> {
        let execution_venue = ExecutionVenue::from_env(
            &env::var("EXECUTION_VENUE").unwrap_or_else(|_| "polymarket".to_string()),
        );

        let private_key = if execution_venue == ExecutionVenue::Polymarket {
            env::var("PRIVATE_KEY")
                .or_else(|_| env::var("POLYGON_PRIVATE_KEY"))
                .map_err(AgentError::Env)?
        } else {
            env_opt("PRIVATE_KEY")
                .or_else(|| env_opt("POLYGON_PRIVATE_KEY"))
                .unwrap_or_default()
        };

        let polygon_rpc_url = if execution_venue == ExecutionVenue::Polymarket {
            env::var("POLYGON_RPC_URL")?
        } else {
            env_opt("POLYGON_RPC_URL").unwrap_or_default()
        };

        let anthropic_key = env::var("ANTHROPIC_KEY")
            .or_else(|_| env::var("ANTHROPIC_API_KEY"))
            .map_err(AgentError::Env)?;
        let anthropic_base_url = env::var("ANTHROPIC_BASE_URL")
            .unwrap_or_else(|_| "https://api.anthropic.com".to_string());

        let clob_api_key = env_opt("CLOB_API_KEY");
        let clob_api_secret =
            env_opt("CLOB_API_SECRET").or_else(|| env_opt("CLOB_API_SECRET_KEY"));
        let clob_api_passphrase =
            env_opt("CLOB_API_PASSPHRASE").or_else(|| env_opt("CLOB_API_PASS"));

        let shadow_mode = env_bool("SHADOW_MODE", true);
        let shadow_balance_override = env::var("SHADOW_BALANCE_OVERRIDE")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| *v >= 0.0);
        let loop_interval_secs = env_u64("LOOP_INTERVAL_SECS", 600);
        let market_fetch_limit = env_usize("MARKET_FETCH_LIMIT", 1000);

        let anthropic_model = env::var("ANTHROPIC_MODEL")
            .unwrap_or_else(|_| "claude-sonnet-4-5-20250929".to_string());
        let anthropic_version = env::var("ANTHROPIC_VERSION")
            .unwrap_or_else(|_| "2023-06-01".to_string());
        let anthropic_max_tokens = env_u32("ANTHROPIC_MAX_TOKENS", 512);
        let anthropic_prompt_max_chars = env_usize("ANTHROPIC_PROMPT_MAX_CHARS", 4000);
        let anthropic_max_markets_per_cycle =
            env_usize("ANTHROPIC_MAX_MARKETS_PER_CYCLE", 12).max(1);
        let anthropic_rate_limit_cooldown_secs =
            env_u64("ANTHROPIC_RATE_LIMIT_COOLDOWN_SECS", 60).max(5);
        let anthropic_input_tokens_per_minute =
            env_u64("ANTHROPIC_INPUT_TOKENS_PER_MINUTE", 5000).max(500);
        let weather_fast_path = env_bool("WEATHER_FAST_PATH", true);
        let weather_station_weight = env_f64("WEATHER_STATION_WEIGHT", 0.70).clamp(0.0, 1.0);
        let weather_station_scale_f = env_f64("WEATHER_STATION_SCALE_F", 4.0).max(0.5);

        let usdc_contract = env::var("USDC_CONTRACT")
            .unwrap_or_else(|_| "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".to_string());
        let usdc_decimals = env_u32("USDC_DECIMALS", 6);

        let starting_balance_usdc = env::var("BANKROLL")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or_else(|| env_f64("STARTING_BALANCE_USDC", 50.0));
        let limit_price_offset_bps = env_i64("LIMIT_PRICE_OFFSET_BPS", 0);

        let dune_api_key = env_opt("DUNE_API_KEY");
        let dune_query_ids = env::var("DUNE_QUERY_IDS")
            .ok()
            .map(parse_csv_u64)
            .unwrap_or_default();
        let moralis_api_key = env_opt("MORALIS_API_KEY");
        let x_bearer_token = env_opt("X_BEARER_TOKEN");
        let news_api_key = env_opt("NEWS_API_KEY");
        let noaa_endpoint = env_opt("NOAA_ENDPOINT");
        let rotowire_rss = env_opt("ROTOWIRE_RSS");
        let espn_injuries_url = env_opt("ESPN_INJURIES_URL");

        let claude_cost_per_million_input = env_f64("CLAUDE_COST_PER_MILLION_INPUT", 0.0);
        let claude_cost_per_million_output = env_f64("CLAUDE_COST_PER_MILLION_OUTPUT", 0.0);

        let clob_exchange_address = env::var("CLOB_EXCHANGE_ADDRESS")
            .unwrap_or_else(|_| "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E".to_string());
        let clob_taker_address = env::var("CLOB_TAKER_ADDRESS")
            .unwrap_or_else(|_| "0x0000000000000000000000000000000000000000".to_string());
        let clob_fee_rate_bps = env_u64("CLOB_FEE_RATE_BPS", 0);
        let clob_nonce = env_u64("CLOB_NONCE", 0);
        let clob_expiration_secs = env_u64("CLOB_EXPIRATION_SECS", 3600);
        let clob_order_type =
            env::var("CLOB_ORDER_TYPE").unwrap_or_else(|_| "GTC".to_string());
        let clob_post_only = env_bool("CLOB_POST_ONLY", true);
        let clob_tick_size = env_f64("CLOB_TICK_SIZE", 0.01);
        let clob_signature_type = env_u64("CLOB_SIGNATURE_TYPE", 0).min(u8::MAX as u64) as u8;
        let clob_neg_risk = env_bool("CLOB_NEG_RISK", false);
        let clob_funder = env_opt("CLOB_FUNDER");

        let betfair_app_key =
            env_opt("BETFAIR_APP_KEY").or_else(|| env_opt("BETFAIR_API_KEY"));
        let betfair_session =
            env_opt("BETFAIR_SESSION").or_else(|| env_opt("BETFAIR_SESSION_TOKEN"));
        let betfair_event_type_ids = env::var("BETFAIR_EVENT_TYPE_IDS")
            .ok()
            .map(parse_csv_string)
            .unwrap_or_else(|| vec!["1".to_string()]);

        let kalshi_api_key = env_opt("KALSHI_API_KEY");
        let kalshi_key_id = env_opt("KALSHI_KEY_ID").or_else(|| env_opt("KALSHI_API_KEY_ID"));
        let kalshi_private_key_b64 =
            env_opt("KALSHI_PRIVATE_KEY").or_else(|| env_opt("KALSHI_RSA_PRIVATE_KEY"));
        let kalshi_base_url = env::var("KALSHI_BASE_URL")
            .unwrap_or_else(|_| "https://api.elections.kalshi.com".to_string());
        let kalshi_post_only = env_bool("KALSHI_POST_ONLY", false);
        let kalshi_time_in_force = env::var("KALSHI_TIME_IN_FORCE")
            .unwrap_or_else(|_| "good_till_canceled".to_string());
        let smarkets_session = env_opt("SMARKETS_SESSION");
        let ibkr_api_key = env_opt("IBKR_API_KEY");
        let odds_api_key = env_opt("ODDS_API_KEY").or_else(|| env_opt("ODDSAPI_KEY"));
        let odds_api_sport = env::var("ODDS_API_SPORT")
            .unwrap_or_else(|_| "basketball_nba".to_string());
        let cftc_url = env_opt("CFTC_URL");
        let robinhood_predicts_url = env_opt("ROBINHOOD_PREDICTS_URL");

        let enable_gui = env_bool("ENABLE_GUI", false);
        let exploration_epsilon = env_f64("EXPLORATION_EPSILON", 0.0);
        let exploration_fraction = env_f64("EXPLORATION_FRACTION", 0.01);
        let enable_rl = env_bool("ENABLE_RL", false);
        let rl_epsilon = env_f64("RL_EPSILON", 0.2);
        let rl_alpha = env_f64("RL_ALPHA", 0.1);
        let rl_gamma = env_f64("RL_GAMMA", 0.9);
        let enable_pq_signing = env_bool("ENABLE_PQ_SIGNING", false);
        let enable_federated = env_bool("ENABLE_FEDERATED", false);
        let federated_cluster_id = env::var("FEDERATED_CLUSTER_ID")
            .unwrap_or_else(|_| "default-cluster".to_string());
        let federated_peer_id = env_opt("FEDERATED_PEER_ID").unwrap_or_else(|| {
            env_opt("COMPUTERNAME")
                .or_else(|| env_opt("HOSTNAME"))
                .unwrap_or_else(|| format!("agent-{}", std::process::id()))
        });
        let federated_shared_key = env_opt("FEDERATED_SHARED_KEY");
        let federated_sync_dir = env::var("FEDERATED_SYNC_DIR")
            .unwrap_or_else(|_| ".federated_sync".to_string());
        let federated_min_peer_updates = env_usize("FEDERATED_MIN_PEER_UPDATES", 1).max(1);
        let federated_max_snapshot_age_secs =
            env_u64("FEDERATED_MAX_SNAPSHOT_AGE_SECS", 3600).max(30);
        let federated_secure_aggregation = env_bool("FEDERATED_SECURE_AGGREGATION", true);
        let federated_local_blend = env_f64("FEDERATED_LOCAL_BLEND", 0.35).clamp(0.05, 1.0);
        let enable_ev_scheduler = env_bool("ENABLE_EV_SCHEDULER", true);
        let scheduler_cycle_token_budget = env_u64(
            "SCHEDULER_CYCLE_TOKEN_BUDGET",
            anthropic_input_tokens_per_minute.max(1),
        )
        .max(1);
        let scheduler_ev_floor = env_f64("SCHEDULER_EV_FLOOR", 0.01).clamp(0.0, 1.0);
        let scheduler_max_candidates = env_usize(
            "SCHEDULER_MAX_CANDIDATES",
            anthropic_max_markets_per_cycle.saturating_mul(4),
        )
        .max(1);
        let scheduler_urgent_window_hours =
            env_i64("SCHEDULER_URGENT_WINDOW_HOURS", 48).clamp(1, 24 * 14);
        let scheduler_timezone_offset_hours =
            env_i32("SCHEDULER_TIMEZONE_OFFSET_HOURS", 0).clamp(-12, 14);
        let enable_arb_bridge = env_bool("ENABLE_ARB_BRIDGE", false);
        let arbitrum_rpc_url = env_opt("ARBITRUM_RPC_URL");
        let arbitrum_chain_id = env_u64("ARBITRUM_CHAIN_ID", 42161);
        let local_polls_url =
            env_opt("LOCAL_POLLS_URL").or_else(|| env_opt("TEXAS_TRIBUNE_POLLS_URL"));
        let enable_self_audit = env_bool("ENABLE_SELF_AUDIT", false);
        let self_audit_threshold = env_f64("SELF_AUDIT_THRESHOLD", 500.0);
        let self_audit_path = env_opt("SELF_AUDIT_PATH");
        let rl_offline_train = env_bool("RL_OFFLINE_TRAIN", false);
        let rl_offline_market_limit = env_usize("RL_OFFLINE_MARKET_LIMIT", 1000);
        let rl_offline_episodes = env_u64("RL_OFFLINE_EPISODES", 5000);
        let rl_offline_epsilon_start = env_f64("RL_OFFLINE_EPSILON_START", 0.3);
        let rl_offline_epsilon_decay = env_f64("RL_OFFLINE_EPSILON_DECAY", 0.995);
        let rl_replay_buffer = env_usize("RL_REPLAY_BUFFER", 10000);
        let rl_finetune_interval = env_u64("RL_FINETUNE_INTERVAL", 100);
        let rl_lr_slow = env_f64("RL_LR_SLOW", 1e-3);
        let rl_lr_fast = env_f64("RL_LR_FAST", 1e-2);
        let augur_rpc_url = env_opt("AUGUR_RPC_URL");
        let augur_contract = env_opt("AUGUR_CONTRACT");
        let augur_markets_url = env_opt("AUGUR_MARKETS_URL");

        Ok(Self {
            execution_venue,
            private_key,
            anthropic_key,
            anthropic_base_url,
            clob_api_key,
            clob_api_secret,
            clob_api_passphrase,
            polygon_rpc_url,
            shadow_mode,
            shadow_balance_override,
            loop_interval_secs,
            market_fetch_limit,
            anthropic_model,
            anthropic_version,
            anthropic_max_tokens,
            anthropic_prompt_max_chars,
            anthropic_max_markets_per_cycle,
            anthropic_rate_limit_cooldown_secs,
            anthropic_input_tokens_per_minute,
            weather_fast_path,
            weather_station_weight,
            weather_station_scale_f,
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
            clob_exchange_address,
            clob_taker_address,
            clob_fee_rate_bps,
            clob_nonce,
            clob_expiration_secs,
            clob_order_type,
            clob_post_only,
            clob_tick_size,
            clob_signature_type,
            clob_neg_risk,
            clob_funder,
            betfair_app_key,
            betfair_session,
            betfair_event_type_ids,
            kalshi_api_key,
            kalshi_key_id,
            kalshi_private_key_b64,
            kalshi_base_url,
            kalshi_post_only,
            kalshi_time_in_force,
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
            federated_cluster_id,
            federated_peer_id,
            federated_shared_key,
            federated_sync_dir,
            federated_min_peer_updates,
            federated_max_snapshot_age_secs,
            federated_secure_aggregation,
            federated_local_blend,
            enable_ev_scheduler,
            scheduler_cycle_token_budget,
            scheduler_ev_floor,
            scheduler_max_candidates,
            scheduler_urgent_window_hours,
            scheduler_timezone_offset_hours,
            enable_arb_bridge,
            arbitrum_rpc_url,
            arbitrum_chain_id,
            local_polls_url,
            enable_self_audit,
            self_audit_threshold,
            self_audit_path,
            rl_offline_train,
            rl_offline_market_limit,
            rl_offline_episodes,
            rl_offline_epsilon_start,
            rl_offline_epsilon_decay,
            rl_replay_buffer,
            rl_finetune_interval,
            rl_lr_slow,
            rl_lr_fast,
            augur_rpc_url,
            augur_contract,
            augur_markets_url,
        })
    }

    pub fn wallet(&self) -> Result<LocalWallet, AgentError> {
        if self.private_key.trim().is_empty() {
            return Err(AgentError::Wallet("PRIVATE_KEY missing".to_string()));
        }
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

fn env_opt(key: &str) -> Option<String> {
    if let Ok(value) = env::var(key) {
        if let Some(normalized) = normalize_env_value(&value) {
            if normalized.starts_with("-----BEGIN ") && !normalized.contains("-----END ") {
                return read_env_value_from_file(key).or(Some(normalized));
            }
            return Some(normalized);
        }
    }
    read_env_value_from_file(key)
}

fn read_env_value_from_file(key: &str) -> Option<String> {
    let contents = fs::read_to_string(".env").ok()?;
    let lines: Vec<&str> = contents.lines().collect();
    let mut idx = 0usize;
    while idx < lines.len() {
        let line = lines[idx].trim();
        if line.is_empty() || line.starts_with('#') {
            idx += 1;
            continue;
        }
        let Some((k, raw_v)) = line.split_once('=') else {
            idx += 1;
            continue;
        };
        if k.trim() != key {
            idx += 1;
            continue;
        }

        let value = raw_v.trim();
        if value.starts_with("-----BEGIN ") && !value.contains("-----END ") {
            let mut block = String::from(value);
            idx += 1;
            while idx < lines.len() {
                let next = lines[idx].trim_end();
                block.push('\n');
                block.push_str(next);
                if next.contains("-----END ") {
                    break;
                }
                idx += 1;
            }
            return normalize_env_value(&block);
        }
        return normalize_env_value(value);
    }
    None
}

fn normalize_env_value(input: &str) -> Option<String> {
    let mut value = input.trim().trim_matches('"').trim_matches('\'').trim().to_string();
    if value.is_empty() {
        return None;
    }
    if value.contains("\\n") {
        value = value.replace("\\n", "\n");
    }

    let path = Path::new(&value);
    if path.exists() && path.is_file() {
        if let Ok(contents) = fs::read_to_string(path) {
            let trimmed = contents.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    Some(value)
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

fn env_i32(key: &str, default: i32) -> i32 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
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

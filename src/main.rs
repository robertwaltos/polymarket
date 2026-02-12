mod ai_engine;
mod audit;
mod bridge;
mod clob_client;
mod config;
mod error;
mod execution;
mod federated;
mod gamma_api;
mod kalshi_client;
mod risk_engine;
mod rl_agent;
mod rl_training;
mod scraper_engine;
mod state;
mod strategy;
mod types;
mod utils;

#[cfg(feature = "gui")]
mod gui;

use crate::audit::self_audit;
use crate::bridge::BridgeClient;
use crate::config::Config;
use crate::error::AgentError;
use crate::execution::{ExecutionClient, ExecutionVenue};
use crate::federated::FederatedClient;
use crate::gamma_api::GammaClient;
use crate::risk_engine::TradeSide;
use crate::rl_agent::QLearningAgent;
use crate::rl_training::offline_train;
use crate::scraper_engine::ScraperEngine;
use crate::state::AgentState;
use crate::strategy::Strategy;
use crate::types::MarketNiche;
use log::{error, info, warn};
use reqwest::Client;
use std::io::Write;
use std::collections::VecDeque;
use std::env;
use tokio::time::{interval, Duration, Instant};

#[cfg(feature = "gui")]
type GuiHandleParam = Option<gui::GuiHandle>;
#[cfg(not(feature = "gui"))]
type GuiHandleParam = ();

// Docker deploy notes (Hetzner/Linode):
// 1) Build: docker build -t polymarket-agent .
// 2) Run: docker run --env-file .env --restart unless-stopped polymarket-agent
// 3) Provide a POLYGON_RPC_URL and PRIVATE_KEY in .env, keep SHADOW_MODE=true for dry runs.

fn main() -> Result<(), AgentError> {
    dotenv::dotenv().ok();
    init_logging();

    if maybe_run_kalshi_health_check()? {
        return Ok(());
    }

    if maybe_run_anthropic_health_check()? {
        return Ok(());
    }

    let config = Config::from_env()?;
    #[cfg(feature = "gui")]
    if config.enable_gui {
        let (gui_handle, gui_flags) = gui::create_gui(gui::GuiState::new(config.starting_balance_usdc));
        let config_clone = config.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to build Tokio runtime");
            if let Err(err) = runtime.block_on(run_agent(config_clone, Some(gui_handle))) {
                eprintln!("[ERROR] Agent loop crashed: {err}");
            }
        });

        gui::run_gui(gui_flags);
        std::process::exit(0);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| AgentError::Internal(err.to_string()))?;
    #[cfg(feature = "gui")]
    {
        runtime.block_on(run_agent(config, None))
    }
    #[cfg(not(feature = "gui"))]
    {
        runtime.block_on(run_agent(config, ()))
    }
}

fn maybe_run_kalshi_health_check() -> Result<bool, AgentError> {
    let args: Vec<String> = env::args().collect();
    if !args.iter().any(|arg| arg == "--kalshi-health" || arg == "kalshi-health") {
        return Ok(false);
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| AgentError::Internal(e.to_string()))?;

    let client = crate::kalshi_client::KalshiClient::from_env_for_health()?;
    let balance = runtime.block_on(client.get_cash_balance())?;
    println!("[HEALTH] Kalshi OK. cash_balance=${:.2}", balance);
    Ok(true)
}

fn maybe_run_anthropic_health_check() -> Result<bool, AgentError> {
    let args: Vec<String> = env::args().collect();
    if !args.iter().any(|arg| arg == "--anthropic-health" || arg == "anthropic-health") {
        return Ok(false);
    }

    let api_key = env::var("ANTHROPIC_KEY")
        .or_else(|_| env::var("ANTHROPIC_API_KEY"))
        .map_err(AgentError::Env)?;
    let version = env::var("ANTHROPIC_VERSION").unwrap_or_else(|_| "2023-06-01".to_string());
    let base_url =
        env::var("ANTHROPIC_BASE_URL").unwrap_or_else(|_| "https://api.anthropic.com".to_string());
    let configured_model = env::var("ANTHROPIC_MODEL")
        .unwrap_or_else(|_| "claude-sonnet-4-5-20250929".to_string());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| AgentError::Internal(e.to_string()))?;

    let report = runtime.block_on(anthropic_health_check(
        &api_key,
        &version,
        &base_url,
        &configured_model,
    ))?;
    println!("[HEALTH] Anthropic OK. models_available={}", report.count);
    if report.configured_model_supported {
        println!(
            "[HEALTH] ANTHROPIC_MODEL is valid: {}",
            configured_model
        );
    } else if let Some(suggested) = report.suggested_model {
        println!(
            "[HEALTH] ANTHROPIC_MODEL '{}' not found. Suggested: '{}'",
            configured_model, suggested
        );
    } else {
        println!(
            "[HEALTH] ANTHROPIC_MODEL '{}' not found in account model list.",
            configured_model
        );
    }
    Ok(true)
}

async fn anthropic_health_check(
    api_key: &str,
    version: &str,
    base_url: &str,
    configured_model: &str,
) -> Result<AnthropicHealthReport, AgentError> {
    let client = Client::builder()
        .user_agent("polymarket-agent/0.1")
        .timeout(Duration::from_secs(15))
        .build()?;

    let base = base_url.trim_end_matches('/');
    let resp = client
        .get(format!("{}/v1/models", base))
        .header("x-api-key", api_key)
        .header("anthropic-version", version)
        .header("content-type", "application/json")
        .send()
        .await?
        .error_for_status()?;

    let body: serde_json::Value = resp.json().await?;
    let models = body["data"]
        .as_array()
        .ok_or_else(|| AgentError::Anthropic("Missing data array".to_string()))?
        .iter()
        .filter_map(|item| item["id"].as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();

    let configured_model_supported = models.iter().any(|m| m == configured_model);
    let suggested_model = if configured_model_supported {
        None
    } else {
        models
            .iter()
            .filter(|m| m.to_ascii_lowercase().contains("sonnet"))
            .max()
            .cloned()
            .or_else(|| models.first().cloned())
    };

    Ok(AnthropicHealthReport {
        count: models.len() as u64,
        configured_model_supported,
        suggested_model,
    })
}

struct AnthropicHealthReport {
    count: u64,
    configured_model_supported: bool,
    suggested_model: Option<String>,
}

async fn run_agent(
    config: Config,
    #[cfg(feature = "gui")] gui_handle: GuiHandleParam,
    #[cfg(not(feature = "gui"))] _gui_handle: GuiHandleParam,
) -> Result<(), AgentError> {

    let http_client = Client::builder()
        .user_agent("polymarket-agent/0.1")
        .timeout(Duration::from_secs(20))
        .build()?;

    let gamma = GammaClient::new(http_client.clone(), "https://gamma-api.polymarket.com");
    let scraper = ScraperEngine::new(http_client.clone(), config.clone());
    let strategy = Strategy::new(http_client.clone(), config.clone());
    let execution = match config.execution_venue {
        ExecutionVenue::Polymarket => {
            let wallet = config.wallet()?;
            ExecutionClient::Polymarket(crate::clob_client::ClobClient::new(
                http_client.clone(),
                config.clone(),
                wallet,
            )?)
        }
        ExecutionVenue::Kalshi => ExecutionClient::Kalshi(crate::kalshi_client::KalshiClient::new(
            http_client.clone(),
            config.clone(),
        )?),
    };
    let mut state = AgentState::new(config.starting_balance_usdc);
    let bridge = BridgeClient::new(config.enable_arb_bridge, config.arbitrum_chain_id);
    let federated = FederatedClient::new(config.enable_federated);

    let mut paused = false;
    let mut edge_override: Option<f64> = None;
    let mut exploration_epsilon = config.exploration_epsilon;
    let exploration_fraction = config.exploration_fraction;
    let mut dynamic_threshold = 0.08_f64;
    let mut last_audit: Option<Instant> = None;
    let mut shadow_bankroll_override: Option<f64> = None;
    let mut trade_count: u64 = 0;
    let mut claude_cooldown_until: Option<Instant> = None;
    let mut anthropic_input_window: VecDeque<(Instant, u64)> = VecDeque::new();
    let mut rl_agent = if config.enable_rl {
        Some(QLearningAgent::new(
            config.rl_epsilon,
            config.rl_alpha,
            config.rl_gamma,
        ))
    } else {
        None
    };

    if config.shadow_mode {
        if let Some(override_val) = config.shadow_balance_override {
            shadow_bankroll_override = Some(override_val);
            state.starting_balance = override_val;
            state.update_bankroll(override_val);
            info!("Shadow bankroll override set to ${:.2}", override_val);
        }
    } else if config.shadow_balance_override.is_some() {
        warn!("SHADOW_BALANCE_OVERRIDE ignored (SHADOW_MODE=false)");
    }

    if config.enable_rl && config.rl_offline_train {
        if let Some(agent) = rl_agent.as_mut() {
            if let Err(err) = offline_train(&config, &gamma, agent).await {
                warn!("Offline RL training failed: {}", err);
            }
        }
    }

    let mut ticker = interval(Duration::from_secs(config.loop_interval_secs));

    loop {
        ticker.tick().await;

        #[cfg(feature = "gui")]
        if let Some(handle) = gui_handle.as_ref() {
            while let Some(cmd) = handle.try_recv_command() {
                match cmd {
                    gui::GuiCommand::TogglePause => {
                        paused = !paused;
                    }
                    gui::GuiCommand::SetEpsilon(val) => {
                        exploration_epsilon = val;
                    }
                    gui::GuiCommand::SetThreshold(val) => {
                        edge_override = Some(val);
                    }
                    gui::GuiCommand::ClearThreshold => {
                        edge_override = None;
                    }
                    gui::GuiCommand::ResetBankroll(value) => {
                        if config.shadow_mode {
                            shadow_bankroll_override = Some(value);
                            state.starting_balance = value;
                            state.update_bankroll(value);
                            info!("Shadow bankroll reset to ${:.2}", value);
                        } else {
                            warn!("Bankroll reset ignored (not in shadow mode)");
                        }
                    }
                }
            }
        }

        if paused {
            continue;
        }

        if let Some(until) = claude_cooldown_until {
            if Instant::now() < until {
                let wait = until.saturating_duration_since(Instant::now()).as_secs();
                warn!(
                    "Claude is in cooldown due to rate limits. Skipping cycle ({}s remaining).",
                    wait
                );
                continue;
            }
            claude_cooldown_until = None;
        }

        let balance = if config.shadow_mode {
            shadow_bankroll_override.unwrap_or(state.bankroll.max(config.starting_balance_usdc))
        } else {
            match execution.get_balance_usd().await {
                Ok(val) => val,
                Err(err) => {
                    warn!("Failed to fetch on-chain balance: {}", err);
                    state.bankroll
                }
            }
        };

        state.update_bankroll(balance);
        info!("Balance: ${:.2} USDC", state.bankroll);
        if state.bankroll <= 0.0 {
            shutdown_due_to_zero_balance();
        }

        #[cfg(feature = "gui")]
        if let Some(handle) = gui_handle.as_ref() {
            let display_threshold = edge_override.unwrap_or(dynamic_threshold);
            handle.update(gui::GuiState::with_params(
                state.bankroll,
                "Idle".to_string(),
                "Awaiting markets".to_string(),
                exploration_epsilon,
                display_threshold,
            ));
        }

        let markets = gamma.fetch_active_markets(config.market_fetch_limit).await?;
        let mut candidates: Vec<(crate::types::Market, MarketNiche)> = Vec::new();
        let mut include_politics = false;
        let mut include_betting = false;
        let mut include_local = false;
        let mut politics_hints: Vec<String> = Vec::new();

        for market in markets {
            if let Some(niche) = strategy.classify_niche(&market) {
                if niche == MarketNiche::Politics {
                    include_politics = true;
                    politics_hints.extend(extract_politics_hints(&market));
                }
                if matches!(niche, MarketNiche::Sports | MarketNiche::Betting | MarketNiche::Crypto)
                {
                    include_betting = true;
                }
                let text = market.market_text().to_lowercase();
                if text.contains("texas") || text.contains("tx ") {
                    include_local = true;
                }
                candidates.push((market, niche));
            }
        }

        if candidates.is_empty() {
            info!("No niche markets found in this cycle.");
            continue;
        }

        let external_data = scraper
            .fetch_all(include_politics, include_betting, include_local, &politics_hints)
            .await?;

        let mut claude_calls_this_cycle = 0usize;
        for (market, niche) in candidates {
            let kalshi_match = strategy.kalshi_match(&market, &external_data);
            let predictit_match = strategy.predictit_match(&market, &external_data);

            if config.execution_venue == ExecutionVenue::Kalshi && kalshi_match.is_none() {
                continue;
            }

            let mut exec_market = market.clone();
            if config.execution_venue == ExecutionVenue::Kalshi {
                if let Some(kalshi) = kalshi_match.as_ref() {
                    exec_market.yes_price = Some(kalshi.probability);
                    exec_market.no_price = Some(1.0 - kalshi.probability);
                }
            }

            let ai_estimate = if let Some(fast_estimate) =
                strategy.weather_fast_estimate(&market, &external_data, niche)
            {
                info!("{}", fast_estimate.reasoning);
                fast_estimate
            } else {
                if claude_calls_this_cycle >= config.anthropic_max_markets_per_cycle {
                    info!(
                        "Claude budget reached for this cycle ({} markets).",
                        config.anthropic_max_markets_per_cycle
                    );
                    break;
                }

                let estimated_input_tokens = strategy
                    .estimate_input_tokens(
                        &market,
                        &external_data,
                        niche,
                        kalshi_match.as_ref(),
                        predictit_match.as_ref(),
                    )
                    .max(1);

                let used_input_tokens =
                    prune_anthropic_input_window(&mut anthropic_input_window, Instant::now());
                let limit = config.anthropic_input_tokens_per_minute;
                if used_input_tokens.saturating_add(estimated_input_tokens) > limit {
                    let until = next_anthropic_window_time(
                        &anthropic_input_window,
                        Instant::now(),
                        config.anthropic_rate_limit_cooldown_secs,
                    );
                    claude_cooldown_until = Some(until);
                    let wait = until.saturating_duration_since(Instant::now()).as_secs();
                    warn!(
                        "Local Anthropic TPM budget reached (used={} est_next={} limit={}). Cooling down {}s.",
                        used_input_tokens,
                        estimated_input_tokens,
                        limit,
                        wait
                    );
                    break;
                }

                claude_calls_this_cycle += 1;
                let ai_estimate = match strategy
                    .infer_probabilities(
                        &market,
                        &external_data,
                        niche,
                        kalshi_match.as_ref(),
                        predictit_match.as_ref(),
                    )
                    .await
                {
                    Ok(result) => result,
                    Err(err) => {
                        if let Some(retry_after_secs) = anthropic_retry_after_secs(&err) {
                            let wait_secs =
                                retry_after_secs.max(config.anthropic_rate_limit_cooldown_secs);
                            claude_cooldown_until =
                                Some(Instant::now() + Duration::from_secs(wait_secs));
                            warn!(
                                "Claude rate-limited. Cooling down for {}s and ending this cycle.",
                                wait_secs
                            );
                            break;
                        }
                        warn!("Claude failed for market {}: {}", market.id, err);
                        continue;
                    }
                };

                let consumed_input_tokens =
                    ai_estimate.input_tokens.max(estimated_input_tokens);
                record_anthropic_input_usage(
                    &mut anthropic_input_window,
                    Instant::now(),
                    consumed_input_tokens,
                );
                ai_estimate
            };
            state.apply_token_cost(ai_estimate.token_cost_usd);

            let yes_price = exec_market.yes_price.unwrap_or_default();
            let adjusted_yes = strategy.adjusted_ai_probability(
                &market,
                &ai_estimate,
                &external_data,
                niche,
                kalshi_match.as_ref(),
                predictit_match.as_ref(),
            );

            let auto_threshold = dynamic_threshold.clamp(0.03, 0.12);
            let threshold = edge_override.unwrap_or(auto_threshold);

            let Some(risk_params) = strategy.build_risk_params(
                &exec_market,
                adjusted_yes,
                state.bankroll,
                niche,
                Some(threshold),
            ) else {
                warn!("Market {} missing prices, skipping", market.id);
                continue;
            };

            // As requested: main calls risk_engine::analyze_opportunity after Claude JSON.
            let mut signal = risk_engine::analyze_opportunity(risk_params);
            if signal.side == TradeSide::None {
                if let Some(explore) = strategy.maybe_exploration_trade(
                    &exec_market,
                    &ai_estimate,
                    state.bankroll,
                    exploration_epsilon,
                    exploration_fraction,
                ) {
                    info!("Exploration: {}", explore.reasoning);
                    let Some(limit_price) =
                        strategy.limit_price_for_signal(&exec_market, &explore)
                    else {
                        warn!("Could not determine limit price for {} (explore)", market.id);
                        continue;
                    };

                    if let Err(err) = execution
                        .place_limit_order(&market, &explore, limit_price, kalshi_match.as_ref())
                        .await
                    {
                        error!("Exploration order failed for market {}: {}", market.id, err);
                        continue;
                    }
                } else {
                    info!("No trade: {}", signal.reasoning);
                }
                continue;
            }

            info!("Market: \"{}\"", market.question);
            info!("Niche: {}", niche.as_str());
            info!(
                "Market Price: {:.2} | AI Fair Value: {:.2} | Adjusted: {:.2}",
                yes_price,
                ai_estimate.yes_probability,
                adjusted_yes
            );
            if let Some(kalshi) = kalshi_match.as_ref() {
                let diff = (kalshi.probability - yes_price).abs();
                if diff >= 0.04 {
                    info!(
                        "Kalshi diff {:.1}% (Kalshi {:.2}, Polly {:.2}, vol {:.0}, ticker {})",
                        diff * 100.0,
                        kalshi.probability,
                        yes_price,
                        kalshi.volume,
                        kalshi.ticker
                    );
                }
            }
            if let Some(predictit) = predictit_match.as_ref() {
                let diff = (predictit.yes_price - yes_price).abs();
                if diff >= 0.04 {
                    info!(
                        "PredictIt diff {:.1}% (PredictIt {:.2}, Polly {:.2}, match {:.2})",
                        diff * 100.0,
                        predictit.yes_price,
                        yes_price,
                        predictit.score
                    );
                }
            }
            info!("Opportunity Found: {}", signal.reasoning);

            let Some(limit_price) = strategy.limit_price_for_signal(&exec_market, &signal) else {
                warn!("Could not determine limit price for {}", market.id);
                continue;
            };

            if let Some(agent) = rl_agent.as_mut() {
                let edge = (adjusted_yes - yes_price).abs();
                let decision = agent.decide(edge, state.bankroll);
                let adjusted = (signal.size_usdc * decision.multiplier)
                    .min(state.bankroll * 0.06);
                let adjusted = (adjusted * 100.0).floor() / 100.0;
                if adjusted >= 1.0 {
                    signal.size_usdc = adjusted;
                }

                let reward = edge * signal.size_usdc;
                agent.update(decision.state, decision.action, reward, decision.state);
                let (mean, std_dev) = state.record_reward(reward);
                if std_dev > 0.0 {
                    let sharpe = mean / std_dev;
                    if sharpe > 0.2 {
                        dynamic_threshold = (dynamic_threshold - 0.002).max(0.03);
                    } else if sharpe < 0.0 {
                        dynamic_threshold = (dynamic_threshold + 0.002).min(0.12);
                    }
                }
            }

            if let Err(err) = execution
                .place_limit_order(&market, &signal, limit_price, kalshi_match.as_ref())
                .await
            {
                error!("Order failed for market {}: {}", market.id, err);
                continue;
            }

            trade_count += 1;
            if let Some(agent) = rl_agent.as_mut() {
                if trade_count % config.rl_finetune_interval == 0 {
                    let alpha = if state.realized_pnl < 0.0 {
                        config.rl_lr_fast
                    } else {
                        config.rl_lr_slow
                    };
                    agent.set_alpha(alpha);
                }
            }

            if config.enable_arb_bridge && !config.shadow_mode && state.realized_pnl >= 100.0 {
                if let Some(kalshi) = kalshi_match.as_ref() {
                    let diff = (kalshi.probability - yes_price).abs();
                    if diff >= 0.04 && kalshi.volume >= 1000.0 {
                        let _ = bridge
                            .bridge_usdc_polygon_to_arbitrum(signal.size_usdc)
                            .await;
                    }
                }
            }

            let updated_balance = if config.shadow_mode {
                shadow_bankroll_override
                    .unwrap_or(state.bankroll.max(config.starting_balance_usdc))
            } else {
                match execution.get_balance_usd().await {
                    Ok(val) => val,
                    Err(err) => {
                        warn!("Failed to refresh balance: {}", err);
                        state.bankroll
                    }
                }
            };

            state.update_bankroll(updated_balance);
            if state.bankroll <= 0.0 {
                shutdown_due_to_zero_balance();
            }

            if config.enable_federated {
                if let Some(agent) = rl_agent.as_ref() {
                    let _ = federated.sync(agent);
                }
            }

            if config.enable_self_audit && state.realized_pnl >= config.self_audit_threshold {
                let should_run = match last_audit {
                    Some(last) => last.elapsed() > Duration::from_secs(3600),
                    None => true,
                };
                if should_run {
                    if let Err(err) = self_audit(&config, &http_client).await {
                        warn!("Self-audit failed: {}", err);
                    }
                    last_audit = Some(Instant::now());
                }
            }

            #[cfg(feature = "gui")]
            if let Some(handle) = gui_handle.as_ref() {
                let display_threshold = edge_override.unwrap_or(dynamic_threshold);
                handle.update(gui::GuiState::with_params(
                    state.bankroll,
                    market.question.clone(),
                    signal.reasoning.clone(),
                    exploration_epsilon,
                    display_threshold,
                ));
            }
        }
    }
}

fn anthropic_retry_after_secs(err: &AgentError) -> Option<u64> {
    let AgentError::Anthropic(message) = err else {
        return None;
    };

    let lower = message.to_ascii_lowercase();
    if !(lower.contains("429") || lower.contains("rate_limit_error")) {
        return None;
    }

    if let Some(idx) = lower.find("retry_after=") {
        let tail = &lower[idx + "retry_after=".len()..];
        let digits: String = tail.chars().take_while(|c| c.is_ascii_digit()).collect();
        if let Ok(seconds) = digits.parse::<u64>() {
            return Some(seconds.max(1));
        }
    }

    Some(60)
}

fn prune_anthropic_input_window(
    window: &mut VecDeque<(Instant, u64)>,
    now: Instant,
) -> u64 {
    while let Some((ts, _)) = window.front() {
        if now.duration_since(*ts) >= Duration::from_secs(60) {
            window.pop_front();
        } else {
            break;
        }
    }
    window.iter().map(|(_, tokens)| *tokens).sum()
}

fn record_anthropic_input_usage(
    window: &mut VecDeque<(Instant, u64)>,
    now: Instant,
    tokens: u64,
) {
    if tokens == 0 {
        return;
    }
    window.push_back((now, tokens));
}

fn next_anthropic_window_time(
    window: &VecDeque<(Instant, u64)>,
    now: Instant,
    fallback_secs: u64,
) -> Instant {
    if let Some((first_ts, _)) = window.front() {
        let end = *first_ts + Duration::from_secs(60);
        if end > now {
            return end;
        }
    }
    now + Duration::from_secs(fallback_secs.max(5))
}

fn init_logging() {
    let mut builder = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info"),
    );
    builder.format(|buf, record| {
        writeln!(buf, "[{}] {}", record.level(), record.args())
    });
    builder.init();
}

fn shutdown_due_to_zero_balance() -> ! {
    println!("[CRITICAL] Balance: $0.00. Game Over.");
    println!("[SYSTEM] Process Terminated.");
    std::process::exit(1);
}

fn extract_politics_hints(market: &crate::types::Market) -> Vec<String> {
    let text = market.market_text().to_lowercase();
    let mut hints = Vec::new();

    let race_keywords = [
        "president",
        "senate",
        "house",
        "governor",
        "primary",
        "nomination",
        "generic",
        "congress",
    ];
    for keyword in race_keywords {
        if text.contains(keyword) {
            hints.push(keyword.to_string());
        }
    }

    let states = [
        ("alabama", "al"),
        ("alaska", "ak"),
        ("arizona", "az"),
        ("arkansas", "ar"),
        ("california", "ca"),
        ("colorado", "co"),
        ("connecticut", "ct"),
        ("delaware", "de"),
        ("florida", "fl"),
        ("georgia", "ga"),
        ("hawaii", "hi"),
        ("idaho", "id"),
        ("illinois", "il"),
        ("indiana", "in"),
        ("iowa", "ia"),
        ("kansas", "ks"),
        ("kentucky", "ky"),
        ("louisiana", "la"),
        ("maine", "me"),
        ("maryland", "md"),
        ("massachusetts", "ma"),
        ("michigan", "mi"),
        ("minnesota", "mn"),
        ("mississippi", "ms"),
        ("missouri", "mo"),
        ("montana", "mt"),
        ("nebraska", "ne"),
        ("nevada", "nv"),
        ("new hampshire", "nh"),
        ("new jersey", "nj"),
        ("new mexico", "nm"),
        ("new york", "ny"),
        ("north carolina", "nc"),
        ("north dakota", "nd"),
        ("ohio", "oh"),
        ("oklahoma", "ok"),
        ("oregon", "or"),
        ("pennsylvania", "pa"),
        ("rhode island", "ri"),
        ("south carolina", "sc"),
        ("south dakota", "sd"),
        ("tennessee", "tn"),
        ("texas", "tx"),
        ("utah", "ut"),
        ("vermont", "vt"),
        ("virginia", "va"),
        ("washington", "wa"),
        ("west virginia", "wv"),
        ("wisconsin", "wi"),
        ("wyoming", "wy"),
    ];

    for (state, abbr) in states {
        if text.contains(state) {
            hints.push(state.to_string());
            hints.push(state.replace(' ', "-"));
        }
        if text.contains(&format!(" {} ", abbr))
            || text.ends_with(&format!(" {}", abbr))
            || text.starts_with(&format!("{} ", abbr))
        {
            hints.push(state.to_string());
            hints.push(state.replace(' ', "-"));
            hints.push(abbr.to_string());
        }
    }

    hints.sort();
    hints.dedup();
    hints
}

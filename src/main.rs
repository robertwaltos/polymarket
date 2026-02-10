mod ai_engine;
mod audit;
mod bridge;
mod clob_client;
mod config;
mod error;
mod federated;
mod gamma_api;
mod risk_engine;
mod rl_agent;
mod scraper_engine;
mod state;
mod strategy;
mod types;
mod utils;

#[cfg(feature = "gui")]
mod gui;

use crate::audit::self_audit;
use crate::bridge::BridgeClient;
use crate::clob_client::ClobClient;
use crate::config::Config;
use crate::error::AgentError;
use crate::federated::FederatedClient;
use crate::gamma_api::GammaClient;
use crate::risk_engine::TradeSide;
use crate::rl_agent::QLearningAgent;
use crate::scraper_engine::ScraperEngine;
use crate::state::AgentState;
use crate::strategy::Strategy;
use crate::types::MarketNiche;
use log::{error, info, warn};
use reqwest::Client;
use std::io::Write;
use tokio::time::{interval, Duration, Instant};

// Docker deploy notes (Hetzner/Linode):
// 1) Build: docker build -t polymarket-agent .
// 2) Run: docker run --env-file .env --restart unless-stopped polymarket-agent
// 3) Provide a POLYGON_RPC_URL and PRIVATE_KEY in .env, keep SHADOW_MODE=true for dry runs.

#[tokio::main]
async fn main() -> Result<(), AgentError> {
    dotenv::dotenv().ok();
    init_logging();

    let config = Config::from_env()?;
    let wallet = config.wallet()?;

    let http_client = Client::builder()
        .user_agent("polymarket-agent/0.1")
        .timeout(Duration::from_secs(20))
        .build()?;

    let gamma = GammaClient::new(http_client.clone(), "https://gamma-api.polymarket.com");
    let scraper = ScraperEngine::new(http_client.clone(), config.clone());
    let strategy = Strategy::new(http_client.clone(), config.clone());
    let clob = ClobClient::new(http_client.clone(), config.clone(), wallet)?;
    let mut state = AgentState::new(config.starting_balance_usdc);
    let bridge = BridgeClient::new(config.enable_arb_bridge, config.arbitrum_chain_id);
    let federated = FederatedClient::new(config.enable_federated);

    #[cfg(feature = "gui")]
    let mut gui_handle = if config.enable_gui {
        Some(gui::spawn_gui(gui::GuiState::new(state.bankroll)))
    } else {
        None
    };

    let mut paused = false;
    let mut edge_override: Option<f64> = None;
    let mut exploration_epsilon = config.exploration_epsilon;
    let exploration_fraction = config.exploration_fraction;
    let mut dynamic_threshold = 0.08_f64;
    let mut last_audit: Option<Instant> = None;
    let mut rl_agent = if config.enable_rl {
        Some(QLearningAgent::new(
            config.rl_epsilon,
            config.rl_alpha,
            config.rl_gamma,
        ))
    } else {
        None
    };

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
                }
            }
        }

        if paused {
            continue;
        }

        let balance = match clob.get_usdc_balance().await {
            Ok(val) => val,
            Err(err) => {
                warn!("Failed to fetch on-chain balance: {}", err);
                state.bankroll
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

        for market in markets {
            if let Some(niche) = strategy.classify_niche(&market) {
                if niche == MarketNiche::Politics {
                    include_politics = true;
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
            .fetch_all(include_politics, include_betting, include_local)
            .await?;

        for (market, niche) in candidates {
            let ai_estimate = match strategy
                .infer_probabilities(&market, &external_data, niche)
                .await
            {
                Ok(result) => result,
                Err(err) => {
                    warn!("Claude failed for market {}: {}", market.id, err);
                    continue;
                }
            };

            state.apply_token_cost(ai_estimate.token_cost_usd);

            let adjusted_yes =
                strategy.adjusted_ai_probability(&ai_estimate, &external_data, niche);

            let auto_threshold = dynamic_threshold.clamp(0.03, 0.12);
            let threshold = edge_override.unwrap_or(auto_threshold);

            let Some(risk_params) = strategy.build_risk_params(
                &market,
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
                    &market,
                    &ai_estimate,
                    state.bankroll,
                    exploration_epsilon,
                    exploration_fraction,
                ) {
                    info!("Exploration: {}", explore.reasoning);
                    let Some(limit_price) =
                        strategy.limit_price_for_signal(&market, &explore)
                    else {
                        warn!("Could not determine limit price for {} (explore)", market.id);
                        continue;
                    };

                    if let Err(err) = clob.place_limit_order(&market, &explore, limit_price).await
                    {
                        error!("Exploration order failed for market {}: {}", market.id, err);
                        continue;
                    }
                } else {
                    info!("No trade: {}", signal.reasoning);
                }
                continue;
            }

            let yes_price = market.yes_price.unwrap_or_default();
            info!("Market: \"{}\"", market.question);
            info!("Niche: {}", niche.as_str());
            info!(
                "Market Price: {:.2} | AI Fair Value: {:.2} | Adjusted: {:.2}",
                yes_price,
                ai_estimate.yes_probability,
                adjusted_yes
            );
            info!("Opportunity Found: {}", signal.reasoning);

            let Some(limit_price) = strategy.limit_price_for_signal(&market, &signal) else {
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

            if let Err(err) = clob.place_limit_order(&market, &signal, limit_price).await {
                error!("Order failed for market {}: {}", market.id, err);
                continue;
            }

            if config.enable_arb_bridge && !config.shadow_mode {
                let edge = (adjusted_yes - yes_price).abs();
                if edge >= 0.04 {
                    let _ = bridge
                        .bridge_usdc_polygon_to_arbitrum(signal.size_usdc)
                        .await;
                }
            }

            let updated_balance = match clob.get_usdc_balance().await {
                Ok(val) => val,
                Err(err) => {
                    warn!("Failed to refresh balance: {}", err);
                    state.bankroll
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

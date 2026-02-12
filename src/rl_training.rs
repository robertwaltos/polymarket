use crate::config::Config;
use crate::error::AgentError;
use crate::gamma_api::GammaClient;
use crate::rl_agent::QLearningAgent;
use crate::types::Market;
use log::info;
use rand::seq::SliceRandom;
use rand::Rng;

pub async fn offline_train(
    config: &Config,
    gamma: &GammaClient,
    agent: &mut QLearningAgent,
) -> Result<(), AgentError> {
    if !config.enable_rl || !config.rl_offline_train {
        return Ok(());
    }

    let markets = gamma
        .fetch_settled_markets(config.rl_offline_market_limit)
        .await?;
    if markets.is_empty() {
        info!("RL offline: no settled markets available.");
        return Ok(());
    }

    let mut rng = rand::thread_rng();
    let mut epsilon = config.rl_offline_epsilon_start;
    let episodes = config.rl_offline_episodes;

    for episode in 0..episodes {
        let market = markets.choose(&mut rng).unwrap_or(&markets[0]);
        let (edge, reward) = simulate_reward(market, &mut rng);

        agent.set_epsilon(epsilon);
        let decision = agent.decide(edge, config.starting_balance_usdc);
        agent.update(decision.state, decision.action, reward, decision.state);

        epsilon = (epsilon * config.rl_offline_epsilon_decay).clamp(0.01, 1.0);

        if episode % 500 == 0 {
            info!("RL offline episode {} / {}", episode, episodes);
        }
    }

    #[cfg(feature = "rl-tch")]
    {
        let samples = markets.len().min(config.rl_replay_buffer);
        train_dqn_stub(samples);
    }

    Ok(())
}

fn simulate_reward(market: &Market, rng: &mut impl Rng) -> (f64, f64) {
    let yes_price = market.yes_price.unwrap_or(0.5).clamp(0.01, 0.99);
    let noise: f64 = rng.gen_range(-0.05..0.05);
    let fair = (0.5 + noise).clamp(0.01, 0.99);
    let edge = (fair - yes_price).abs();

    let pnl = edge * 100.0;
    let volatility_penalty = edge * edge * 50.0;
    let reward = pnl - volatility_penalty;

    (edge, reward)
}

#[cfg(feature = "rl-tch")]
fn train_dqn_stub(samples: usize) {
    use tch::{nn, Device, Kind, Tensor};

    let vs = nn::VarStore::new(Device::Cpu);
    let net = nn::seq()
        .add(nn::linear(
            &vs.root() / "layer1",
            3,
            16,
            Default::default(),
        ))
        .add_fn(|x| x.relu())
        .add(nn::linear(
            &vs.root() / "layer2",
            16,
            3,
            Default::default(),
        ));

    let input = Tensor::randn([samples as i64, 3], (Kind::Float, Device::Cpu));
    let output = net.forward(&input);
    let _ = output.sum(Kind::Float).double_value(&[]);
}

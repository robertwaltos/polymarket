#[derive(Debug, Clone)]
pub struct AgentState {
    pub starting_balance: f64,
    pub bankroll: f64,
    pub realized_pnl: f64,
    pub token_cost_usd: f64,
    pub recent_rewards: Vec<f64>,
}

impl AgentState {
    pub fn new(starting_balance: f64) -> Self {
        Self {
            starting_balance,
            bankroll: starting_balance,
            realized_pnl: 0.0,
            token_cost_usd: 0.0,
            recent_rewards: Vec::new(),
        }
    }

    pub fn update_bankroll(&mut self, balance: f64) {
        self.bankroll = balance;
        self.realized_pnl = balance - self.starting_balance - self.token_cost_usd;
    }

    pub fn apply_token_cost(&mut self, cost_usd: f64) {
        if cost_usd <= 0.0 {
            return;
        }
        self.token_cost_usd += cost_usd;
        self.realized_pnl -= cost_usd;
    }

    pub fn record_reward(&mut self, reward: f64) -> (f64, f64) {
        self.recent_rewards.push(reward);
        if self.recent_rewards.len() > 50 {
            self.recent_rewards.remove(0);
        }

        let mean = if self.recent_rewards.is_empty() {
            0.0
        } else {
            self.recent_rewards.iter().sum::<f64>() / self.recent_rewards.len() as f64
        };

        let variance = if self.recent_rewards.len() < 2 {
            0.0
        } else {
            let mut sum = 0.0;
            for value in &self.recent_rewards {
                sum += (value - mean).powi(2);
            }
            sum / (self.recent_rewards.len() as f64 - 1.0)
        };
        let std_dev = variance.sqrt();

        (mean, std_dev)
    }
}

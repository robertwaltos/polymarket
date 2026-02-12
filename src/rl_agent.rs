use rand::Rng;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) struct StateKey {
    edge_bucket: i32,
    bankroll_bucket: i32,
}

#[derive(Debug, Clone, Copy)]
pub struct RlDecision {
    pub action: usize,
    pub multiplier: f64,
    pub state: StateKey,
}

pub struct QLearningAgent {
    q_table: HashMap<StateKey, [f64; 3]>,
    epsilon: f64,
    alpha: f64,
    gamma: f64,
}

impl QLearningAgent {
    pub fn new(epsilon: f64, alpha: f64, gamma: f64) -> Self {
        Self {
            q_table: HashMap::new(),
            epsilon,
            alpha,
            gamma,
        }
    }

    pub fn decide(&mut self, edge: f64, bankroll: f64) -> RlDecision {
        let state = StateKey {
            edge_bucket: bucket_edge(edge),
            bankroll_bucket: bucket_bankroll(bankroll),
        };
        let q_values = self.q_table.entry(state).or_insert([0.0; 3]);

        let mut rng = rand::thread_rng();
        let explore: f64 = rng.gen();
        let action = if explore < self.epsilon {
            rng.gen_range(0..3)
        } else {
            argmax(q_values)
        };

        let multiplier = match action {
            0 => 0.75,
            1 => 1.0,
            _ => 1.25,
        };

        RlDecision {
            action,
            multiplier,
            state,
        }
    }

    pub fn update(&mut self, state: StateKey, action: usize, reward: f64, next_state: StateKey) {
        let max_next = {
            let next_values = self.q_table.entry(next_state).or_insert([0.0; 3]);
            next_values
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max)
        };
        let q_values = self.q_table.entry(state).or_insert([0.0; 3]);
        let target = reward + self.gamma * max_next;
        let current = q_values[action];
        q_values[action] = current + self.alpha * (target - current);
    }

    pub fn set_alpha(&mut self, alpha: f64) {
        if alpha > 0.0 {
            self.alpha = alpha;
        }
    }

    pub fn set_epsilon(&mut self, epsilon: f64) {
        if (0.0..=1.0).contains(&epsilon) {
            self.epsilon = epsilon;
        }
    }
}

fn argmax(values: &[f64; 3]) -> usize {
    let mut best_idx = 0;
    let mut best_val = values[0];
    for (idx, val) in values.iter().enumerate().skip(1) {
        if *val > best_val {
            best_val = *val;
            best_idx = idx;
        }
    }
    best_idx
}

fn bucket_edge(edge: f64) -> i32 {
    let normalized = (edge * 100.0).clamp(0.0, 100.0);
    (normalized / 2.0).floor() as i32
}

fn bucket_bankroll(bankroll: f64) -> i32 {
    let normalized = (bankroll / 10.0).clamp(0.0, 20.0);
    normalized.floor() as i32
}

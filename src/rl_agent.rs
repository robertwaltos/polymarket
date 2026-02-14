use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedQEntry {
    pub edge_bucket: i32,
    pub bankroll_bucket: i32,
    pub q_values: [f64; 3],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QLearningSnapshot {
    pub policy_version: u64,
    pub training_steps: u64,
    pub entries: Vec<FederatedQEntry>,
}

pub struct QLearningAgent {
    q_table: HashMap<StateKey, [f64; 3]>,
    epsilon: f64,
    alpha: f64,
    gamma: f64,
    policy_version: u64,
    training_steps: u64,
}

impl QLearningAgent {
    pub fn new(epsilon: f64, alpha: f64, gamma: f64) -> Self {
        Self {
            q_table: HashMap::new(),
            epsilon,
            alpha,
            gamma,
            policy_version: 1,
            training_steps: 0,
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
        self.training_steps = self.training_steps.saturating_add(1);
        self.policy_version = self.policy_version.saturating_add(1);
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

    pub fn policy_version(&self) -> u64 {
        self.policy_version
    }

    pub fn snapshot(&self) -> QLearningSnapshot {
        let mut entries = self
            .q_table
            .iter()
            .map(|(state, values)| FederatedQEntry {
                edge_bucket: state.edge_bucket,
                bankroll_bucket: state.bankroll_bucket,
                q_values: *values,
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| (entry.edge_bucket, entry.bankroll_bucket));

        QLearningSnapshot {
            policy_version: self.policy_version,
            training_steps: self.training_steps.max(1),
            entries,
        }
    }

    pub fn merge_snapshot(&mut self, snapshot: &QLearningSnapshot, blend: f64) -> usize {
        let weight = blend.clamp(0.0, 1.0);
        if snapshot.entries.is_empty() {
            return 0;
        }

        let mut merged = 0usize;
        for entry in &snapshot.entries {
            let state = StateKey {
                edge_bucket: entry.edge_bucket,
                bankroll_bucket: entry.bankroll_bucket,
            };
            let local = self.q_table.entry(state).or_insert(entry.q_values);
            if weight < 1.0 {
                for (idx, local_value) in local.iter_mut().enumerate() {
                    *local_value = (*local_value * (1.0 - weight)) + (entry.q_values[idx] * weight);
                }
            } else {
                *local = entry.q_values;
            }
            merged = merged.saturating_add(1);
        }

        self.training_steps = self.training_steps.max(snapshot.training_steps);
        self.policy_version = self
            .policy_version
            .max(snapshot.policy_version)
            .saturating_add(1);
        merged
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

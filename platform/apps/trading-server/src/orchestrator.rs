use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorConfig {
    pub total_capital_usd: f64,
    pub starting_allocation_usd: f64,
    pub max_allocation_per_strategy_usd: f64,
    pub promote_multiplier: f64,
    pub promote_top_n: usize,
    pub kill_bottom_n: usize,
    pub min_trades_before_ranking: u64,
    pub kill_drawdown_pct: f64,
    pub kill_score_threshold: f64,
}

impl OrchestratorConfig {
    pub fn from_env() -> Self {
        Self {
            total_capital_usd: env_f64("ORCH_TOTAL_CAPITAL_USD", 100_000.0).max(1_000.0),
            starting_allocation_usd: env_f64("ORCH_STARTING_ALLOCATION_USD", 5_000.0).max(100.0),
            max_allocation_per_strategy_usd: env_f64("ORCH_MAX_ALLOCATION_USD", 40_000.0)
                .max(100.0),
            promote_multiplier: env_f64("ORCH_PROMOTE_MULTIPLIER", 1.25).clamp(1.01, 3.0),
            promote_top_n: env_u64("ORCH_PROMOTE_TOP_N", 2) as usize,
            kill_bottom_n: env_u64("ORCH_KILL_BOTTOM_N", 1) as usize,
            min_trades_before_ranking: env_u64("ORCH_MIN_TRADES", 20),
            kill_drawdown_pct: env_f64("ORCH_KILL_DRAWDOWN_PCT", 25.0).max(1.0),
            kill_score_threshold: env_f64("ORCH_KILL_SCORE_THRESHOLD", -20.0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyLifecycle {
    Active,
    Paused,
    Killed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyPerformance {
    pub total_return_pct: f64,
    pub sharpe: f64,
    pub max_drawdown_pct: f64,
    pub win_rate: f64,
    pub trades: u64,
    pub updated_at: DateTime<Utc>,
}

impl Default for StrategyPerformance {
    fn default() -> Self {
        Self {
            total_return_pct: 0.0,
            sharpe: 0.0,
            max_drawdown_pct: 0.0,
            win_rate: 0.5,
            trades: 0,
            updated_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyAgent {
    pub id: Uuid,
    pub name: String,
    pub venue: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub lifecycle: StrategyLifecycle,
    pub allocation_usd: f64,
    pub score: f64,
    pub rank: usize,
    pub last_score_note: Option<String>,
    pub performance: StrategyPerformance,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpsertStrategyAgentInput {
    #[serde(default)]
    pub id: Option<Uuid>,
    pub name: String,
    #[serde(default)]
    pub venue: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub active: Option<bool>,
    #[serde(default)]
    pub allocation_usd: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyPerformanceInput {
    pub total_return_pct: f64,
    pub sharpe: f64,
    pub max_drawdown_pct: f64,
    pub win_rate: f64,
    pub trades: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyAction {
    pub at: DateTime<Utc>,
    pub agent_id: Uuid,
    pub agent_name: String,
    pub action: String,
    pub reason: String,
    pub allocation_before_usd: f64,
    pub allocation_after_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagBias {
    pub tag: String,
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorSnapshot {
    pub config: OrchestratorConfig,
    pub evaluated_at: Option<DateTime<Utc>>,
    pub total_allocated_usd: f64,
    pub agents: Vec<StrategyAgent>,
    pub last_actions: Vec<StrategyAction>,
    pub active_tag_bias: Vec<TagBias>,
}

#[derive(Debug, Clone)]
struct OrchestratorState {
    agents: HashMap<Uuid, StrategyAgent>,
    evaluated_at: Option<DateTime<Utc>>,
    last_actions: Vec<StrategyAction>,
    active_tag_bias: HashMap<String, f64>,
}

#[derive(Clone)]
pub struct StrategyOrchestrator {
    config: OrchestratorConfig,
    state: Arc<Mutex<OrchestratorState>>,
}

impl StrategyOrchestrator {
    pub fn new(config: OrchestratorConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(OrchestratorState {
                agents: HashMap::new(),
                evaluated_at: None,
                last_actions: Vec::new(),
                active_tag_bias: HashMap::new(),
            })),
        }
    }

    pub fn upsert_agent(&self, input: UpsertStrategyAgentInput) -> Result<StrategyAgent> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("orchestrator lock poisoned"))?;
        let now = Utc::now();
        let id = input.id.unwrap_or_else(Uuid::new_v4);
        let tags = normalize_tags(input.tags);

        let entry = guard.agents.entry(id).or_insert_with(|| StrategyAgent {
            id,
            name: input.name.clone(),
            venue: normalize_text(input.venue.clone()),
            description: normalize_text(input.description.clone()),
            tags: tags.clone(),
            lifecycle: if input.active.unwrap_or(true) {
                StrategyLifecycle::Active
            } else {
                StrategyLifecycle::Paused
            },
            allocation_usd: input
                .allocation_usd
                .unwrap_or(self.config.starting_allocation_usd)
                .clamp(0.0, self.config.max_allocation_per_strategy_usd),
            score: 0.0,
            rank: 0,
            last_score_note: Some("new strategy".to_string()),
            performance: StrategyPerformance::default(),
            created_at: now,
            updated_at: now,
        });

        entry.name = input.name;
        entry.venue = normalize_text(input.venue);
        entry.description = normalize_text(input.description);
        entry.tags = tags;
        if let Some(active) = input.active {
            entry.lifecycle = if active {
                StrategyLifecycle::Active
            } else {
                StrategyLifecycle::Paused
            };
        }
        if let Some(allocation) = input.allocation_usd {
            entry.allocation_usd =
                allocation.clamp(0.0, self.config.max_allocation_per_strategy_usd);
        }
        entry.updated_at = now;

        let result = entry.clone();
        drop(guard);
        self.recompute_ranks(HashMap::new())?;
        Ok(result)
    }

    pub fn update_performance(
        &self,
        id: Uuid,
        input: StrategyPerformanceInput,
    ) -> Result<StrategyAgent> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("orchestrator lock poisoned"))?;
        let Some(agent) = guard.agents.get_mut(&id) else {
            return Err(anyhow!("strategy not found: {id}"));
        };
        agent.performance = StrategyPerformance {
            total_return_pct: input.total_return_pct,
            sharpe: input.sharpe,
            max_drawdown_pct: input.max_drawdown_pct.max(0.0),
            win_rate: input.win_rate.clamp(0.0, 1.0),
            trades: input.trades,
            updated_at: Utc::now(),
        };
        agent.updated_at = Utc::now();
        let result = agent.clone();
        drop(guard);
        self.recompute_ranks(HashMap::new())?;
        Ok(result)
    }

    pub fn evaluate(&self, tag_bias: HashMap<String, f64>) -> Result<OrchestratorSnapshot> {
        self.recompute_ranks(tag_bias.clone())?;
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("orchestrator lock poisoned"))?;
        let now = Utc::now();
        let mut actions = Vec::new();
        let promote_n = self.config.promote_top_n.max(1);
        let kill_n = self.config.kill_bottom_n;
        let min_trades = self.config.min_trades_before_ranking;

        let mut ranked: Vec<(Uuid, f64)> = guard
            .agents
            .iter()
            .filter(|(_, agent)| !matches!(agent.lifecycle, StrategyLifecycle::Killed))
            .map(|(id, agent)| (*id, agent.score))
            .collect();
        ranked.sort_by(|a, b| compare_f64_desc(a.1, b.1));

        for (idx, (id, _)) in ranked.iter().enumerate() {
            if idx >= promote_n {
                break;
            }
            if let Some(agent) = guard.agents.get_mut(id) {
                if agent.performance.trades < min_trades {
                    continue;
                }
                let before = agent.allocation_usd;
                let after = (before * self.config.promote_multiplier)
                    .clamp(0.0, self.config.max_allocation_per_strategy_usd);
                if after > before {
                    agent.allocation_usd = after;
                    if !matches!(agent.lifecycle, StrategyLifecycle::Killed) {
                        agent.lifecycle = StrategyLifecycle::Active;
                    }
                    agent.updated_at = now;
                    actions.push(StrategyAction {
                        at: now,
                        agent_id: agent.id,
                        agent_name: agent.name.clone(),
                        action: "promoted".to_string(),
                        reason: format!("rank {} with score {:.2}", idx + 1, agent.score),
                        allocation_before_usd: before,
                        allocation_after_usd: after,
                    });
                }
            }
        }

        if kill_n > 0 && ranked.len() > promote_n {
            let mut killed = 0usize;
            for (id, score) in ranked.iter().rev() {
                if killed >= kill_n {
                    break;
                }
                let Some(agent) = guard.agents.get_mut(id) else {
                    continue;
                };
                if matches!(agent.lifecycle, StrategyLifecycle::Killed) {
                    continue;
                }
                if agent.performance.trades < min_trades {
                    continue;
                }
                if *score > self.config.kill_score_threshold
                    && agent.performance.max_drawdown_pct < self.config.kill_drawdown_pct
                {
                    continue;
                }
                let before = agent.allocation_usd;
                agent.lifecycle = StrategyLifecycle::Killed;
                agent.allocation_usd = 0.0;
                agent.updated_at = now;
                actions.push(StrategyAction {
                    at: now,
                    agent_id: agent.id,
                    agent_name: agent.name.clone(),
                    action: "killed".to_string(),
                    reason: format!(
                        "bottom cohort score {:.2}, drawdown {:.2}%",
                        score, agent.performance.max_drawdown_pct
                    ),
                    allocation_before_usd: before,
                    allocation_after_usd: 0.0,
                });
                killed += 1;
            }
        }

        enforce_capital_budget(
            &mut guard.agents,
            self.config.total_capital_usd,
            self.config.max_allocation_per_strategy_usd,
            now,
            &mut actions,
        );

        guard.last_actions = actions;
        guard.evaluated_at = Some(now);
        guard.active_tag_bias = tag_bias;
        self.snapshot_from_guard(&guard)
    }

    pub fn snapshot(&self) -> Result<OrchestratorSnapshot> {
        let guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("orchestrator lock poisoned"))?;
        self.snapshot_from_guard(&guard)
    }

    pub fn load_snapshot(&self, snapshot: OrchestratorSnapshot) -> Result<()> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("orchestrator lock poisoned"))?;
        guard.agents = snapshot
            .agents
            .into_iter()
            .map(|agent| (agent.id, agent))
            .collect();
        guard.last_actions = snapshot.last_actions;
        guard.evaluated_at = snapshot.evaluated_at;
        guard.active_tag_bias = snapshot
            .active_tag_bias
            .into_iter()
            .map(|entry| (entry.tag, entry.weight))
            .collect();
        Ok(())
    }

    fn recompute_ranks(&self, tag_bias: HashMap<String, f64>) -> Result<()> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("orchestrator lock poisoned"))?;

        for agent in guard.agents.values_mut() {
            if matches!(agent.lifecycle, StrategyLifecycle::Killed) {
                agent.score = f64::NEG_INFINITY;
                agent.rank = usize::MAX;
                agent.last_score_note = Some("strategy killed".to_string());
                continue;
            }

            let (score, note) =
                compute_score(agent, self.config.min_trades_before_ranking, &tag_bias);
            agent.score = score;
            agent.last_score_note = Some(note);
        }

        let mut ranking: Vec<(Uuid, f64)> = guard
            .agents
            .iter()
            .filter(|(_, agent)| !matches!(agent.lifecycle, StrategyLifecycle::Killed))
            .map(|(id, agent)| (*id, agent.score))
            .collect();
        ranking.sort_by(|a, b| compare_f64_desc(a.1, b.1));
        for (idx, (id, _)) in ranking.iter().enumerate() {
            if let Some(agent) = guard.agents.get_mut(id) {
                agent.rank = idx + 1;
            }
        }
        if !tag_bias.is_empty() {
            guard.active_tag_bias = tag_bias;
        }
        Ok(())
    }

    fn snapshot_from_guard(&self, guard: &OrchestratorState) -> Result<OrchestratorSnapshot> {
        let mut agents: Vec<StrategyAgent> = guard.agents.values().cloned().collect();
        agents.sort_by(|a, b| match (a.rank, b.rank) {
            (ra, rb) if ra == rb => compare_f64_desc(a.score, b.score),
            (usize::MAX, usize::MAX) => compare_f64_desc(a.score, b.score),
            (usize::MAX, _) => Ordering::Greater,
            (_, usize::MAX) => Ordering::Less,
            _ => a.rank.cmp(&b.rank),
        });
        let total_allocated_usd: f64 = agents
            .iter()
            .filter(|agent| !matches!(agent.lifecycle, StrategyLifecycle::Killed))
            .map(|agent| agent.allocation_usd.max(0.0))
            .sum();
        let mut active_tag_bias: Vec<TagBias> = guard
            .active_tag_bias
            .iter()
            .map(|(tag, weight)| TagBias {
                tag: tag.clone(),
                weight: *weight,
            })
            .collect();
        active_tag_bias.sort_by(|a, b| compare_f64_desc(a.weight, b.weight));
        Ok(OrchestratorSnapshot {
            config: self.config.clone(),
            evaluated_at: guard.evaluated_at,
            total_allocated_usd,
            agents,
            last_actions: guard.last_actions.clone(),
            active_tag_bias,
        })
    }
}

fn compute_score(
    agent: &StrategyAgent,
    min_trades: u64,
    tag_bias: &HashMap<String, f64>,
) -> (f64, String) {
    let perf = &agent.performance;
    let ret_component = perf.total_return_pct * 0.45;
    let sharpe_component = perf.sharpe * 14.0;
    let win_component = (perf.win_rate.clamp(0.0, 1.0) - 0.5) * 30.0;
    let drawdown_penalty = perf.max_drawdown_pct.max(0.0) * 0.75;
    let sample_component = if perf.trades >= min_trades {
        6.0
    } else {
        -10.0
    };
    let intel_alignment = compute_intel_alignment(&agent.tags, tag_bias) * 8.0;
    let score = ret_component + sharpe_component + win_component - drawdown_penalty
        + sample_component
        + intel_alignment;
    let note = format!(
        "ret={:.2} sharpe={:.2} win={:.2} dd={:.2} intel={:.2}",
        ret_component, sharpe_component, win_component, drawdown_penalty, intel_alignment
    );
    (score, note)
}

fn compute_intel_alignment(tags: &[String], tag_bias: &HashMap<String, f64>) -> f64 {
    if tags.is_empty() || tag_bias.is_empty() {
        return 0.0;
    }
    let mut total = 0.0;
    let mut count = 0u64;
    for tag in tags {
        if let Some(weight) = tag_bias.get(tag) {
            total += *weight;
            count += 1;
        }
    }
    if count == 0 {
        0.0
    } else {
        total / count as f64
    }
}

fn enforce_capital_budget(
    agents: &mut HashMap<Uuid, StrategyAgent>,
    total_capital_usd: f64,
    max_allocation_per_strategy_usd: f64,
    now: DateTime<Utc>,
    actions: &mut Vec<StrategyAction>,
) {
    let mut active_ids = Vec::new();
    let mut total = 0.0;
    for (id, agent) in agents.iter_mut() {
        if matches!(agent.lifecycle, StrategyLifecycle::Killed) {
            agent.allocation_usd = 0.0;
            continue;
        }
        agent.allocation_usd = agent
            .allocation_usd
            .clamp(0.0, max_allocation_per_strategy_usd.max(0.0));
        total += agent.allocation_usd;
        active_ids.push(*id);
    }

    if total <= total_capital_usd || total <= 0.0 {
        return;
    }

    let scale = total_capital_usd / total;
    for id in active_ids {
        if let Some(agent) = agents.get_mut(&id) {
            let before = agent.allocation_usd;
            let after = (before * scale).max(0.0);
            if (after - before).abs() > f64::EPSILON {
                agent.allocation_usd = after;
                agent.updated_at = now;
                actions.push(StrategyAction {
                    at: now,
                    agent_id: agent.id,
                    agent_name: agent.name.clone(),
                    action: "scaled".to_string(),
                    reason: "scaled to fit total capital budget".to_string(),
                    allocation_before_usd: before,
                    allocation_after_usd: after,
                });
            }
        }
    }
}

fn normalize_tags(tags: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for tag in tags {
        let normalized = tag.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        if !out.iter().any(|existing| existing == &normalized) {
            out.push(normalized);
        }
    }
    out
}

fn normalize_text(value: Option<String>) -> Option<String> {
    value.and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn compare_f64_desc(a: f64, b: f64) -> Ordering {
    b.partial_cmp(&a).unwrap_or(Ordering::Equal)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default)
}

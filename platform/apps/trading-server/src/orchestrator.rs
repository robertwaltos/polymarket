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
    pub challenger_shadow_allocation_pct: f64,
    pub challenger_promotion_score_delta: f64,
    pub challenger_min_trades: u64,
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
            challenger_shadow_allocation_pct: env_f64("ORCH_CHALLENGER_SHADOW_PCT", 0.05)
                .clamp(0.01, 0.50),
            challenger_promotion_score_delta: env_f64("ORCH_CHALLENGER_PROMOTE_DELTA", 5.0)
                .max(0.1),
            challenger_min_trades: env_u64("ORCH_CHALLENGER_MIN_TRADES", 30),
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
    #[serde(default)]
    pub microstructure_policy_score: Option<f64>,
    #[serde(default)]
    pub event_policy_score: Option<f64>,
    #[serde(default)]
    pub risk_policy_score: Option<f64>,
    #[serde(default)]
    pub ensemble_score: Option<f64>,
    #[serde(default)]
    pub ensemble_policy_version: Option<String>,
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

#[derive(Debug, Clone, Deserialize)]
pub struct EnsemblePolicyInput {
    pub microstructure_score: f64,
    pub event_probability_score: f64,
    pub risk_allocator_score: f64,
    #[serde(default)]
    pub micro_weight: Option<f64>,
    #[serde(default)]
    pub event_weight: Option<f64>,
    #[serde(default)]
    pub risk_weight: Option<f64>,
    #[serde(default)]
    pub policy_version: Option<String>,
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
#[serde(rename_all = "snake_case")]
pub enum RolloutStatus {
    Shadow,
    Promoted,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChampionChallengerRollout {
    pub rollout_id: Uuid,
    pub champion_id: Uuid,
    pub challenger_id: Uuid,
    pub champion_name: String,
    pub challenger_name: String,
    pub shadow_allocation_usd: f64,
    pub score_delta: f64,
    pub status: RolloutStatus,
    pub reason: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub promoted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StartChallengerInput {
    pub champion_id: Uuid,
    pub challenger_id: Uuid,
    #[serde(default)]
    pub shadow_allocation_usd: Option<f64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketRegime {
    Normal,
    ThinLiquidity,
    NewsShock,
    Trending,
}

impl Default for MarketRegime {
    fn default() -> Self {
        Self::Normal
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CapitalAllocationInput {
    #[serde(default)]
    pub total_capital_override_usd: Option<f64>,
    #[serde(default)]
    pub regime: Option<MarketRegime>,
    #[serde(default)]
    pub min_allocation_floor_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapitalAllocationEntry {
    pub agent_id: Uuid,
    pub agent_name: String,
    pub lifecycle: StrategyLifecycle,
    pub score: f64,
    pub risk_adjusted_edge: f64,
    pub regime_multiplier: f64,
    pub drawdown_penalty: f64,
    pub allocation_before_usd: f64,
    pub allocation_after_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapitalAllocationPlan {
    pub evaluated_at: DateTime<Utc>,
    pub regime: MarketRegime,
    pub total_capital_usd: f64,
    pub allocated_usd: f64,
    pub unallocated_usd: f64,
    pub entries: Vec<CapitalAllocationEntry>,
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
    #[serde(default)]
    pub rollouts: Vec<ChampionChallengerRollout>,
}

#[derive(Debug, Clone)]
struct OrchestratorState {
    agents: HashMap<Uuid, StrategyAgent>,
    evaluated_at: Option<DateTime<Utc>>,
    last_actions: Vec<StrategyAction>,
    active_tag_bias: HashMap<String, f64>,
    rollouts: HashMap<Uuid, ChampionChallengerRollout>,
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
                rollouts: HashMap::new(),
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
            microstructure_policy_score: None,
            event_policy_score: None,
            risk_policy_score: None,
            ensemble_score: None,
            ensemble_policy_version: None,
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

    pub fn update_ensemble_policy(&self, id: Uuid, input: EnsemblePolicyInput) -> Result<StrategyAgent> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("orchestrator lock poisoned"))?;
        let Some(agent) = guard.agents.get_mut(&id) else {
            return Err(anyhow!("strategy not found: {id}"));
        };

        let (micro_w, event_w, risk_w) = normalize_ensemble_weights(
            input.micro_weight.unwrap_or(0.40),
            input.event_weight.unwrap_or(0.35),
            input.risk_weight.unwrap_or(0.25),
        );
        let micro = input.microstructure_score.clamp(-1.0, 1.0);
        let event = input.event_probability_score.clamp(-1.0, 1.0);
        let risk = input.risk_allocator_score.clamp(-1.0, 1.0);
        let ensemble = (micro * micro_w) + (event * event_w) + (risk * risk_w);

        agent.microstructure_policy_score = Some(micro);
        agent.event_policy_score = Some(event);
        agent.risk_policy_score = Some(risk);
        agent.ensemble_score = Some(ensemble);
        agent.ensemble_policy_version = normalize_text(input.policy_version);
        agent.updated_at = Utc::now();
        let result = agent.clone();
        drop(guard);
        self.recompute_ranks(HashMap::new())?;
        Ok(result)
    }

    pub fn start_challenger_rollout(
        &self,
        input: StartChallengerInput,
    ) -> Result<ChampionChallengerRollout> {
        if input.champion_id == input.challenger_id {
            return Err(anyhow!("champion_id and challenger_id must be different"));
        }
        self.recompute_ranks(HashMap::new())?;
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("orchestrator lock poisoned"))?;
        let now = Utc::now();

        let champion = guard
            .agents
            .get(&input.champion_id)
            .cloned()
            .ok_or_else(|| anyhow!("champion strategy not found"))?;
        let challenger = guard
            .agents
            .get(&input.challenger_id)
            .cloned()
            .ok_or_else(|| anyhow!("challenger strategy not found"))?;
        if matches!(champion.lifecycle, StrategyLifecycle::Killed) {
            return Err(anyhow!("champion strategy is killed"));
        }
        if matches!(challenger.lifecycle, StrategyLifecycle::Killed) {
            return Err(anyhow!("challenger strategy is killed"));
        }

        let default_shadow = (champion.allocation_usd * self.config.challenger_shadow_allocation_pct)
            .clamp(50.0, self.config.max_allocation_per_strategy_usd);
        let shadow_allocation = input
            .shadow_allocation_usd
            .unwrap_or(default_shadow)
            .clamp(10.0, self.config.max_allocation_per_strategy_usd);

        if let Some(challenger_mut) = guard.agents.get_mut(&input.challenger_id) {
            challenger_mut.lifecycle = StrategyLifecycle::Active;
            challenger_mut.allocation_usd = challenger_mut
                .allocation_usd
                .max(10.0)
                .min(shadow_allocation);
            challenger_mut.updated_at = now;
        }

        let rollout = ChampionChallengerRollout {
            rollout_id: Uuid::new_v4(),
            champion_id: champion.id,
            challenger_id: challenger.id,
            champion_name: champion.name,
            challenger_name: challenger.name,
            shadow_allocation_usd: shadow_allocation,
            score_delta: 0.0,
            status: RolloutStatus::Shadow,
            reason: "challenger launched in shadow mode".to_string(),
            created_at: now,
            updated_at: now,
            promoted_at: None,
        };
        guard.rollouts.insert(rollout.rollout_id, rollout.clone());
        Ok(rollout)
    }

    pub fn evaluate_rollouts(&self) -> Result<Vec<ChampionChallengerRollout>> {
        self.recompute_ranks(HashMap::new())?;
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("orchestrator lock poisoned"))?;
        let now = Utc::now();
        let mut changed = false;

        let rollout_ids: Vec<Uuid> = guard.rollouts.keys().copied().collect();
        for rollout_id in rollout_ids {
            let Some(existing) = guard.rollouts.get(&rollout_id).cloned() else {
                continue;
            };
            if !matches!(existing.status, RolloutStatus::Shadow) {
                continue;
            }
            let Some(champion) = guard.agents.get(&existing.champion_id).cloned() else {
                if let Some(rollout) = guard.rollouts.get_mut(&rollout_id) {
                    rollout.status = RolloutStatus::Rejected;
                    rollout.reason = "champion strategy no longer exists".to_string();
                    rollout.updated_at = now;
                }
                changed = true;
                continue;
            };
            let Some(challenger) = guard.agents.get(&existing.challenger_id).cloned() else {
                if let Some(rollout) = guard.rollouts.get_mut(&rollout_id) {
                    rollout.status = RolloutStatus::Rejected;
                    rollout.reason = "challenger strategy no longer exists".to_string();
                    rollout.updated_at = now;
                }
                changed = true;
                continue;
            };

            let min_trades = self
                .config
                .challenger_min_trades
                .max(self.config.min_trades_before_ranking);
            if champion.performance.trades < min_trades || challenger.performance.trades < min_trades
            {
                if let Some(rollout) = guard.rollouts.get_mut(&rollout_id) {
                    rollout.reason = format!(
                        "insufficient rollout sample (champion_trades={}, challenger_trades={}, min={})",
                        champion.performance.trades, challenger.performance.trades, min_trades
                    );
                    rollout.updated_at = now;
                }
                continue;
            }

            let score_delta = challenger.score - champion.score;
            let promote_threshold = self.config.challenger_promotion_score_delta.max(0.1);
            if let Some(rollout) = guard.rollouts.get_mut(&rollout_id) {
                rollout.score_delta = score_delta;
                rollout.updated_at = now;
            }

            if score_delta >= promote_threshold {
                let champion_allocation = champion.allocation_usd.max(0.0);
                let transfer = champion_allocation * 0.75;
                if let Some(champion_mut) = guard.agents.get_mut(&existing.champion_id) {
                    champion_mut.lifecycle = StrategyLifecycle::Paused;
                    champion_mut.allocation_usd = (champion_allocation - transfer).max(0.0);
                    champion_mut.updated_at = now;
                }
                if let Some(challenger_mut) = guard.agents.get_mut(&existing.challenger_id) {
                    challenger_mut.lifecycle = StrategyLifecycle::Active;
                    challenger_mut.allocation_usd = (challenger_mut.allocation_usd + transfer)
                        .clamp(0.0, self.config.max_allocation_per_strategy_usd);
                    challenger_mut.updated_at = now;
                }
                if let Some(rollout) = guard.rollouts.get_mut(&rollout_id) {
                    rollout.status = RolloutStatus::Promoted;
                    rollout.reason = format!(
                        "challenger promoted (score_delta={:.2} >= threshold={:.2})",
                        score_delta, promote_threshold
                    );
                    rollout.promoted_at = Some(now);
                    rollout.updated_at = now;
                }
                changed = true;
            } else if score_delta <= -promote_threshold {
                if let Some(challenger_mut) = guard.agents.get_mut(&existing.challenger_id) {
                    challenger_mut.lifecycle = StrategyLifecycle::Paused;
                    challenger_mut.allocation_usd = challenger_mut
                        .allocation_usd
                        .min(existing.shadow_allocation_usd)
                        .max(0.0);
                    challenger_mut.updated_at = now;
                }
                if let Some(rollout) = guard.rollouts.get_mut(&rollout_id) {
                    rollout.status = RolloutStatus::Rejected;
                    rollout.reason = format!(
                        "challenger rejected (score_delta={:.2} <= -threshold={:.2})",
                        score_delta, promote_threshold
                    );
                    rollout.updated_at = now;
                }
                changed = true;
            } else if let Some(rollout) = guard.rollouts.get_mut(&rollout_id) {
                rollout.reason = format!(
                    "shadow comparison in-progress (score_delta={:.2}, threshold={:.2})",
                    score_delta, promote_threshold
                );
            }
        }

        if changed {
            guard.evaluated_at = Some(now);
        }
        let mut rollouts: Vec<ChampionChallengerRollout> = guard.rollouts.values().cloned().collect();
        rollouts.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        Ok(rollouts)
    }

    pub fn rollouts(&self) -> Result<Vec<ChampionChallengerRollout>> {
        let guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("orchestrator lock poisoned"))?;
        let mut rollouts: Vec<ChampionChallengerRollout> = guard.rollouts.values().cloned().collect();
        rollouts.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        Ok(rollouts)
    }

    pub fn allocate_capital(&self, input: CapitalAllocationInput) -> Result<CapitalAllocationPlan> {
        self.recompute_ranks(HashMap::new())?;
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("orchestrator lock poisoned"))?;
        let now = Utc::now();

        let regime = input.regime.unwrap_or_default();
        let total_capital_usd = input
            .total_capital_override_usd
            .unwrap_or(self.config.total_capital_usd)
            .clamp(100.0, self.config.total_capital_usd.max(100.0));
        let floor = input
            .min_allocation_floor_usd
            .unwrap_or(0.0)
            .clamp(0.0, (self.config.max_allocation_per_strategy_usd * 0.20).max(0.0));

        let min_trades = self.config.min_trades_before_ranking.max(1);
        let mut weights: Vec<(Uuid, f64, f64, f64)> = Vec::new();
        for (id, agent) in guard.agents.iter() {
            if matches!(agent.lifecycle, StrategyLifecycle::Killed) {
                continue;
            }
            let raw_edge = compute_risk_adjusted_edge(agent, min_trades);
            let regime_multiplier = regime_multiplier_for_agent(regime, &agent.tags);
            let drawdown_penalty = 1.0 + (agent.performance.max_drawdown_pct.max(0.0) / 25.0);
            let final_edge = (raw_edge * regime_multiplier).max(0.0);
            weights.push((*id, final_edge, regime_multiplier, drawdown_penalty));
        }

        let mut entries = Vec::new();
        let eligible_count = weights.len().max(1);
        let floor_total = (floor * eligible_count as f64).min(total_capital_usd);
        let distributable = (total_capital_usd - floor_total).max(0.0);
        let total_weight: f64 = weights.iter().map(|(_, weight, _, _)| *weight).sum();

        for (id, weight, regime_multiplier, drawdown_penalty) in &weights {
            let Some(agent) = guard.agents.get_mut(id) else {
                continue;
            };
            let before = agent.allocation_usd.max(0.0);
            let weighted_component = if total_weight > 0.0 {
                (weight / total_weight) * distributable
            } else {
                distributable / eligible_count as f64
            };
            let mut after = floor + weighted_component;
            if matches!(agent.lifecycle, StrategyLifecycle::Paused) {
                after = after.min(floor.max(10.0));
            }
            after = after.clamp(0.0, self.config.max_allocation_per_strategy_usd);
            agent.allocation_usd = after;
            agent.updated_at = now;

            entries.push(CapitalAllocationEntry {
                agent_id: agent.id,
                agent_name: agent.name.clone(),
                lifecycle: agent.lifecycle.clone(),
                score: agent.score,
                risk_adjusted_edge: *weight,
                regime_multiplier: *regime_multiplier,
                drawdown_penalty: *drawdown_penalty,
                allocation_before_usd: before,
                allocation_after_usd: after,
            });
        }

        let mut actions = Vec::new();
        for entry in &entries {
            if (entry.allocation_after_usd - entry.allocation_before_usd).abs() <= f64::EPSILON {
                continue;
            }
            actions.push(StrategyAction {
                at: now,
                agent_id: entry.agent_id,
                agent_name: entry.agent_name.clone(),
                action: "reallocated".to_string(),
                reason: format!(
                    "capital allocator regime={:?} risk_adjusted_edge={:.2}",
                    regime, entry.risk_adjusted_edge
                ),
                allocation_before_usd: entry.allocation_before_usd,
                allocation_after_usd: entry.allocation_after_usd,
            });
        }

        enforce_capital_budget(
            &mut guard.agents,
            total_capital_usd,
            self.config.max_allocation_per_strategy_usd,
            now,
            &mut actions,
        );

        // Refresh plan entries after hard-cap enforcement.
        for entry in entries.iter_mut() {
            if let Some(agent) = guard.agents.get(&entry.agent_id) {
                entry.allocation_after_usd = agent.allocation_usd.max(0.0);
            }
        }
        entries.sort_by(|a, b| compare_f64_desc(a.risk_adjusted_edge, b.risk_adjusted_edge));

        guard.last_actions = actions;
        guard.evaluated_at = Some(now);
        let allocated_usd: f64 = guard
            .agents
            .values()
            .filter(|agent| !matches!(agent.lifecycle, StrategyLifecycle::Killed))
            .map(|agent| agent.allocation_usd.max(0.0))
            .sum();

        Ok(CapitalAllocationPlan {
            evaluated_at: now,
            regime,
            total_capital_usd,
            allocated_usd,
            unallocated_usd: (total_capital_usd - allocated_usd).max(0.0),
            entries,
        })
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
        guard.rollouts = snapshot
            .rollouts
            .into_iter()
            .map(|rollout| (rollout.rollout_id, rollout))
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
        let mut rollouts: Vec<ChampionChallengerRollout> = guard.rollouts.values().cloned().collect();
        rollouts.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        Ok(OrchestratorSnapshot {
            config: self.config.clone(),
            evaluated_at: guard.evaluated_at,
            total_allocated_usd,
            agents,
            last_actions: guard.last_actions.clone(),
            active_tag_bias,
            rollouts,
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
    let ensemble_component = agent.ensemble_score.unwrap_or(0.0) * 20.0;
    let score = ret_component + sharpe_component + win_component - drawdown_penalty
        + sample_component
        + intel_alignment
        + ensemble_component;
    let note = format!(
        "ret={:.2} sharpe={:.2} win={:.2} dd={:.2} intel={:.2} ensemble={:.2}",
        ret_component,
        sharpe_component,
        win_component,
        drawdown_penalty,
        intel_alignment,
        ensemble_component
    );
    (score, note)
}

fn compute_risk_adjusted_edge(agent: &StrategyAgent, min_trades: u64) -> f64 {
    if matches!(agent.lifecycle, StrategyLifecycle::Killed) {
        return 0.0;
    }
    let perf = &agent.performance;
    let score_component = agent.score.max(0.0);
    let sharpe_component = perf.sharpe.max(0.0) * 12.0;
    let return_component = perf.total_return_pct.max(0.0) * 0.35;
    let win_component = (perf.win_rate.clamp(0.0, 1.0) - 0.5).max(0.0) * 20.0;
    let sample_multiplier = if perf.trades >= min_trades { 1.0 } else { 0.35 };
    let drawdown_penalty = 1.0 + (perf.max_drawdown_pct.max(0.0) / 25.0);
    ((score_component + sharpe_component + return_component + win_component) * sample_multiplier)
        / drawdown_penalty
}

fn regime_multiplier_for_agent(regime: MarketRegime, tags: &[String]) -> f64 {
    if tags.is_empty() {
        return 1.0;
    }

    let has_tag = |needle: &str| tags.iter().any(|tag| tag == needle);
    let mut multiplier = 1.0_f64;
    match regime {
        MarketRegime::Normal => {}
        MarketRegime::ThinLiquidity => {
            if has_tag("market_making") || has_tag("mean_reversion") {
                multiplier += 0.18;
            }
            if has_tag("momentum") {
                multiplier -= 0.12;
            }
        }
        MarketRegime::NewsShock => {
            if has_tag("event_driven") || has_tag("sentiment") {
                multiplier += 0.22;
            }
            if has_tag("mean_reversion") {
                multiplier -= 0.10;
            }
        }
        MarketRegime::Trending => {
            if has_tag("momentum") || has_tag("cross_venue") {
                multiplier += 0.20;
            }
            if has_tag("market_making") {
                multiplier -= 0.08;
            }
        }
    }
    multiplier.clamp(0.50, 1.50)
}

fn normalize_ensemble_weights(micro: f64, event: f64, risk: f64) -> (f64, f64, f64) {
    let mut micro = micro.max(0.0);
    let mut event = event.max(0.0);
    let mut risk = risk.max(0.0);
    let total = micro + event + risk;
    if total <= 0.0 {
        micro = 0.40;
        event = 0.35;
        risk = 0.25;
        return (micro, event, risk);
    }
    (micro / total, event / total, risk / total)
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

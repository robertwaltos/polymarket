use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SliceAlgorithm {
    Twap,
    Vwap,
    Pov,
    Iceberg,
    PassiveMaker,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlicePlanConfig {
    pub algorithm: SliceAlgorithm,
    pub side: domain::Side,
    pub total_quantity: f64,
    pub reference_price: f64,
    pub duration_secs: u64,
    pub child_count: Option<usize>,
    pub participation_rate: Option<f64>,
    pub visible_fraction: Option<f64>,
    pub depth_usd: Option<f64>,
    pub spread_bps: Option<f64>,
    pub queue_ahead_qty: Option<f64>,
    pub target_child_notional_usd: Option<f64>,
    pub min_child_qty: Option<f64>,
    pub max_child_qty: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliceChildPlan {
    pub child_index: usize,
    pub offset_ms: u64,
    pub quantity: f64,
    pub limit_price: f64,
    pub expected_fill_probability: f64,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlicePlan {
    pub algorithm: SliceAlgorithm,
    pub total_quantity: f64,
    pub planned_quantity: f64,
    pub unallocated_quantity: f64,
    pub child_count: usize,
    pub expected_fill_ratio: f64,
    pub notes: Vec<String>,
    pub children: Vec<SliceChildPlan>,
}

pub fn plan_slice(config: &SlicePlanConfig) -> SlicePlan {
    let total_quantity = config.total_quantity.abs().max(0.0);
    if total_quantity <= f64::EPSILON {
        return SlicePlan {
            algorithm: config.algorithm,
            total_quantity,
            planned_quantity: 0.0,
            unallocated_quantity: 0.0,
            child_count: 0,
            expected_fill_ratio: 0.0,
            notes: vec!["total_quantity is zero; no slices produced".to_string()],
            children: Vec::new(),
        };
    }

    let duration_secs = config.duration_secs.max(1);
    let mut child_count = config.child_count.unwrap_or_else(|| {
        let intervals = (duration_secs / 5).max(1);
        intervals as usize
    });
    child_count = child_count.clamp(1, 512);

    let interval_ms = ((duration_secs as f64 * 1000.0) / child_count as f64).round() as u64;
    let participation_rate = config.participation_rate.unwrap_or(0.10).clamp(0.01, 1.0);
    let visible_fraction = config.visible_fraction.unwrap_or(0.10).clamp(0.01, 1.0);
    let spread_bps = config.spread_bps.unwrap_or(15.0).clamp(0.1, 2_000.0);
    let depth_usd = config.depth_usd.unwrap_or(10_000.0).max(1.0);
    let queue_ahead_qty = config.queue_ahead_qty.unwrap_or(0.0).max(0.0);
    let target_child_notional = config.target_child_notional_usd.unwrap_or(500.0).max(1.0);
    let depth_qty = (depth_usd / config.reference_price.max(0.000_001)).max(0.000_001);

    let mut notes = vec![
        format!("duration_secs={duration_secs} child_count={child_count}"),
        format!(
            "spread_bps={spread_bps:.2} depth_usd={depth_usd:.2} queue_ahead_qty={queue_ahead_qty:.4}"
        ),
    ];

    let raw_weights = weights_for_algorithm(config.algorithm, child_count);
    let weight_sum: f64 = raw_weights.iter().sum::<f64>().max(0.000_001);
    let normalized_weights: Vec<f64> = raw_weights.into_iter().map(|w| w / weight_sum).collect();

    let spread_factor = (15.0 / spread_bps).clamp(0.2, 1.5);
    let depth_factor = (depth_usd / 10_000.0).sqrt().clamp(0.2, 2.5);
    let queue_factor = (1.0 / (1.0 + queue_ahead_qty / depth_qty.max(0.000_001))).clamp(0.1, 1.0);
    let adaptive_scale = (spread_factor * depth_factor * queue_factor).clamp(0.15, 1.25);
    let expected_fill_probability = (0.35 + 0.4 * adaptive_scale).clamp(0.05, 0.99);
    notes.push(format!(
        "adaptive_scale={adaptive_scale:.3} expected_fill_prob~{expected_fill_probability:.2}"
    ));

    let mut max_child_qty = config.max_child_qty.unwrap_or(f64::INFINITY).max(0.0);
    max_child_qty = max_child_qty.min(target_child_notional / config.reference_price.max(0.000_001));
    if matches!(config.algorithm, SliceAlgorithm::Iceberg) {
        max_child_qty = max_child_qty.min((total_quantity * visible_fraction).max(0.000_001));
    }
    if !max_child_qty.is_finite() || max_child_qty <= 0.0 {
        max_child_qty = total_quantity;
    }
    let min_child_qty = config
        .min_child_qty
        .unwrap_or((total_quantity / child_count as f64) * 0.15)
        .clamp(0.0, max_child_qty);

    let mut quantities = vec![0.0; child_count];
    for idx in 0..child_count {
        let weight = normalized_weights[idx];
        let mut qty = total_quantity * weight;
        qty *= adaptive_scale;
        if matches!(config.algorithm, SliceAlgorithm::Pov) {
            let pov_cap = depth_qty * participation_rate;
            qty = qty.min(pov_cap.max(min_child_qty));
        }
        qty = qty.clamp(min_child_qty, max_child_qty);
        quantities[idx] = qty;
    }

    normalize_quantities(&mut quantities, total_quantity, min_child_qty, max_child_qty);

    let half_spread = (spread_bps / 10_000.0) * config.reference_price * 0.5;
    let mut children = Vec::with_capacity(child_count);
    for (idx, qty) in quantities.into_iter().enumerate() {
        if qty <= f64::EPSILON {
            continue;
        }
        let queue_tilt = (queue_ahead_qty / depth_qty).clamp(0.0, 4.0);
        let urgency = (idx as f64 + 1.0) / child_count as f64;
        let passive_offset = (1.0 - urgency) * half_spread * (1.0 + queue_tilt * 0.25);
        let aggressive_offset = urgency * half_spread * 0.4;
        let price_shift = match (config.algorithm, config.side) {
            (SliceAlgorithm::PassiveMaker, domain::Side::Buy) => -passive_offset,
            (SliceAlgorithm::PassiveMaker, domain::Side::Sell) => passive_offset,
            (SliceAlgorithm::Pov, domain::Side::Buy) => aggressive_offset * 0.6,
            (SliceAlgorithm::Pov, domain::Side::Sell) => -aggressive_offset * 0.6,
            (_, domain::Side::Buy) => aggressive_offset * 0.25,
            (_, domain::Side::Sell) => -aggressive_offset * 0.25,
        };
        let limit_price = (config.reference_price + price_shift).max(0.000_001);
        let rationale = format!(
            "algo={:?} weight_idx={} adaptive={adaptive_scale:.2} spread_bps={spread_bps:.1}",
            config.algorithm,
            idx + 1
        );
        children.push(SliceChildPlan {
            child_index: idx + 1,
            offset_ms: interval_ms * idx as u64,
            quantity: qty,
            limit_price,
            expected_fill_probability,
            rationale,
        });
    }

    let planned_quantity: f64 = children.iter().map(|child| child.quantity).sum();
    let unallocated_quantity = (total_quantity - planned_quantity).max(0.0);
    let expected_fill_ratio = if total_quantity <= f64::EPSILON {
        0.0
    } else {
        ((planned_quantity * expected_fill_probability) / total_quantity).clamp(0.0, 1.0)
    };

    SlicePlan {
        algorithm: config.algorithm,
        total_quantity,
        planned_quantity,
        unallocated_quantity,
        child_count: children.len(),
        expected_fill_ratio,
        notes,
        children,
    }
}

fn weights_for_algorithm(algorithm: SliceAlgorithm, child_count: usize) -> Vec<f64> {
    match algorithm {
        SliceAlgorithm::Twap => vec![1.0; child_count],
        SliceAlgorithm::Vwap => (0..child_count)
            .map(|idx| {
                let x = (idx as f64 + 0.5) / child_count as f64;
                0.6 + 1.2 * (1.0 - (2.0 * x - 1.0).abs())
            })
            .collect(),
        SliceAlgorithm::Pov => (0..child_count)
            .map(|idx| {
                let x = (idx as f64 + 1.0) / child_count as f64;
                0.5 + 1.5 * x
            })
            .collect(),
        SliceAlgorithm::Iceberg => vec![1.0; child_count],
        SliceAlgorithm::PassiveMaker => (0..child_count)
            .map(|idx| {
                let x = (idx as f64 + 1.0) / child_count as f64;
                0.4 + 1.6 * x.powf(1.5)
            })
            .collect(),
    }
}

fn normalize_quantities(
    quantities: &mut [f64],
    total_quantity: f64,
    min_child_qty: f64,
    max_child_qty: f64,
) {
    if quantities.is_empty() {
        return;
    }
    let mut current_total: f64 = quantities.iter().sum();
    if current_total <= f64::EPSILON {
        return;
    }

    if current_total > total_quantity {
        let scale = total_quantity / current_total;
        for qty in quantities.iter_mut() {
            *qty = (*qty * scale).clamp(0.0, max_child_qty);
        }
    } else if current_total < total_quantity {
        let mut remaining = total_quantity - current_total;
        let mut index = 0usize;
        while remaining > 0.0 && index < quantities.len() * 8 {
            let slot = index % quantities.len();
            let capacity = (max_child_qty - quantities[slot]).max(0.0);
            if capacity > 0.0 {
                let add = remaining.min(capacity);
                quantities[slot] += add;
                remaining -= add;
            }
            index += 1;
        }
    }

    for qty in quantities.iter_mut() {
        if *qty > 0.0 {
            *qty = qty.clamp(min_child_qty, max_child_qty);
        }
    }
    current_total = quantities.iter().sum();
    if current_total > total_quantity {
        let overshoot = current_total - total_quantity;
        if let Some(last) = quantities.last_mut() {
            *last = (*last - overshoot).max(0.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> SlicePlanConfig {
        SlicePlanConfig {
            algorithm: SliceAlgorithm::Twap,
            side: domain::Side::Buy,
            total_quantity: 100.0,
            reference_price: 50.0,
            duration_secs: 60,
            child_count: Some(6),
            participation_rate: Some(0.1),
            visible_fraction: Some(0.1),
            depth_usd: Some(20_000.0),
            spread_bps: Some(12.0),
            queue_ahead_qty: Some(20.0),
            target_child_notional_usd: Some(1_000.0),
            min_child_qty: Some(1.0),
            max_child_qty: Some(50.0),
        }
    }

    #[test]
    fn twap_plan_produces_multiple_children() {
        let plan = plan_slice(&base_config());
        assert_eq!(plan.child_count, 6);
        assert!(plan.planned_quantity > 0.0);
        assert!(plan.expected_fill_ratio > 0.0);
    }

    #[test]
    fn iceberg_respects_visible_fraction() {
        let mut config = base_config();
        config.algorithm = SliceAlgorithm::Iceberg;
        config.visible_fraction = Some(0.05);
        let plan = plan_slice(&config);
        let visible_cap = config.total_quantity * 0.05 + 1e-9;
        assert!(plan.children.iter().all(|child| child.quantity <= visible_cap));
    }

    #[test]
    fn passive_maker_buy_prices_below_reference() {
        let mut config = base_config();
        config.algorithm = SliceAlgorithm::PassiveMaker;
        config.side = domain::Side::Buy;
        let plan = plan_slice(&config);
        assert!(plan
            .children
            .iter()
            .all(|child| child.limit_price <= config.reference_price + 1e-9));
    }
}

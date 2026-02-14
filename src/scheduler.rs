use crate::config::Config;
use crate::strategy::{KalshiMatch, PredictItMatch, Strategy};
use crate::types::{ExternalData, Market, MarketNiche};
use chrono::{Datelike, Duration, NaiveDate, TimeZone, Utc, Weekday};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct ScheduledCandidate {
    pub market: Market,
    pub niche: MarketNiche,
    pub kalshi_match: Option<KalshiMatch>,
    pub predictit_match: Option<PredictItMatch>,
    pub expected_value_score: f64,
    pub priority_score: f64,
    pub estimated_input_tokens: u64,
    pub settlement_hours: Option<f64>,
}

pub fn schedule_candidates(
    strategy: &Strategy,
    config: &Config,
    external_data: &ExternalData,
    candidates: Vec<(Market, MarketNiche)>,
) -> Vec<ScheduledCandidate> {
    if candidates.is_empty() {
        return Vec::new();
    }

    let mut ranked = candidates
        .into_iter()
        .map(|(market, niche)| {
            let kalshi_match = strategy.kalshi_match(&market, external_data);
            let predictit_match = strategy.predictit_match(&market, external_data);

            let estimated_input_tokens = strategy
                .estimate_input_tokens(
                    &market,
                    external_data,
                    niche,
                    kalshi_match.as_ref(),
                    predictit_match.as_ref(),
                )
                .max(1);

            let (settlement_hours, settlement_weight) =
                settlement_signal(&market, config.scheduler_timezone_offset_hours, config.scheduler_urgent_window_hours);
            let expected_value_score =
                expected_value_proxy(&market, niche, kalshi_match.as_ref(), predictit_match.as_ref())
                    * settlement_weight;

            let token_efficiency = expected_value_score
                / ((estimated_input_tokens as f64 / 1000.0).max(0.2));
            let priority_score = (expected_value_score * 0.72) + (token_efficiency * 0.28);

            ScheduledCandidate {
                market,
                niche,
                kalshi_match,
                predictit_match,
                expected_value_score,
                priority_score,
                estimated_input_tokens,
                settlement_hours,
            }
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|a, b| {
        b.priority_score
            .partial_cmp(&a.priority_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                b.expected_value_score
                    .partial_cmp(&a.expected_value_score)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                a.estimated_input_tokens
                    .cmp(&b.estimated_input_tokens)
            })
    });

    if !config.enable_ev_scheduler {
        ranked.truncate(config.scheduler_max_candidates.max(1));
        return ranked;
    }

    let max_candidates = config.scheduler_max_candidates.max(1);
    let token_budget = config.scheduler_cycle_token_budget.max(1);
    let mut selected = Vec::new();
    let mut used_tokens = 0u64;

    for candidate in ranked.iter().cloned() {
        if selected.len() >= max_candidates {
            break;
        }

        if candidate.expected_value_score < config.scheduler_ev_floor {
            continue;
        }

        let next_tokens = used_tokens.saturating_add(candidate.estimated_input_tokens);
        if next_tokens > token_budget {
            if selected.is_empty() {
                used_tokens = next_tokens;
                selected.push(candidate);
            }
            continue;
        }

        used_tokens = next_tokens;
        selected.push(candidate);
    }

    if selected.is_empty() {
        let fallback_len = max_candidates.min(ranked.len()).max(1);
        ranked.truncate(fallback_len);
        return ranked;
    }

    selected
}

fn expected_value_proxy(
    market: &Market,
    niche: MarketNiche,
    kalshi_match: Option<&KalshiMatch>,
    predictit_match: Option<&PredictItMatch>,
) -> f64 {
    let yes_price = market.yes_price.unwrap_or(0.5).clamp(0.01, 0.99);
    let mut dislocation = (0.5 - yes_price).abs() * 0.10;

    if let Some(kalshi) = kalshi_match {
        dislocation = dislocation.max((kalshi.probability - yes_price).abs());
        let volume_boost = (kalshi.volume / 5_000.0).clamp(0.0, 1.0);
        dislocation *= 1.0 + (0.35 * volume_boost);
    }

    if let Some(predictit) = predictit_match {
        if (0.0..=1.0).contains(&predictit.yes_price) {
            dislocation = dislocation.max((predictit.yes_price - yes_price).abs());
        }
    }

    let niche_weight = match niche {
        MarketNiche::Politics => 1.15,
        MarketNiche::Sports => 1.08,
        MarketNiche::Crypto => 1.10,
        MarketNiche::Weather => 1.00,
        MarketNiche::Betting => 1.12,
    };

    (dislocation * niche_weight).clamp(0.0, 1.0)
}

fn settlement_signal(
    market: &Market,
    timezone_offset_hours: i32,
    urgent_window_hours: i64,
) -> (Option<f64>, f64) {
    let text = market.market_text().to_ascii_lowercase();
    let local_now = Utc::now() + Duration::hours(timezone_offset_hours as i64);
    let hours_to_settlement = infer_hours_to_settlement(&text, local_now);

    let weight = match hours_to_settlement {
        Some(hours) if hours <= 0.0 => 0.25,
        Some(hours) if hours <= urgent_window_hours as f64 => {
            let ratio = hours / urgent_window_hours as f64;
            (1.35 - (0.35 * ratio)).clamp(1.0, 1.35)
        }
        Some(hours) => (urgent_window_hours as f64 / hours).clamp(0.45, 1.0),
        None => 1.0,
    };

    (hours_to_settlement, weight)
}

fn infer_hours_to_settlement(text: &str, local_now: chrono::DateTime<Utc>) -> Option<f64> {
    if text.contains("today") || text.contains("tonight") {
        return Some(8.0);
    }
    if text.contains("tomorrow") {
        return Some(24.0);
    }
    if text.contains("this week") {
        return Some(96.0);
    }

    if let Some(hours) = hours_until_weekday(text, local_now) {
        return Some(hours);
    }

    if let Some(hours) = hours_until_explicit_date(text, local_now) {
        return Some(hours);
    }

    None
}

fn hours_until_weekday(text: &str, local_now: chrono::DateTime<Utc>) -> Option<f64> {
    let weekdays = [
        ("monday", Weekday::Mon),
        ("tuesday", Weekday::Tue),
        ("wednesday", Weekday::Wed),
        ("thursday", Weekday::Thu),
        ("friday", Weekday::Fri),
        ("saturday", Weekday::Sat),
        ("sunday", Weekday::Sun),
    ];

    for (name, weekday) in weekdays {
        if !text.contains(name) {
            continue;
        }
        let current_idx = local_now.weekday().num_days_from_monday() as i64;
        let target_idx = weekday.num_days_from_monday() as i64;
        let mut delta_days = target_idx - current_idx;
        if delta_days < 0 {
            delta_days += 7;
        }
        if delta_days == 0 {
            delta_days = 1;
        }
        return Some((delta_days as f64) * 24.0);
    }
    None
}

fn hours_until_explicit_date(text: &str, local_now: chrono::DateTime<Utc>) -> Option<f64> {
    for token in text.split_whitespace() {
        let cleaned = token
            .trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '-' && ch != '/');
        if cleaned.is_empty() {
            continue;
        }

        if let Ok(date) = NaiveDate::parse_from_str(cleaned, "%Y-%m-%d") {
            if let Some(hours) = hours_until_date(local_now, date) {
                return Some(hours);
            }
        }

        if let Ok(date) = NaiveDate::parse_from_str(cleaned, "%m/%d/%Y") {
            if let Some(hours) = hours_until_date(local_now, date) {
                return Some(hours);
            }
        }

        if let Ok(date) = NaiveDate::parse_from_str(cleaned, "%m/%d/%y") {
            if let Some(hours) = hours_until_date(local_now, date) {
                return Some(hours);
            }
        }
    }
    None
}

fn hours_until_date(local_now: chrono::DateTime<Utc>, date: NaiveDate) -> Option<f64> {
    let naive_dt = date.and_hms_opt(12, 0, 0)?;
    let target = Utc.from_utc_datetime(&naive_dt);
    let delta = target - local_now;
    Some((delta.num_seconds() as f64) / 3600.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ev_proxy_increases_with_cross_market_dislocation() {
        let market = Market {
            id: "m1".to_string(),
            question: "Will BTC close above 70k today?".to_string(),
            description: None,
            yes_price: Some(0.42),
            no_price: Some(0.58),
        };
        let base = expected_value_proxy(&market, MarketNiche::Crypto, None, None);
        let kalshi = KalshiMatch {
            ticker: "KXBTCHIGH".to_string(),
            title: "BTC closes above 70k".to_string(),
            subtitle: "".to_string(),
            probability: 0.66,
            volume: 25_000.0,
            variance: 0.03,
            score: 0.95,
        };
        let dislocated =
            expected_value_proxy(&market, MarketNiche::Crypto, Some(&kalshi), None);
        assert!(dislocated > base);
    }

    #[test]
    fn settlement_weight_prioritizes_near_term_markets() {
        let urgent_market = Market {
            id: "u".to_string(),
            question: "Will it rain today in NYC?".to_string(),
            description: None,
            yes_price: Some(0.53),
            no_price: Some(0.47),
        };
        let long_market = Market {
            id: "l".to_string(),
            question: "Will ETH reach 10k by 2099-12-31?".to_string(),
            description: None,
            yes_price: Some(0.15),
            no_price: Some(0.85),
        };

        let (_, urgent_weight) = settlement_signal(&urgent_market, 0, 48);
        let (_, long_weight) = settlement_signal(&long_market, 0, 48);
        assert!(urgent_weight > long_weight);
    }
}

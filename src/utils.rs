use crate::error::AgentError;
use std::future::Future;
use tokio::time::{sleep, Duration};

pub async fn retry<F, Fut, T>(mut op: F, max_attempts: u32) -> Result<T, AgentError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, AgentError>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Ok(val) => return Ok(val),
            Err(err) => {
                attempt += 1;
                if attempt >= max_attempts {
                    return Err(err);
                }
                let backoff = 2u64.saturating_pow(attempt);
                sleep(Duration::from_secs(backoff)).await;
            }
        }
    }
}

pub fn extract_json_block(text: &str) -> Option<String> {
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    if start >= end {
        return None;
    }
    Some(text[start..=end].to_string())
}

pub fn clamp_probability(value: f64) -> f64 {
    if value.is_nan() {
        0.0
    } else if value < 0.0 {
        0.0
    } else if value > 1.0 {
        1.0
    } else {
        value
    }
}

pub fn bayesian_update(prior: f64, likelihood: f64) -> f64 {
    let prior = clamp_probability(prior);
    let likelihood = clamp_probability(likelihood);
    let numerator = prior * likelihood;
    let denominator = numerator + (1.0 - prior) * (1.0 - likelihood);
    if denominator == 0.0 {
        prior
    } else {
        clamp_probability(numerator / denominator)
    }
}

pub fn bayesian_aggregate(prior: f64, sources: &[(f64, f64)]) -> f64 {
    let mut mean = clamp_probability(prior);
    let mut var = 0.1_f64;

    for (source_prob, source_var) in sources {
        let p = clamp_probability(*source_prob);
        let sv = if *source_var <= 0.0 { 0.05 } else { *source_var };
        let post_var = 1.0 / (1.0 / var + 1.0 / sv);
        let post_mean = post_var * (mean / var + p / sv);
        mean = post_mean;
        var = post_var;
    }

    clamp_probability(mean)
}

pub fn dirichlet_update(alpha: &mut [f64], counts: &[f64]) {
    for (a, c) in alpha.iter_mut().zip(counts.iter()) {
        *a = (*a + *c).max(0.0);
    }
}

pub fn truncate_string(input: &str, max_len: usize) -> String {
    if input.len() <= max_len {
        input.to_string()
    } else {
        let mut s = input[..max_len].to_string();
        s.push_str("...");
        s
    }
}

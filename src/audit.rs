use crate::config::Config;
use crate::error::AgentError;
use log::warn;
use reqwest::Client;
use serde_json::json;
use std::fs;

pub async fn self_audit(config: &Config, client: &Client) -> Result<(), AgentError> {
    if !config.enable_self_audit {
        return Ok(());
    }

    let Some(path) = &config.self_audit_path else {
        warn!("Self-audit enabled but SELF_AUDIT_PATH not set");
        return Ok(());
    };

    let code = fs::read_to_string(path)
        .map_err(|e| AgentError::Internal(format!("audit read failed: {}", e)))?;

    let prompt = format!(
        "Review this Rust code for bugs, panics, or logic errors. Summarize top issues only.\n\n{}",
        code
    );

    let payload = json!({
        "model": config.anthropic_model,
        "max_tokens": 400,
        "temperature": 0.1,
        "messages": [
            {"role": "user", "content": prompt}
        ]
    });

    let resp = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", &config.anthropic_key)
        .header("anthropic-version", &config.anthropic_version)
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;

    let body: serde_json::Value = resp.json().await?;
    warn!("[SELF-AUDIT] {}", body);
    Ok(())
}

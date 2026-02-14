use crate::config::Config;
use crate::error::AgentError;
use crate::rl_agent::{FederatedQEntry, QLearningAgent, QLearningSnapshot};
use hmac::{Hmac, Mac};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

pub struct FederatedClient {
    enabled: bool,
    cluster_id: String,
    peer_id: String,
    shared_key: Vec<u8>,
    sync_dir: PathBuf,
    min_peer_updates: usize,
    max_snapshot_age_secs: u64,
    secure_aggregation: bool,
    local_blend: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FederatedPayload {
    cluster_id: String,
    peer_id: String,
    base_policy_version: u64,
    policy_version: u64,
    training_steps: u64,
    generated_at_epoch_ms: u64,
    entries: Vec<FederatedQEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FederatedEnvelope {
    payload: FederatedPayload,
    signature_hex: String,
}

impl FederatedClient {
    pub fn from_config(config: &Config) -> Self {
        let shared_key = config
            .federated_shared_key
            .as_ref()
            .map(|value| value.trim().as_bytes().to_vec())
            .unwrap_or_default();
        let enabled = config.enable_federated && !shared_key.is_empty();

        if config.enable_federated && shared_key.is_empty() {
            warn!(
                "[FEDERATED] ENABLE_FEDERATED=true but FEDERATED_SHARED_KEY is missing. Disabling sync."
            );
        }

        Self {
            enabled,
            cluster_id: config.federated_cluster_id.clone(),
            peer_id: config.federated_peer_id.clone(),
            shared_key,
            sync_dir: PathBuf::from(&config.federated_sync_dir),
            min_peer_updates: config.federated_min_peer_updates.max(1),
            max_snapshot_age_secs: config.federated_max_snapshot_age_secs.max(30),
            secure_aggregation: config.federated_secure_aggregation,
            local_blend: config.federated_local_blend.clamp(0.05, 1.0),
        }
    }

    pub fn sync(&self, agent: &mut QLearningAgent) -> Result<(), AgentError> {
        if !self.enabled {
            return Ok(());
        }

        let cluster_dir = self.sync_dir.join(sanitize_path_segment(&self.cluster_id));
        fs::create_dir_all(&cluster_dir)
            .map_err(|err| AgentError::Internal(format!("federated mkdir failed: {err}")))?;

        self.publish_local_snapshot(&cluster_dir, agent)?;

        let local_version = agent.policy_version();
        let peer_updates = self.collect_peer_updates(&cluster_dir, local_version)?;
        if peer_updates.len() < self.min_peer_updates {
            return Ok(());
        }

        if self.secure_aggregation && peer_updates.len() < 2 {
            info!(
                "[FEDERATED] secure aggregation requires >=2 peers; got {}",
                peer_updates.len()
            );
            return Ok(());
        }

        let aggregate = aggregate_peer_updates(&peer_updates);
        let merged = agent.merge_snapshot(&aggregate, self.local_blend);
        if merged > 0 {
            info!(
                "[FEDERATED] merged {} q-states from {} peers (local_version={} -> new_version={})",
                merged,
                peer_updates.len(),
                local_version,
                agent.policy_version()
            );
        }

        self.garbage_collect(&cluster_dir);
        Ok(())
    }

    fn publish_local_snapshot(
        &self,
        cluster_dir: &Path,
        agent: &QLearningAgent,
    ) -> Result<(), AgentError> {
        let snapshot = agent.snapshot();
        let payload = FederatedPayload {
            cluster_id: self.cluster_id.clone(),
            peer_id: self.peer_id.clone(),
            base_policy_version: snapshot.policy_version,
            policy_version: snapshot.policy_version,
            training_steps: snapshot.training_steps,
            generated_at_epoch_ms: now_epoch_millis(),
            entries: snapshot.entries,
        };
        let signature_hex = sign_payload(&payload, &self.shared_key)?;
        let envelope = FederatedEnvelope {
            payload,
            signature_hex,
        };

        let basename = format!(
            "{}-{}-{}.json",
            sanitize_path_segment(&self.peer_id),
            envelope.payload.policy_version,
            envelope.payload.generated_at_epoch_ms
        );
        let temp_path = cluster_dir.join(format!("{basename}.tmp"));
        let final_path = cluster_dir.join(basename);

        let body = serde_json::to_vec(&envelope)
            .map_err(|err| AgentError::Internal(format!("federated encode failed: {err}")))?;
        fs::write(&temp_path, body)
            .map_err(|err| AgentError::Internal(format!("federated write failed: {err}")))?;
        fs::rename(&temp_path, &final_path)
            .map_err(|err| AgentError::Internal(format!("federated rename failed: {err}")))?;
        Ok(())
    }

    fn collect_peer_updates(
        &self,
        cluster_dir: &Path,
        local_version: u64,
    ) -> Result<Vec<FederatedPayload>, AgentError> {
        let mut latest_by_peer: HashMap<String, FederatedPayload> = HashMap::new();
        let entries = fs::read_dir(cluster_dir)
            .map_err(|err| AgentError::Internal(format!("federated read_dir failed: {err}")))?;

        for entry in entries {
            let Ok(entry) = entry else {
                continue;
            };
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if !path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("json"))
                .unwrap_or(false)
            {
                continue;
            }

            let Ok(raw) = fs::read(&path) else {
                continue;
            };
            let Ok(envelope) = serde_json::from_slice::<FederatedEnvelope>(&raw) else {
                continue;
            };
            if !verify_envelope(&envelope, &self.shared_key) {
                warn!(
                    "[FEDERATED] dropped invalid signature from peer={}",
                    envelope.payload.peer_id
                );
                continue;
            }

            let payload = envelope.payload;
            if payload.cluster_id != self.cluster_id || payload.peer_id == self.peer_id {
                continue;
            }

            if is_stale(payload.generated_at_epoch_ms, self.max_snapshot_age_secs) {
                continue;
            }

            match classify_version(local_version, payload.base_policy_version, payload.policy_version)
            {
                VersionDisposition::Accept => {}
                VersionDisposition::Stale => continue,
                VersionDisposition::Conflict => {
                    warn!(
                        "[FEDERATED] version conflict from peer={} local={} base={} peer={}",
                        payload.peer_id,
                        local_version,
                        payload.base_policy_version,
                        payload.policy_version
                    );
                    continue;
                }
            }

            match latest_by_peer.get(&payload.peer_id) {
                Some(existing) => {
                    let is_newer = payload.policy_version > existing.policy_version
                        || (payload.policy_version == existing.policy_version
                            && payload.generated_at_epoch_ms > existing.generated_at_epoch_ms);
                    if is_newer {
                        latest_by_peer.insert(payload.peer_id.clone(), payload);
                    }
                }
                None => {
                    latest_by_peer.insert(payload.peer_id.clone(), payload);
                }
            }
        }

        Ok(latest_by_peer.into_values().collect())
    }

    fn garbage_collect(&self, cluster_dir: &Path) {
        let retention = self.max_snapshot_age_secs.saturating_mul(4);
        let Ok(entries) = fs::read_dir(cluster_dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            if !metadata.is_file() {
                continue;
            }
            let Ok(modified) = metadata.modified() else {
                continue;
            };
            let Ok(elapsed) = modified.elapsed() else {
                continue;
            };
            if elapsed.as_secs() > retention {
                let _ = fs::remove_file(path);
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum VersionDisposition {
    Accept,
    Stale,
    Conflict,
}

fn classify_version(local: u64, base: u64, remote: u64) -> VersionDisposition {
    if remote <= local {
        return VersionDisposition::Stale;
    }
    if base > local {
        return VersionDisposition::Conflict;
    }
    VersionDisposition::Accept
}

fn aggregate_peer_updates(peer_updates: &[FederatedPayload]) -> QLearningSnapshot {
    let mut buckets: HashMap<(i32, i32), ([f64; 3], f64)> = HashMap::new();
    let mut policy_version = 0u64;
    let mut total_steps = 0u64;

    for payload in peer_updates {
        let weight = (payload.training_steps.max(1) as f64).sqrt().clamp(1.0, 10_000.0);
        policy_version = policy_version.max(payload.policy_version);
        total_steps = total_steps.saturating_add(payload.training_steps.max(1));

        for entry in &payload.entries {
            let slot = buckets
                .entry((entry.edge_bucket, entry.bankroll_bucket))
                .or_insert(([0.0; 3], 0.0));
            for idx in 0..3 {
                slot.0[idx] += entry.q_values[idx].clamp(-50.0, 50.0) * weight;
            }
            slot.1 += weight;
        }
    }

    let mut entries = Vec::with_capacity(buckets.len());
    for ((edge_bucket, bankroll_bucket), (sums, total_weight)) in buckets {
        if total_weight <= 0.0 {
            continue;
        }
        entries.push(FederatedQEntry {
            edge_bucket,
            bankroll_bucket,
            q_values: [
                sums[0] / total_weight,
                sums[1] / total_weight,
                sums[2] / total_weight,
            ],
        });
    }
    entries.sort_by_key(|entry| (entry.edge_bucket, entry.bankroll_bucket));

    QLearningSnapshot {
        policy_version,
        training_steps: total_steps.max(1),
        entries,
    }
}

fn sign_payload(payload: &FederatedPayload, key: &[u8]) -> Result<String, AgentError> {
    let serialized = serde_json::to_vec(payload)
        .map_err(|err| AgentError::Internal(format!("federated payload encode failed: {err}")))?;
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|err| AgentError::Internal(format!("federated hmac init failed: {err}")))?;
    mac.update(&serialized);
    Ok(hex_encode(&mac.finalize().into_bytes()))
}

fn verify_envelope(envelope: &FederatedEnvelope, key: &[u8]) -> bool {
    let Ok(expected) = sign_payload(&envelope.payload, key) else {
        return false;
    };
    expected.eq_ignore_ascii_case(envelope.signature_hex.trim())
}

fn sanitize_path_segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "peer".to_string()
    } else {
        out
    }
}

fn is_stale(generated_at_epoch_ms: u64, max_age_secs: u64) -> bool {
    let now = now_epoch_millis();
    if generated_at_epoch_ms > now {
        return false;
    }
    let age_secs = (now - generated_at_epoch_ms) / 1000;
    age_secs > max_age_secs
}

fn now_epoch_millis() -> u64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    duration.as_millis().min(u64::MAX as u128) as u64
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_classifier_detects_stale_and_conflict() {
        assert!(matches!(
            classify_version(10, 8, 12),
            VersionDisposition::Accept
        ));
        assert!(matches!(
            classify_version(10, 7, 10),
            VersionDisposition::Stale
        ));
        assert!(matches!(
            classify_version(10, 11, 12),
            VersionDisposition::Conflict
        ));
    }

    #[test]
    fn envelope_signature_verification_roundtrip() {
        let payload = FederatedPayload {
            cluster_id: "test-cluster".to_string(),
            peer_id: "peer-a".to_string(),
            base_policy_version: 3,
            policy_version: 4,
            training_steps: 12,
            generated_at_epoch_ms: 123,
            entries: vec![FederatedQEntry {
                edge_bucket: 1,
                bankroll_bucket: 2,
                q_values: [0.1, 0.2, 0.3],
            }],
        };

        let key = b"shared-secret".to_vec();
        let signature_hex = sign_payload(&payload, &key).expect("signing should succeed");
        let envelope = FederatedEnvelope {
            payload: payload.clone(),
            signature_hex,
        };
        assert!(verify_envelope(&envelope, &key));

        let mut tampered = envelope.clone();
        tampered.payload.policy_version = 7;
        assert!(!verify_envelope(&tampered, &key));
    }
}

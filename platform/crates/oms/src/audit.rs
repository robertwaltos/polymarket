use chrono::{DateTime, Utc};
use domain::{OrderAck, OrderRequest};
use risk::RiskCheckReport;
use serde::{Deserialize, Serialize};
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditModeDecision {
    pub requested: domain::ExecutionMode,
    pub effective: domain::ExecutionMode,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SignalDecisionTrace {
    pub strategy_id: Option<uuid::Uuid>,
    pub agent_id: Option<String>,
    pub policy_version: Option<String>,
    pub feature_hash: Option<String>,
    pub confidence: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskDecisionTrace {
    pub reference_price: Option<f64>,
    pub quote_age_secs: Option<i64>,
    pub checks: RiskCheckReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionDecisionTrace {
    pub venue: domain::Venue,
    pub symbol: String,
    pub requested_mode: domain::ExecutionMode,
    pub effective_mode: domain::ExecutionMode,
    pub status: domain::OrderStatus,
    pub simulated: bool,
    pub venue_order_id: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionTrace {
    pub signal: SignalDecisionTrace,
    pub risk: RiskDecisionTrace,
    pub execution: ExecutionDecisionTrace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderAuditRecord {
    pub order: OrderRequest,
    pub ack: OrderAck,
    pub decision: AuditModeDecision,
    #[serde(default)]
    pub decision_trace: Option<DecisionTrace>,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum AuditError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("audit lock poisoned")]
    LockPoisoned,
}

pub trait AuditStore: Send + Sync {
    fn record(&self, record: &OrderAuditRecord) -> Result<(), AuditError>;
    fn recent(&self, limit: usize) -> Result<Vec<OrderAuditRecord>, AuditError>;
}

#[derive(Default)]
pub struct InMemoryAuditStore {
    records: Mutex<Vec<OrderAuditRecord>>,
}

impl InMemoryAuditStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AuditStore for InMemoryAuditStore {
    fn record(&self, record: &OrderAuditRecord) -> Result<(), AuditError> {
        let mut guard = self.records.lock().map_err(|_| AuditError::LockPoisoned)?;
        guard.push(record.clone());
        Ok(())
    }

    fn recent(&self, limit: usize) -> Result<Vec<OrderAuditRecord>, AuditError> {
        let guard = self.records.lock().map_err(|_| AuditError::LockPoisoned)?;
        let take_n = limit.max(1);
        let mut out: Vec<OrderAuditRecord> = guard.iter().rev().take(take_n).cloned().collect();
        out.reverse();
        Ok(out)
    }
}

pub struct JsonlAuditStore {
    path: PathBuf,
    write_lock: Mutex<()>,
}

impl JsonlAuditStore {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, AuditError> {
        let path = path.into();
        ensure_parent_dir(&path)?;
        if !path.exists() {
            File::create(&path)?;
        }
        Ok(Self {
            path,
            write_lock: Mutex::new(()),
        })
    }
}

impl AuditStore for JsonlAuditStore {
    fn record(&self, record: &OrderAuditRecord) -> Result<(), AuditError> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| AuditError::LockPoisoned)?;
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)?;
        let line = serde_json::to_string(record)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        file.flush()?;
        Ok(())
    }

    fn recent(&self, limit: usize) -> Result<Vec<OrderAuditRecord>, AuditError> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        let reader = BufReader::new(file);
        let mut rows: Vec<OrderAuditRecord> = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<OrderAuditRecord>(&line) {
                Ok(record) => rows.push(record),
                Err(_) => {
                    continue;
                }
            }
        }
        let take_n = limit.max(1);
        if rows.len() > take_n {
            rows = rows.split_off(rows.len() - take_n);
        }
        Ok(rows)
    }
}

fn ensure_parent_dir(path: &Path) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all(parent)?;
        }
    }
    Ok(())
}

use crate::rl_agent::QLearningAgent;
use crate::error::AgentError;
use log::info;

pub struct FederatedClient {
    enabled: bool,
}

impl FederatedClient {
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    pub fn sync(&self, _agent: &QLearningAgent) -> Result<(), AgentError> {
        if !self.enabled {
            return Ok(());
        }

        // Placeholder for libp2p gossipsub exchange of q-table snapshots.
        info!("[FEDERATED] Syncing RL parameters with peers (stub)");
        Ok(())
    }
}

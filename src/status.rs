use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

/// Per-account registration status, reported by the SIP registration loop
/// and exposed to the web UI.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RegistrationStatus {
    pub username: String,
    pub domain: String,
    pub registered: bool,
    pub expires: Option<u64>,
    pub transport: Option<String>,
    pub last_error: Option<String>,
    /// Unix epoch seconds of the last update.
    pub updated_at: Option<i64>,
}

/// Shared, thread-safe board of account statuses.
#[derive(Debug, Default)]
pub struct StatusBoard {
    entries: Mutex<HashMap<String, RegistrationStatus>>,
}

impl StatusBoard {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, status: RegistrationStatus) {
        let key = format!("{}@{}", status.username, status.domain);
        self.entries.lock().unwrap().insert(key, status);
    }

    pub fn snapshot(&self) -> Vec<RegistrationStatus> {
        let map = self.entries.lock().unwrap();
        let mut list: Vec<RegistrationStatus> = map.values().cloned().collect();
        list.sort_by(|a, b| a.username.cmp(&b.username));
        list
    }
}

pub fn now_epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Default)]
pub struct CallStats {
    pub total_calls: AtomicU32,
    pub current_calls: AtomicU32,
    pub finished_calls: AtomicU32,
    pub status_codes: Mutex<HashMap<u16, u32>>,
    pub total_duration: AtomicU64, // in milliseconds
}

impl CallStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_total(&self) {
        self.total_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_current(&self) {
        self.current_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_current(&self) {
        self.current_calls.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_finished(&self) {
        self.finished_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn add_status(&self, code: u16) {
        let mut map = self.status_codes.lock().await;
        *map.entry(code).or_insert(0) += 1;
    }

    pub fn add_duration(&self, duration: Duration) {
        self.total_duration
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }
}

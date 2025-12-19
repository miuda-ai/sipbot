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
    pub tx_packets: AtomicU64,
    pub rx_packets: AtomicU64,
    pub tx_bytes: AtomicU64,
    pub rx_bytes: AtomicU64,
    pub rx_lost_packets: AtomicU64,
    pub nack_sent: AtomicU64,
    pub nack_recv: AtomicU64,
    pub nack_recovered: AtomicU64,
}

impl CallStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_rx_lost(&self, count: u64) {
        self.rx_lost_packets.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_nack_sent(&self, count: u64) {
        self.nack_sent.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_nack_recv(&self, count: u64) {
        self.nack_recv.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_nack_recovered(&self, count: u64) {
        self.nack_recovered.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_tx(&self, packets: u64, bytes: u64) {
        self.tx_packets.fetch_add(packets, Ordering::Relaxed);
        self.tx_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn inc_rx(&self, packets: u64, bytes: u64) {
        self.rx_packets.fetch_add(packets, Ordering::Relaxed);
        self.rx_bytes.fetch_add(bytes, Ordering::Relaxed);
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

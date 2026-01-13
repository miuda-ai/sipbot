use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Default)]
pub struct CallStats {
    pub total_planned_calls: AtomicU32,
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

    pub fn set_total_planned(&self, total: u32) {
        self.total_planned_calls.store(total, Ordering::Relaxed);
    }

    pub fn add_total_planned(&self, count: u32) {
        self.total_planned_calls.fetch_add(count, Ordering::Relaxed);
    }

    pub async fn print_summary(&self) {
        let total = self.total_planned_calls.load(Ordering::Relaxed);
        let finished = self.finished_calls.load(Ordering::Relaxed);
        let current = self.current_calls.load(Ordering::Relaxed);
        let total_duration_ms = self.total_duration.load(Ordering::Relaxed);
        let avg_duration = if finished > 0 {
            total_duration_ms as f64 / finished as f64 / 1000.0
        } else {
            0.0
        };

        let status_codes = {
            let map = self.status_codes.lock().await;
            let mut codes: Vec<_> = map.iter().collect();
            codes.sort_by_key(|a| a.0);
            codes
                .iter()
                .map(|(k, v)| format!("{}:{}", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let tx_p = self.tx_packets.load(Ordering::Relaxed);
        let tx_b = self.tx_bytes.load(Ordering::Relaxed);
        let rx_p = self.rx_packets.load(Ordering::Relaxed);
        let rx_b = self.rx_bytes.load(Ordering::Relaxed);
        let rx_lost = self.rx_lost_packets.load(Ordering::Relaxed);
        let loss = if rx_p + rx_lost > 0 {
            rx_lost as f64 * 100.0 / (rx_p + rx_lost) as f64
        } else {
            0.0
        };

        let nack_s = self.nack_sent.load(Ordering::Relaxed);
        let nack_r = self.nack_recv.load(Ordering::Relaxed);
        let nack_rec = self.nack_recovered.load(Ordering::Relaxed);

        println!(
            "Progress: {}/{} (Current: {}), Avg Duration: {:.2}s, Status: [{}], TX: {}p/{}b, RX: {}p/{}b, Loss: {:.2}%, NACK: {}s/{}r/{}rec",
            finished,
            total,
            current,
            avg_duration,
            status_codes,
            tx_p,
            tx_b,
            rx_p,
            rx_b,
            loss,
            nack_s,
            nack_r,
            nack_rec
        );
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

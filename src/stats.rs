use std::array;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

const STATUS_CODE_MAX: usize = 700;

#[derive(Debug)]
pub struct CallStats {
    pub total_planned_calls: AtomicU32,
    pub total_calls: AtomicU32,
    pub current_calls: AtomicU32,
    pub finished_calls: AtomicU32,
    pub status_codes: [AtomicU32; STATUS_CODE_MAX],
    pub total_duration: AtomicU64, // in milliseconds
    pub total_setup_latency_ms: AtomicU64,
    pub setup_samples: AtomicU32,
    pub tx_packets: AtomicU64,
    pub rx_packets: AtomicU64,
    pub tx_bytes: AtomicU64,
    pub rx_bytes: AtomicU64,
    pub rx_lost_packets: AtomicU64,
    pub nack_sent: AtomicU64,
    pub nack_recv: AtomicU64,
    pub nack_recovered: AtomicU64,
    pub total_rtcp_rtt_us: AtomicU64,
    pub rtcp_rtt_samples: AtomicU32,
    pub rx_dtmf_events: AtomicU64,
    pub tx_dtmf_events: AtomicU64,
    pub audio_quality_sample_rate_mismatches: AtomicU64,
    pub audio_quality_shrill_count: AtomicU64,
    pub audio_quality_muffled_count: AtomicU64,
    pub audio_quality_clipping_frames: AtomicU64,
    pub audio_quality_silence_frames: AtomicU64,
    pub audio_quality_total_frames: AtomicU64,
}

impl Default for CallStats {
    fn default() -> Self {
        Self {
            total_planned_calls: AtomicU32::new(0),
            total_calls: AtomicU32::new(0),
            current_calls: AtomicU32::new(0),
            finished_calls: AtomicU32::new(0),
            status_codes: array::from_fn(|_| AtomicU32::new(0)),
            total_duration: AtomicU64::new(0),
            total_setup_latency_ms: AtomicU64::new(0),
            setup_samples: AtomicU32::new(0),
            tx_packets: AtomicU64::new(0),
            rx_packets: AtomicU64::new(0),
            tx_bytes: AtomicU64::new(0),
            rx_bytes: AtomicU64::new(0),
            rx_lost_packets: AtomicU64::new(0),
            nack_sent: AtomicU64::new(0),
            nack_recv: AtomicU64::new(0),
            nack_recovered: AtomicU64::new(0),
            total_rtcp_rtt_us: AtomicU64::new(0),
            rtcp_rtt_samples: AtomicU32::new(0),
            rx_dtmf_events: AtomicU64::new(0),
            tx_dtmf_events: AtomicU64::new(0),
            audio_quality_sample_rate_mismatches: AtomicU64::new(0),
            audio_quality_shrill_count: AtomicU64::new(0),
            audio_quality_muffled_count: AtomicU64::new(0),
            audio_quality_clipping_frames: AtomicU64::new(0),
            audio_quality_silence_frames: AtomicU64::new(0),
            audio_quality_total_frames: AtomicU64::new(0),
        }
    }
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

    pub fn print_progress(&self) {
        let current = self.current_calls.load(Ordering::Relaxed);
        let finished = self.finished_calls.load(Ordering::Relaxed);
        let total = self.total_planned_calls.load(Ordering::Relaxed);
        let c200 = self.status_count(200);
        let c180 = self.status_count(180);
        let c183 = self.status_count(183);

        let mut c3xx = 0;
        let mut c4xx = 0;
        let mut c5xx = 0;
        let mut c6xx = 0;

        for code in 300..700 {
            let count = self.status_codes[code].load(Ordering::Relaxed);
            if count == 0 {
                continue;
            }
            match code {
                300..=399 => c3xx += count,
                400..=499 => c4xx += count,
                500..=599 => c5xx += count,
                600..=699 => c6xx += count,
                _ => {}
            }
        }

        print!(
            "\rProgress: {}/{}, Active: {}, 200: {}, 180: {}, 183: {}, 3xx: {}, 4xx: {}, 5xx: {}, 6xx: {}",
            finished, total, current, c200, c180, c183, c3xx, c4xx, c5xx, c6xx
        );
        use std::io::Write;
        let _ = std::io::stdout().flush();
    }

    pub fn print_summary(&self) {
        let total = self.total_planned_calls.load(Ordering::Relaxed);
        let finished = self.finished_calls.load(Ordering::Relaxed);
        let current = self.current_calls.load(Ordering::Relaxed);

        if total == 0 && finished == 0 && current == 0 {
            return;
        }

        let total_duration_ms = self.total_duration.load(Ordering::Relaxed);
        let avg_duration = if finished > 0 {
            total_duration_ms as f64 / finished as f64 / 1000.0
        } else {
            0.0
        };

        let setup_samples = self.setup_samples.load(Ordering::Relaxed);
        let total_setup_ms = self.total_setup_latency_ms.load(Ordering::Relaxed);
        let avg_setup_ms = if setup_samples > 0 {
            total_setup_ms as f64 / setup_samples as f64
        } else {
            0.0
        };

        let mut codes = Vec::new();
        for code in 100..700 {
            let count = self.status_codes[code].load(Ordering::Relaxed);
            if count > 0 {
                codes.push(format!("{}:{}", code, count));
            }
        }
        let status_codes = codes.join(", ");

        let tx_p = self.tx_packets.load(Ordering::Relaxed);
        let tx_b = self.tx_bytes.load(Ordering::Relaxed);
        let rx_p = self.rx_packets.load(Ordering::Relaxed);
        let rx_b = self.rx_bytes.load(Ordering::Relaxed);
        let loss = self.average_loss_rate();
        let avg_rtcp_rtt_ms = self.average_rtcp_rtt_ms();

        let nack_s = self.nack_sent.load(Ordering::Relaxed);
        let nack_r = self.nack_recv.load(Ordering::Relaxed);
        let nack_rec = self.nack_recovered.load(Ordering::Relaxed);

        println!(
            "Progress: {}/{} (Current: {}), Avg Duration: {:.2}s, Avg Setup Latency: {:.2}ms, Avg RTCP RTT: {:.2}ms, Status: [{}], TX: {}p/{}b, RX: {}p/{}b, Avg Loss: {:.2}%, NACK: {}s/{}r/{}rec",
            finished,
            total,
            current,
            avg_duration,
            avg_setup_ms,
            avg_rtcp_rtt_ms,
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

    pub fn current(&self) -> u32 {
        self.current_calls.load(Ordering::Relaxed)
    }

    pub fn inc_finished(&self) {
        self.finished_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_status(&self, code: u16) {
        let idx = code as usize;
        if idx < STATUS_CODE_MAX {
            self.status_codes[idx].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn status_count(&self, code: usize) -> u32 {
        if code < STATUS_CODE_MAX {
            self.status_codes[code].load(Ordering::Relaxed)
        } else {
            0
        }
    }

    pub fn add_duration(&self, duration: Duration) {
        self.total_duration
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn add_setup_latency(&self, duration: Duration) {
        self.total_setup_latency_ms
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
        self.setup_samples.fetch_add(1, Ordering::Relaxed);
    }

    pub fn average_loss_rate(&self) -> f64 {
        let rx = self.rx_packets.load(Ordering::Relaxed);
        let lost = self.rx_lost_packets.load(Ordering::Relaxed);
        if rx + lost == 0 {
            return 0.0;
        }
        lost as f64 * 100.0 / (rx + lost) as f64
    }

    pub fn add_rtcp_rtt(&self, rtt: Duration) {
        self.total_rtcp_rtt_us
            .fetch_add(rtt.as_micros() as u64, Ordering::Relaxed);
        self.rtcp_rtt_samples.fetch_add(1, Ordering::Relaxed);
    }

    pub fn average_rtcp_rtt_ms(&self) -> f64 {
        let samples = self.rtcp_rtt_samples.load(Ordering::Relaxed);
        if samples == 0 {
            return 0.0;
        }
        let total_us = self.total_rtcp_rtt_us.load(Ordering::Relaxed);
        (total_us as f64 / samples as f64) / 1000.0
    }

    pub fn inc_rx_dtmf(&self) {
        self.rx_dtmf_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_tx_dtmf(&self) {
        self.tx_dtmf_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_audio_quality_stats(
        &self,
        mismatch: u64,
        shrill: u64,
        muffled: u64,
        clipping: u64,
        silence: u64,
        frames: u64,
    ) {
        self.audio_quality_sample_rate_mismatches
            .fetch_add(mismatch, Ordering::Relaxed);
        self.audio_quality_shrill_count
            .fetch_add(shrill, Ordering::Relaxed);
        self.audio_quality_muffled_count
            .fetch_add(muffled, Ordering::Relaxed);
        self.audio_quality_clipping_frames
            .fetch_add(clipping, Ordering::Relaxed);
        self.audio_quality_silence_frames
            .fetch_add(silence, Ordering::Relaxed);
        self.audio_quality_total_frames
            .fetch_add(frames, Ordering::Relaxed);
    }
}

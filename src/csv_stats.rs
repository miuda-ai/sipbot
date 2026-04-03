use crate::stats::CallStats;
use anyhow::Result;
use chrono::Local;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::time::{interval, Duration};
use tracing::{error, info};

/// CSV Statistics Recorder for load testing
pub struct CsvStatsRecorder {
    stats: Arc<CallStats>,
    file_path: String,
    interval_secs: u64,
    start_time: std::time::Instant,
}

impl CsvStatsRecorder {
    pub fn new(
        stats: Arc<CallStats>,
        file_path: impl Into<String>,
        interval_secs: u64,
    ) -> Self {
        Self {
            stats,
            file_path: file_path.into(),
            interval_secs,
            start_time: std::time::Instant::now(),
        }
    }

    /// Write CSV header
    async fn write_header(&self, file: &mut tokio::fs::File) -> Result<()> {
        let header = "timestamp,elapsed_time_ms,total_planned,total_calls,current_calls,finished_calls,c200_count,c180_count,c183_count,c3xx_count,c4xx_count,c5xx_count,c6xx_count,avg_duration_ms,avg_setup_latency_ms,avg_rtcp_rtt_ms,tx_packets,rx_packets,tx_bytes,rx_bytes,rx_lost_packets,packet_loss_rate_pct,nack_sent,nack_recv,nack_recovered\n";
        file.write_all(header.as_bytes()).await?;
        file.flush().await?;
        Ok(())
    }

    /// Format current statistics as CSV row
    fn format_csv_row(&self) -> String {
        let timestamp = Local::now().format("%Y-%m-%dT%H:%M:%S%.3f");
        let elapsed_ms = self.start_time.elapsed().as_millis();

        let total_planned = self.stats.total_planned_calls.load(Ordering::Relaxed);
        let total_calls = self.stats.total_calls.load(Ordering::Relaxed);
        let current = self.stats.current_calls.load(Ordering::Relaxed);
        let finished = self.stats.finished_calls.load(Ordering::Relaxed);

        // SIP response code counts
        let c200 = self.stats.status_codes[200].load(Ordering::Relaxed);
        let c180 = self.stats.status_codes[180].load(Ordering::Relaxed);
        let c183 = self.stats.status_codes[183].load(Ordering::Relaxed);

        let mut c3xx = 0u32;
        let mut c4xx = 0u32;
        let mut c5xx = 0u32;
        let mut c6xx = 0u32;
        for code in 300..700 {
            let count = self.stats.status_codes[code].load(Ordering::Relaxed);
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

        // Duration and latency
        let total_duration_ms = self.stats.total_duration.load(Ordering::Relaxed);
        let avg_duration = if finished > 0 {
            total_duration_ms as f64 / finished as f64
        } else {
            0.0
        };

        let setup_samples = self.stats.setup_samples.load(Ordering::Relaxed);
        let total_setup_ms = self.stats.total_setup_latency_ms.load(Ordering::Relaxed);
        let avg_setup_ms = if setup_samples > 0 {
            total_setup_ms as f64 / setup_samples as f64
        } else {
            0.0
        };

        // RTCP RTT
        let rtcp_samples = self.stats.rtcp_rtt_samples.load(Ordering::Relaxed);
        let total_rtcp_us = self.stats.total_rtcp_rtt_us.load(Ordering::Relaxed);
        let avg_rtcp_ms = if rtcp_samples > 0 {
            (total_rtcp_us as f64 / rtcp_samples as f64) / 1000.0
        } else {
            0.0
        };

        // RTP stats
        let tx_packets = self.stats.tx_packets.load(Ordering::Relaxed);
        let rx_packets = self.stats.rx_packets.load(Ordering::Relaxed);
        let tx_bytes = self.stats.tx_bytes.load(Ordering::Relaxed);
        let rx_bytes = self.stats.rx_bytes.load(Ordering::Relaxed);
        let rx_lost = self.stats.rx_lost_packets.load(Ordering::Relaxed);
        let loss_rate = self.stats.average_loss_rate();

        // NACK stats
        let nack_sent = self.stats.nack_sent.load(Ordering::Relaxed);
        let nack_recv = self.stats.nack_recv.load(Ordering::Relaxed);
        let nack_recovered = self.stats.nack_recovered.load(Ordering::Relaxed);

        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{:.2},{:.2},{:.2},{},{},{},{},{},{:.2},{},{},{}\n",
            timestamp,
            elapsed_ms,
            total_planned,
            total_calls,
            current,
            finished,
            c200,
            c180,
            c183,
            c3xx,
            c4xx,
            c5xx,
            c6xx,
            avg_duration,
            avg_setup_ms,
            avg_rtcp_ms,
            tx_packets,
            rx_packets,
            tx_bytes,
            rx_bytes,
            rx_lost,
            loss_rate,
            nack_sent,
            nack_recv,
            nack_recovered
        )
    }

    /// Start the CSV recording loop
    pub async fn start(&self) -> Result<()> {
        let path = Path::new(&self.file_path);

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }

        // Create/truncate file and write header
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .await?;

        self.write_header(&mut file).await?;

        info!("[CSV] Started recording stats to: {}", self.file_path);

        // Record initial state
        let initial_row = self.format_csv_row();
        file.write_all(initial_row.as_bytes()).await?;
        file.flush().await?;

        // Start periodic recording
        let mut ticker = interval(Duration::from_secs(self.interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            let row = self.format_csv_row();
            if let Err(e) = file.write_all(row.as_bytes()).await {
                error!("[CSV] Failed to write stats: {}", e);
                continue;
            }
            if let Err(e) = file.flush().await {
                error!("[CSV] Failed to flush stats: {}", e);
                continue;
            }
        }
    }

    /// Start the recorder in a background task
    pub fn spawn(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = self.start().await {
                error!("[CSV] Recorder failed: {}", e);
            }
        })
    }
}

/// Write final summary to a separate file
pub async fn write_final_summary(
    stats: &CallStats,
    file_path: impl AsRef<Path>,
) -> Result<()> {
    let path = file_path.as_ref();
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .await?;

    let finished = stats.finished_calls.load(Ordering::Relaxed);
    let total = stats.total_planned_calls.load(Ordering::Relaxed);
    let current = stats.current_calls.load(Ordering::Relaxed);

    let total_duration_ms = stats.total_duration.load(Ordering::Relaxed);
    let avg_duration = if finished > 0 {
        total_duration_ms as f64 / finished as f64 / 1000.0
    } else {
        0.0
    };

    let setup_samples = stats.setup_samples.load(Ordering::Relaxed);
    let total_setup_ms = stats.total_setup_latency_ms.load(Ordering::Relaxed);
    let avg_setup_ms = if setup_samples > 0 {
        total_setup_ms as f64 / setup_samples as f64
    } else {
        0.0
    };

    let avg_rtcp_ms = stats.average_rtcp_rtt_ms();

    let summary = format!(
        r#"Sipbot Load Test Summary
==========================
Timestamp: {}

Call Statistics
---------------
Total Planned: {}
Total Finished: {}
Current Active: {}
Success Rate: {:.2}%

Timing
------
Average Call Duration: {:.2} seconds
Average Setup Latency: {:.2} ms
Average RTCP RTT: {:.2} ms

SIP Response Codes
------------------
"#,
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        total,
        finished,
        current,
        if total > 0 {
            finished as f64 * 100.0 / total as f64
        } else {
            0.0
        },
        avg_duration,
        avg_setup_ms,
        avg_rtcp_ms
    );

    file.write_all(summary.as_bytes()).await?;

    // Write response code counts
    for code in 100..700 {
        let count = stats.status_codes[code].load(Ordering::Relaxed);
        if count > 0 {
            let line = format!("  {}: {}\n", code, count);
            file.write_all(line.as_bytes()).await?;
        }
    }

    // RTP Stats
    let tx_packets = stats.tx_packets.load(Ordering::Relaxed);
    let rx_packets = stats.rx_packets.load(Ordering::Relaxed);
    let tx_bytes = stats.tx_bytes.load(Ordering::Relaxed);
    let rx_bytes = stats.rx_bytes.load(Ordering::Relaxed);
    let rx_lost = stats.rx_lost_packets.load(Ordering::Relaxed);
    let loss_rate = stats.average_loss_rate();

    let rtp_summary = format!(
        r#"
RTP Statistics
--------------
TX Packets: {}
RX Packets: {}
TX Bytes: {}
RX Bytes: {}
RX Lost Packets: {}
Packet Loss Rate: {:.2}%

NACK Statistics
---------------
NACK Sent: {}
NACK Received: {}
NACK Recovered: {}
"#,
        tx_packets,
        rx_packets,
        tx_bytes,
        rx_bytes,
        rx_lost,
        loss_rate,
        stats.nack_sent.load(Ordering::Relaxed),
        stats.nack_recv.load(Ordering::Relaxed),
        stats.nack_recovered.load(Ordering::Relaxed)
    );

    file.write_all(rtp_summary.as_bytes()).await?;
    file.flush().await?;

    info!("[CSV] Final summary written to: {}", path.display());
    Ok(())
}

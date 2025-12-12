use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};
use tracing::{error, info};

pub struct Recorder {
    tx: mpsc::UnboundedSender<AudioPacket>,
}

enum AudioPacket {
    Rx(Vec<i16>),
    Tx(Vec<i16>),
    Stop,
}

impl Drop for Recorder {
    fn drop(&mut self) {
        let _ = self.tx.send(AudioPacket::Stop);
    }
}

impl Recorder {
    pub fn new(username: String, path: PathBuf) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            info!("[{}] Recorder started: {:?}", username, path);
            let spec = hound::WavSpec {
                channels: 2,
                sample_rate: 8000,
                bits_per_sample: 16,
                sample_format: hound::SampleFormat::Int,
            };
            let mut writer = match hound::WavWriter::create(&path, spec) {
                Ok(w) => w,
                Err(e) => {
                    error!("[{}] Failed to create wav writer: {:?}", username, e);
                    return;
                }
            };

            let mut rx_buffer: Vec<i16> = Vec::new();
            let mut tx_buffer: Vec<i16> = Vec::new();

            // 20ms at 8000Hz = 160 samples
            let chunk_size = 160;
            let mut ticker = interval(Duration::from_millis(20));

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let rx_chunk = if rx_buffer.len() >= chunk_size {
                            rx_buffer.drain(0..chunk_size).collect::<Vec<_>>()
                        } else {
                            let mut chunk = rx_buffer.drain(..).collect::<Vec<_>>();
                            chunk.resize(chunk_size, 0);
                            chunk
                        };

                        let tx_chunk = if tx_buffer.len() >= chunk_size {
                            tx_buffer.drain(0..chunk_size).collect::<Vec<_>>()
                        } else {
                            let mut chunk = tx_buffer.drain(..).collect::<Vec<_>>();
                            chunk.resize(chunk_size, 0);
                            chunk
                        };

                        for i in 0..chunk_size {
                            // Interleaved: Left=RX, Right=TX
                            if let Err(e) = writer.write_sample(rx_chunk[i]) {
                                error!("[{}] Failed to write RX sample: {:?}", username, e);
                            }
                            if let Err(e) = writer.write_sample(tx_chunk[i]) {
                                error!("[{}] Failed to write TX sample: {:?}", username, e);
                            }
                        }
                    }
                    msg = rx.recv() => {
                        match msg {
                            Some(AudioPacket::Rx(samples)) => {
                                rx_buffer.extend(samples);
                            }
                            Some(AudioPacket::Tx(samples)) => {
                                tx_buffer.extend(samples);
                            }
                            Some(AudioPacket::Stop) | None => {
                                break;
                            }
                        }
                    }
                }
            }
            if let Err(e) = writer.finalize() {
                error!("Failed to finalize wav writer: {:?}", e);
            }
            info!("[{}] Recorder stopped: {:?}", username, path);
        });

        Self { tx }
    }

    pub fn record_rx(&self, samples: &[i16]) {
        let _ = self.tx.send(AudioPacket::Rx(samples.to_vec()));
    }

    pub fn record_tx(&self, samples: &[i16]) {
        let _ = self.tx.send(AudioPacket::Tx(samples.to_vec()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_recorder() {
        let path = std::env::temp_dir().join("test_recorder.wav");
        if path.exists() {
            std::fs::remove_file(&path).unwrap();
        }

        let recorder = Recorder::new("mock".to_string(), path.clone());

        // Send 160 samples (20ms)
        let samples = vec![1000; 160];
        recorder.record_rx(&samples);
        recorder.record_tx(&samples);

        // Wait for at least one tick (20ms) + some buffer
        tokio::time::sleep(Duration::from_millis(100)).await;

        drop(recorder);
        // Wait for task to finish
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(path.exists());

        let mut reader = hound::WavReader::open(&path).unwrap();
        let spec = reader.spec();
        assert_eq!(spec.channels, 2);
        assert_eq!(spec.sample_rate, 8000);

        let read_samples: Vec<i16> = reader.samples().map(|s| s.unwrap()).collect();

        // We expect at least 160 * 2 samples.
        // Since we waited 100ms, we might have recorded 5 chunks (5 * 160 * 2 = 1600 samples).
        // The first chunk should have our data. Subsequent chunks should be 0.

        assert!(read_samples.len() >= 320);

        // Check first chunk (Left=RX, Right=TX)
        // RX=1000, TX=1000
        assert_eq!(read_samples[0], 1000);
        assert_eq!(read_samples[1], 1000);

        // Check silence in later chunks (if any)
        // If we waited 100ms, we likely have silence.
        if read_samples.len() > 320 {
            // The 161th frame (index 320, 321) should be 0 if we didn't send more data
            // But wait, if we sent only 160 samples, and the loop ticked multiple times,
            // the buffer would be empty for subsequent ticks.
            // So yes, silence.
            // Note: The loop might tick before we send data?
            // If tick happens before recv, we get silence first.
            // But we sleep 100ms.
            // It's racy to check exact index of non-silence without synchronization.
            // But we can check that we have *some* 1000s.
            assert!(read_samples.contains(&1000));
        }
    }
}

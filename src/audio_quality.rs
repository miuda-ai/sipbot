use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AudioQualityConfig {
    pub enabled: bool,
    pub sample_rate_check: bool,
    pub clipping_threshold: f64,
    pub shrill_threshold: f64,
    pub muffled_threshold: f64,
    pub silence_threshold_rms: f64,
    pub report_interval: usize,
}

impl Default for AudioQualityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sample_rate_check: true,
            clipping_threshold: 0.95,
            shrill_threshold: 0.65,
            muffled_threshold: 0.15,
            silence_threshold_rms: 50.0,
            report_interval: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AudioQualityReport {
    pub sample_count_mismatch: bool,
    pub expected_samples: usize,
    pub actual_samples: usize,
    pub ts_drift_ppm: f64,
    pub rms: f64,
    pub clipping_ratio: f64,
    pub dc_offset: f64,
    pub zero_crossing_rate: f64,
    pub spectral_tilt: f64,
    pub high_freq_ratio: f64,
    pub is_shrill: bool,
    pub is_muffled: bool,
    pub is_silence: bool,
    pub label: String,
}

impl Default for AudioQualityReport {
    fn default() -> Self {
        Self {
            sample_count_mismatch: false,
            expected_samples: 0,
            actual_samples: 0,
            ts_drift_ppm: 0.0,
            rms: 0.0,
            clipping_ratio: 0.0,
            dc_offset: 0.0,
            zero_crossing_rate: 0.0,
            spectral_tilt: 0.0,
            high_freq_ratio: 0.0,
            is_shrill: false,
            is_muffled: false,
            is_silence: true,
            label: String::new(),
        }
    }
}

pub struct AudioQualityAnalyzer {
    config: AudioQualityConfig,
    frame_count: usize,
    pub total_frames: usize,
    pub mismatch_count: usize,
    pub shrill_count: usize,
    pub muffled_count: usize,
    pub clipping_frames: usize,
    pub silence_frames: usize,
    total_clipping_ratio: f64,
    total_rms: f64,
    total_spectral_tilt: f64,
    last_rtp_ts: Option<u32>,
}

impl AudioQualityAnalyzer {
    pub fn new(config: AudioQualityConfig) -> Self {
        Self {
            config,
            frame_count: 0,
            total_frames: 0,
            mismatch_count: 0,
            shrill_count: 0,
            muffled_count: 0,
            clipping_frames: 0,
            silence_frames: 0,
            total_clipping_ratio: 0.0,
            total_rms: 0.0,
            total_spectral_tilt: 0.0,
            last_rtp_ts: None,
        }
    }

    pub fn analyze_frame(
        &mut self,
        pcm: &[i16],
        rtp_timestamp: u32,
        clock_rate: u32,
        actual_sample_rate: u32,
        frame_duration_ms: u32,
    ) -> AudioQualityReport {
        self.frame_count += 1;
        self.total_frames += 1;
        let mut report = AudioQualityReport::default();

        if pcm.is_empty() {
            report.is_silence = true;
            report.label = "empty".to_string();
            return report;
        }

        let expected = (actual_sample_rate as usize) * frame_duration_ms as usize / 1000;
        report.expected_samples = expected;
        report.actual_samples = pcm.len();

        if self.config.sample_rate_check && expected > 0 {
            let ratio = pcm.len() as f64 / expected as f64;
            if (ratio - 1.0).abs() > 0.05 {
                report.sample_count_mismatch = true;
                self.mismatch_count += 1;
            }
        }

        let sum_sq: f64 = pcm.iter().map(|&s| (s as f64).powi(2)).sum();
        report.rms = (sum_sq / pcm.len() as f64).sqrt();
        self.total_rms += report.rms;

        if report.rms < self.config.silence_threshold_rms {
            report.is_silence = true;
            self.silence_frames += 1;
            report.label = "silence".to_string();
            report_periodic(
                self.config.enabled,
                self.frame_count,
                self.config.report_interval,
                &report,
            );
            return report;
        }
        report.is_silence = false;

        let sum: f64 = pcm.iter().map(|&s| s as f64).sum();
        report.dc_offset = sum / pcm.len() as f64;

        let max_val = i16::MAX as f64;
        let clip_count = pcm
            .iter()
            .filter(|&&s| s.unsigned_abs() as f64 >= max_val * self.config.clipping_threshold)
            .count();
        report.clipping_ratio = clip_count as f64 / pcm.len() as f64;
        self.total_clipping_ratio += report.clipping_ratio;
        if report.clipping_ratio > 0.01 {
            self.clipping_frames += 1;
        }

        let zcr = pcm
            .windows(2)
            .filter(|w| (w[0] >= 0 && w[1] < 0) || (w[0] < 0 && w[1] >= 0))
            .count();
        report.zero_crossing_rate = zcr as f64 / pcm.len() as f64;

        let diff_rms = if pcm.len() > 1 {
            let sum_diff_sq: f64 = pcm
                .windows(2)
                .map(|w| {
                    let d = (w[1] as f64) - (w[0] as f64);
                    d * d
                })
                .sum();
            (sum_diff_sq / (pcm.len() - 1) as f64).sqrt()
        } else {
            0.0
        };

        report.spectral_tilt = if report.rms > 0.0 {
            (diff_rms / report.rms).min(2.0)
        } else {
            0.0
        };
        self.total_spectral_tilt += report.spectral_tilt;

        let zcr_norm = report.zero_crossing_rate * actual_sample_rate as f64 / 8000.0;
        report.high_freq_ratio = report.spectral_tilt.min(1.0);

        if report.high_freq_ratio > self.config.shrill_threshold && zcr_norm > 0.08 {
            report.is_shrill = true;
            self.shrill_count += 1;
        }
        if report.spectral_tilt < self.config.muffled_threshold
            && zcr_norm < 0.04
            && report.rms > self.config.silence_threshold_rms * 2.0
        {
            report.is_muffled = true;
            self.muffled_count += 1;
        }

        if let Some(last_ts) = self.last_rtp_ts {
            let delta = rtp_timestamp.wrapping_sub(last_ts) as i32;
            let expected_delta = (clock_rate as i32) * frame_duration_ms as i32 / 1000;
            if delta > 0 && expected_delta > 0 {
                let drift =
                    (delta as i64 - expected_delta as i64) * 1_000_000 / expected_delta as i64;
                report.ts_drift_ppm = drift as f64;
            }
        }
        self.last_rtp_ts = Some(rtp_timestamp);

        if report.is_shrill {
            report.label = "shrill".to_string();
        } else if report.is_muffled {
            report.label = "muffled".to_string();
        } else if report.clipping_ratio > 0.01 {
            report.label = "clipping".to_string();
        } else if report.rms > 500.0 {
            report.label = "speech".to_string();
        } else {
            report.label = "noise".to_string();
        }

        report_periodic(
            self.config.enabled,
            self.frame_count,
            self.config.report_interval,
            &report,
        );

        report
    }

    pub fn summary(&self) -> String {
        let avg_rms = self.total_rms / self.total_frames.max(1) as f64;
        let avg_tilt = self.total_spectral_tilt / self.total_frames.max(1) as f64;
        let avg_clip = self.total_clipping_ratio / self.total_frames.max(1) as f64;
        format!(
            "AudioQuality: frames={}, mismatch={}, shrill={}, muffled={}, clipping_frames={}, silence_frames={}, avg_rms={:.1}, avg_tilt={:.3}, avg_clip={:.2}",
            self.total_frames, self.mismatch_count, self.shrill_count,
            self.muffled_count, self.clipping_frames, self.silence_frames,
            avg_rms, avg_tilt, avg_clip * 100.0
        )
    }
}

fn report_periodic(enabled: bool, frame_count: usize, interval: usize, report: &AudioQualityReport) {
    if enabled && frame_count % interval == 0 {
        tracing::debug!(
            "[AudioQuality] frame={} rms={:.1} tilt={:.3} zcr={:.3} clip={:.2} dc={:.1} {}",
            frame_count,
            report.rms,
            report.spectral_tilt,
            report.zero_crossing_rate,
            report.clipping_ratio * 100.0,
            report.dc_offset,
            report.label,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sine(freq: f64, sr: u32, ms: u32) -> Vec<i16> {
        let n = (sr as f64 * ms as f64 / 1000.0) as usize;
        (0..n)
            .map(|i| {
                let t = i as f64 / sr as f64;
                (std::f64::consts::TAU * freq * t).sin().mul_add(16000.0, 0.0) as i16
            })
            .collect()
    }

    fn noise(sr: u32, ms: u32, amp: f64) -> Vec<i16> {
        use rand::RngExt;
        let n = (sr as f64 * ms as f64 / 1000.0) as usize;
        let mut rng = rand::rng();
        (0..n)
            .map(|_| (rng.random::<f64>().mul_add(2.0, -1.0) * amp) as i16)
            .collect()
    }

    #[test]
    fn test_silence_detection() {
        let mut aq = AudioQualityAnalyzer::new(AudioQualityConfig::default());
        let r = aq.analyze_frame(&[0i16; 160], 0, 8000, 8000, 20);
        assert!(r.is_silence);
        assert_eq!(r.label, "silence");
    }

    #[test]
    fn test_speech_level() {
        let mut aq = AudioQualityAnalyzer::new(AudioQualityConfig::default());
        let pcm = sine(440.0, 8000, 20);
        let r = aq.analyze_frame(&pcm, 0, 8000, 8000, 20);
        assert!(!r.is_silence);
        assert!(r.rms > 100.0);
        assert_eq!(r.actual_samples, 160);
    }

    #[test]
    fn test_sample_rate_mismatch_detection() {
        let mut aq = AudioQualityAnalyzer::new(AudioQualityConfig::default());
        let pcm = sine(440.0, 16000, 20);
        let r = aq.analyze_frame(&pcm, 0, 8000, 8000, 20);
        assert!(r.sample_count_mismatch);
        assert_eq!(r.actual_samples, 320);
        assert_eq!(r.expected_samples, 160);
    }

    #[test]
    fn test_shrill_detection() {
        let mut cfg = AudioQualityConfig::default();
        cfg.shrill_threshold = 0.3;
        let mut aq = AudioQualityAnalyzer::new(cfg);
        let pcm = sine(3000.0, 8000, 40);
        let r = aq.analyze_frame(&pcm, 0, 8000, 8000, 20);
        assert!(r.is_shrill);
    }

    #[test]
    fn test_muffled_detection() {
        let mut cfg = AudioQualityConfig::default();
        cfg.muffled_threshold = 0.2;
        let mut aq = AudioQualityAnalyzer::new(cfg);
        let pcm = sine(80.0, 8000, 40);
        let r = aq.analyze_frame(&pcm, 0, 8000, 8000, 20);
        assert!(r.is_muffled);
    }

    #[test]
    fn test_clipping_detection() {
        let mut aq = AudioQualityAnalyzer::new(AudioQualityConfig::default());
        let mut pcm = sine(440.0, 8000, 20);
        for s in pcm.iter_mut().take(40) {
            *s = i16::MAX;
        }
        let r = aq.analyze_frame(&pcm, 0, 8000, 8000, 20);
        assert!(r.clipping_ratio > 0.01);
    }

    #[test]
    fn test_dc_offset() {
        let mut aq = AudioQualityAnalyzer::new(AudioQualityConfig::default());
        let pcm: Vec<i16> = (0..160).map(|_| 1000i16).collect();
        let r = aq.analyze_frame(&pcm, 0, 8000, 8000, 20);
        assert!((r.dc_offset - 1000.0).abs() < 1.0);
    }

    #[test]
    fn test_noise_high_zcr() {
        let mut aq = AudioQualityAnalyzer::new(AudioQualityConfig::default());
        let n = noise(8000, 20, 5000.0);
        let r = aq.analyze_frame(&n, 0, 8000, 8000, 20);
        assert!(!r.is_silence);
        assert!(r.zero_crossing_rate > 0.1);
    }

    #[test]
    fn test_empty_frame() {
        let mut aq = AudioQualityAnalyzer::new(AudioQualityConfig::default());
        let r = aq.analyze_frame(&[], 0, 8000, 8000, 20);
        assert!(r.is_silence);
        assert_eq!(r.label, "empty");
    }

    #[test]
    fn test_summary() {
        let mut aq = AudioQualityAnalyzer::new(AudioQualityConfig::default());
        for i in 0..10 {
            let pcm = sine(440.0, 8000, 20);
            aq.analyze_frame(&pcm, i * 160, 8000, 8000, 20);
        }
        let s = aq.summary();
        assert!(s.contains("frames=10"));
    }

    #[test]
    fn test_config_default() {
        let cfg = AudioQualityConfig::default();
        assert!(!cfg.enabled);
        assert!((cfg.clipping_threshold - 0.95).abs() < 0.01);
    }

    #[test]
    fn test_report_periodic_only_when_enabled() {
        let mut cfg = AudioQualityConfig::default();
        cfg.enabled = true;
        cfg.report_interval = 1;
        let mut aq = AudioQualityAnalyzer::new(cfg);
        let pcm = sine(440.0, 8000, 20);
        let r = aq.analyze_frame(&pcm, 0, 8000, 8000, 20);
        assert!(!r.is_silence);
    }

    #[test]
    fn test_timestamp_drift() {
        let mut aq = AudioQualityAnalyzer::new(AudioQualityConfig::default());
        let pcm = sine(440.0, 8000, 20);
        aq.analyze_frame(&pcm, 0, 8000, 8000, 20);
        let r = aq.analyze_frame(&pcm, 400, 8000, 8000, 20);
        assert!(r.ts_drift_ppm.abs() > 0.0);
    }

    #[test]
    fn test_different_durations() {
        let mut aq = AudioQualityAnalyzer::new(AudioQualityConfig::default());
        let pcm = sine(440.0, 8000, 10);
        let r = aq.analyze_frame(&pcm, 0, 8000, 8000, 10);
        assert_eq!(r.actual_samples, 80);
    }

    #[test]
    fn test_sample_rate_check_disabled() {
        let mut cfg = AudioQualityConfig::default();
        cfg.sample_rate_check = false;
        let mut aq = AudioQualityAnalyzer::new(cfg);
        let pcm = sine(440.0, 16000, 20);
        let r = aq.analyze_frame(&pcm, 0, 8000, 8000, 20);
        assert!(!r.sample_count_mismatch);
    }

    #[test]
    fn test_silence_threshold() {
        let mut cfg = AudioQualityConfig::default();
        cfg.silence_threshold_rms = 10000.0;
        let mut aq = AudioQualityAnalyzer::new(cfg);
        let pcm = sine(440.0, 8000, 20);
        let r = aq.analyze_frame(&pcm, 0, 8000, 8000, 20);
        // sine at amplitude 16000 has RMS ~11313, threshold 10000 means NOT silence
        assert!(!r.is_silence);
    }

    #[test]
    fn test_noise_label_when_not_speech_silence_or_clipping() {
        let mut cfg = AudioQualityConfig::default();
        cfg.silence_threshold_rms = 1.0;
        cfg.shrill_threshold = 1.0;
        let mut aq = AudioQualityAnalyzer::new(cfg);
        let n = noise(8000, 20, 100.0);
        let r = aq.analyze_frame(&n, 0, 8000, 8000, 20);
        assert_eq!(r.label, "noise");
    }
}

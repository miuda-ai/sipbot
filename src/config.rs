use crate::audio_quality::AudioQualityConfig;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;
use tokio::fs;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub addr: Option<String>,
    pub external_ip: Option<String>,
    pub recorders: Option<String>,
    pub accounts: Vec<AccountConfig>,
}

impl Config {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let content = fs::read_to_string(path)
            .await
            .context("Failed to read config file")?;
        let config: Config = toml::from_str(&content).context("Failed to parse config file")?;
        Ok(config)
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct AccountConfig {
    pub username: String,
    pub auth_username: Option<String>,
    pub domain: String,
    pub password: Option<String>,
    pub proxy: Option<String>,
    pub register: Option<bool>,              // Default to true if missing
    #[serde(default)]
    pub from_user: Option<String>,            // Optional From URI user part (for outbound calls without registration)
    pub target: Option<String>,              // Target URI for outbound calls
    pub record: Option<String>,              // Recording file path
    pub srtp_enabled: Option<bool>,          // Enable SRTP/SDES
    pub nack_enabled: Option<bool>,          // Enable NACK
    pub jitter_buffer_enabled: Option<bool>, // Enable Jitter Buffer
    pub reject_prob: Option<u8>,             // Reject probability (1-99%)
    pub codecs: Option<Vec<String>>,         // Preferred codecs (opus, g722, g729, pcmu, pcma)
    pub headers: Option<Vec<String>>,        // Custom SIP headers (e.g., "X-Custom: value")
    #[serde(default)]
    pub cancel_prob: u8, // Cancel probability (1-99%)

    // Stage 1: Early Media (183)
    pub early_media: Option<EarlyMediaConfig>,

    // Stage 2: Ring (Wait with optional Ringing/Ringback)
    pub ring: Option<RingConfig>,

    // Stage 3: Answer (200 OK)
    pub answer: Option<AnswerConfig>,

    // Stage 4: Hangup
    pub hangup: Option<HangupConfig>,

    // REFER handling (for transfer testing)
    pub refer_reject: Option<u16>, // If set, reject REFER with this status code (e.g., 405)

    // Audio quality analysis configuration
    pub audio_quality: Option<AudioQualityConfig>,

    /// DTMF flow after answer: "1s:2,1.5s:#" means send '2' after 1s, then '#' after 1.5s
    pub dtmf_flows: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DtmfFlowEntry {
    pub delay: std::time::Duration,
    pub digit: char,
}

pub fn parse_dtmf_flows(input: &str) -> Result<Vec<DtmfFlowEntry>> {
    let mut entries = Vec::new();
    for part in input.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let Some((delay_str, digit_str)) = part.split_once(':') else {
            anyhow::bail!("Invalid dtmf_flow entry '{}': expected <delay>:<digit>", part);
        };
        let delay_str = delay_str.trim();
        let digit_str = digit_str.trim();
        let delay = if delay_str.ends_with('s') {
            let num: f64 = delay_str[..delay_str.len() - 1]
                .parse()
                .with_context(|| format!("Invalid delay '{}'", delay_str))?;
            std::time::Duration::from_secs_f64(num)
        } else {
            let num: f64 = delay_str
                .parse()
                .with_context(|| format!("Invalid delay '{}'", delay_str))?;
            std::time::Duration::from_secs_f64(num)
        };
        let digit = digit_str
            .chars()
            .next()
            .with_context(|| format!("Missing digit in '{}'", part))?;
        anyhow::ensure!(
            digit.is_ascii_digit() || digit == '*' || digit == '#'
                || ('A'..='D').contains(&digit)
                || ('a'..='d').contains(&digit),
            "Invalid DTMF digit '{}'",
            digit
        );
        entries.push(DtmfFlowEntry { delay, digit });
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dtmf_flows_basic() {
        let entries = parse_dtmf_flows("1s:2,1.5s:#").unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].digit, '2');
        assert_eq!(entries[0].delay, std::time::Duration::from_millis(1000));
        assert_eq!(entries[1].digit, '#');
        assert_eq!(entries[1].delay, std::time::Duration::from_millis(1500));
    }

    #[test]
    fn test_parse_dtmf_flows_no_suffix() {
        let entries = parse_dtmf_flows("0.5:1,2:0").unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].digit, '1');
        assert_eq!(entries[0].delay, std::time::Duration::from_millis(500));
        assert_eq!(entries[1].digit, '0');
        assert_eq!(entries[1].delay, std::time::Duration::from_millis(2000));
    }

    #[test]
    fn test_parse_dtmf_flows_star() {
        let entries = parse_dtmf_flows("1s:*").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].digit, '*');
    }

    #[test]
    fn test_parse_dtmf_flows_empty() {
        let entries = parse_dtmf_flows("").unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_parse_dtmf_flows_invalid_digit() {
        assert!(parse_dtmf_flows("1s:X").is_err());
    }

    #[test]
    fn test_parse_dtmf_flows_missing_colon() {
        assert!(parse_dtmf_flows("1s2").is_err());
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct EarlyMediaConfig {
    pub wav_file: Option<String>,
    pub local: Option<bool>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RingConfig {
    pub duration_secs: u64,
    pub ringback: Option<String>, // Optional wav file for 183, otherwise 180
    pub local: Option<bool>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum AnswerConfig {
    Play { wav_file: String },
    Echo,
    Local,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HangupConfig {
    pub code: u16, // SIP Code (e.g., 603, 486). If 0/200 and answered, send BYE.
    pub after_secs: Option<u64>, // Delay before hanging up
}

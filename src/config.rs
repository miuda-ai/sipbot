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

#[derive(Debug, Deserialize, Clone)]
pub struct AccountConfig {
    pub username: String,
    pub auth_username: Option<String>,
    pub domain: String,
    pub password: Option<String>,
    pub proxy: Option<String>,
    pub register: Option<bool>,     // Default to true if missing
    pub target: Option<String>,     // Target URI for outbound calls
    pub record: Option<String>,     // Recording file path
    pub srtp_enabled: Option<bool>, // Enable SRTP/SDES

    // Stage 1: Early Media (183)
    pub early_media: Option<EarlyMediaConfig>,

    // Stage 2: Ring (Wait with optional Ringing/Ringback)
    pub ring: Option<RingConfig>,

    // Stage 3: Answer (200 OK)
    pub answer: Option<AnswerConfig>,

    // Stage 4: Hangup
    pub hangup: Option<HangupConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EarlyMediaConfig {
    pub wav_file: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RingConfig {
    pub duration_secs: u64,
    pub ringback: Option<String>, // Optional wav file for 183, otherwise 180
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum AnswerConfig {
    Play { wav_file: String },
    Echo,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HangupConfig {
    pub code: u16, // SIP Code (e.g., 603, 486). If 0/200 and answered, send BYE.
    pub after_secs: Option<u64>, // Delay before hanging up
}

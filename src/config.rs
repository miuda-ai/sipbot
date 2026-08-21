use crate::audio_quality::AudioQualityConfig;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;
use tokio::fs;

pub const DEFAULT_TS_JUMP_TOLERANCE_MS: u32 = 50;

fn default_ts_jump_tolerance_ms() -> u32 {
    DEFAULT_TS_JUMP_TOLERANCE_MS
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub addr: Option<String>,
    pub external_ip: Option<String>,
    pub recorders: Option<String>,
    pub ws_url: Option<String>,
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
    pub webrtc_enabled: Option<bool>,        // Enable WebRTC media (ICE+DTLS) + +sip.ice contact
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

    /// RTP timestamp-jump tolerance in milliseconds for the seq/ts jump
    /// (audio-glitch) statistics. Defaults to 50.
    #[serde(default = "default_ts_jump_tolerance_ms")]
    pub ts_jump_tolerance_ms: u32,

    /// DTMF flow after answer: "1s:2,1.5s:#" means send '2' after 1s, then '#' after 1.5s
    pub dtmf_flows: Option<String>,

    /// Re-INVITE flow after answer: "5s:hold,10s:resume" means send hold after 5s, resume after 10s
    pub reinvite_flows: Option<String>,

    /// SIP INFO flow after answer: "3s:application/vnd.rustpbx+json:{\"action\":\"ivr.exec\"};5s:application/dtmf-relay:Signal=5\r\nDuration=100\r\n"
    /// Entries are semicolon-separated. Each entry: <delay>:<content_type>:<body>
    pub info_flows: Option<String>,
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

#[derive(Debug, Clone, PartialEq)]
pub enum ReinviteAction {
    Hold,
    Resume,
}

impl std::str::FromStr for ReinviteAction {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_lowercase().as_str() {
            "hold" => Ok(ReinviteAction::Hold),
            "resume" => Ok(ReinviteAction::Resume),
            _ => anyhow::bail!("Invalid reinvite action '{}': expected 'hold' or 'resume'", s),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReinviteFlowEntry {
    pub delay: std::time::Duration,
    pub action: ReinviteAction,
}

pub fn parse_reinvite_flows(input: &str) -> Result<Vec<ReinviteFlowEntry>> {
    let mut entries = Vec::new();
    for part in input.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let Some((delay_str, action_str)) = part.split_once(':') else {
            anyhow::bail!("Invalid reinvite_flow entry '{}': expected <delay>:<action>", part);
        };
        let delay_str = delay_str.trim();
        let action_str = action_str.trim();
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
        let action: ReinviteAction = action_str.parse()?;
        entries.push(ReinviteFlowEntry { delay, action });
    }
    Ok(entries)
}

#[derive(Debug, Clone)]
pub struct InfoFlowEntry {
    pub delay: std::time::Duration,
    pub content_type: String,
    pub body: String,
}

/// Parse info_flows: "3s:application/json:{\"k\":\"v\"};5s:application/dtmf-relay:Signal=5"
///
/// Entries are semicolon-separated. Each entry format:
///   <delay>:<content_type>:<body>
///
/// The body is everything after the second colon, so it may contain colons,
/// commas, braces, etc. Use `\n` in the body for literal newlines.
pub fn parse_info_flows(input: &str) -> Result<Vec<InfoFlowEntry>> {
    let mut entries = Vec::new();
    for part in input.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        // Find the first colon (delay boundary)
        let Some((delay_str, rest)) = part.split_once(':') else {
            anyhow::bail!("Invalid info_flow entry '{}': expected <delay>:<content_type>:<body>", part);
        };
        // Find the second colon (content_type / body boundary)
        let Some((content_type_str, body_str)) = rest.split_once(':') else {
            anyhow::bail!("Invalid info_flow entry '{}': expected <delay>:<content_type>:<body>", part);
        };
        let delay_str = delay_str.trim();
        let content_type = content_type_str.trim().to_string();
        let body = body_str.replace("\\n", "\n");
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
        anyhow::ensure!(!content_type.is_empty(), "Empty content_type in info_flow '{}'", part);
        entries.push(InfoFlowEntry {
            delay,
            content_type,
            body,
        });
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

    #[test]
    fn test_parse_reinvite_flows_basic() {
        let entries = parse_reinvite_flows("5s:hold,10s:resume").unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].action, ReinviteAction::Hold);
        assert_eq!(entries[0].delay, std::time::Duration::from_millis(5000));
        assert_eq!(entries[1].action, ReinviteAction::Resume);
        assert_eq!(entries[1].delay, std::time::Duration::from_millis(10000));
    }

    #[test]
    fn test_parse_reinvite_flows_no_suffix() {
        let entries = parse_reinvite_flows("2.5:hold,15:resume").unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].delay, std::time::Duration::from_secs_f64(2.5));
        assert_eq!(entries[0].action, ReinviteAction::Hold);
        assert_eq!(entries[1].delay, std::time::Duration::from_secs(15));
        assert_eq!(entries[1].action, ReinviteAction::Resume);
    }

    #[test]
    fn test_parse_reinvite_flows_empty() {
        let entries = parse_reinvite_flows("").unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_parse_reinvite_flows_invalid_action() {
        assert!(parse_reinvite_flows("5s:invalid").is_err());
    }

    #[test]
    fn test_parse_reinvite_flows_missing_colon() {
        assert!(parse_reinvite_flows("5s:hold").is_ok());
        assert!(parse_reinvite_flows("5s").is_err());
    }

    #[test]
    fn test_parse_info_flows_basic() {
        let entries = parse_info_flows(
            "3s:application/vnd.rustpbx+json:{\"action\":\"ivr.exec\"};5s:application/dtmf-relay:Signal=5",
        )
        .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].delay, std::time::Duration::from_millis(3000));
        assert_eq!(entries[0].content_type, "application/vnd.rustpbx+json");
        assert_eq!(entries[0].body, "{\"action\":\"ivr.exec\"}");
        assert_eq!(entries[1].delay, std::time::Duration::from_millis(5000));
        assert_eq!(entries[1].content_type, "application/dtmf-relay");
        assert_eq!(entries[1].body, "Signal=5");
    }

    #[test]
    fn test_parse_info_flows_single() {
        let entries =
            parse_info_flows("0.5:application/json:{\"key\":\"value\"}").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].delay, std::time::Duration::from_millis(500));
        assert_eq!(entries[0].content_type, "application/json");
        assert!(entries[0].body.contains("key"));
    }

    #[test]
    fn test_parse_info_flows_newline_escape() {
        let entries = parse_info_flows("1s:text/plain:line1\\nline2").unwrap();
        assert_eq!(entries[0].body, "line1\nline2");
    }

    #[test]
    fn test_parse_info_flows_empty() {
        assert!(parse_info_flows("").unwrap().is_empty());
    }

    #[test]
    fn test_parse_info_flows_missing_content_type() {
        assert!(parse_info_flows("3s:only_body_no_second_colon").is_err());
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct EarlyMediaConfig {
    pub wav_file: Option<String>,
    pub local: Option<bool>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RingConfig {
    // Optional cap on the ring stage. If omitted with a ringback file/builtin,
    // the call is answered when playback finishes.
    #[serde(default)]
    pub duration_secs: Option<u64>,
    // Optional wav file for 183. Empty string = built-in ringing.wav.
    // None -> 180 Ringing without media.
    pub ringback: Option<String>,
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

use crate::audio_quality::AudioQualityConfig;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;

pub const DEFAULT_TS_JUMP_TOLERANCE_MS: u32 = 50;

fn default_ts_jump_tolerance_ms() -> u32 {
    DEFAULT_TS_JUMP_TOLERANCE_MS
}

/// Account transport type. `udp`/`tcp` bind locally, `ws`/`wss` connect out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TransportKind {
    #[default]
    Udp,
    Tcp,
    Ws,
    Wss,
}

impl TransportKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransportKind::Udp => "udp",
            TransportKind::Tcp => "tcp",
            TransportKind::Ws => "ws",
            TransportKind::Wss => "wss",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "udp" => Some(TransportKind::Udp),
            "tcp" => Some(TransportKind::Tcp),
            "ws" => Some(TransportKind::Ws),
            "wss" => Some(TransportKind::Wss),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Config {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorders: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ws_url: Option<String>,
    /// serve mode: HTTP listen address for the web UI/API (e.g. "0.0.0.0:8080").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_addr: Option<String>,
    /// serve mode: directory for announcement/media wav files (playable+uploadable from the UI).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_dir: Option<String>,
    /// serve mode: directory for persisted call records (JSON per call).
    /// Defaults to "./records" in serve mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records_dir: Option<String>,
    pub accounts: Vec<AccountConfig>,
    /// serve mode: reusable answer strategies; accounts bind by name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub strategies: Vec<StrategyConfig>,
}

impl Config {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())
            .await
            .with_context(|| format!("Failed to read config file {:?}", path.as_ref()))?;
        let config: Config = toml::from_str(&content).context("Failed to parse config file")?;
        Ok(config)
    }

    /// Serialize back to a TOML string (serve mode persistence).
    pub fn to_toml(&self) -> Result<String> {
        toml::to_string_pretty(self).context("Failed to serialize config")
    }

    /// Effective HTTP address for serve mode.
    pub fn http_listen_addr(&self) -> String {
        self.http_addr
            .clone()
            .unwrap_or_else(|| "0.0.0.0:8080".to_string())
    }

    /// Effective media directory for serve mode (fallback: recorders dir, then ./wavs).
    pub fn media_directory(&self) -> String {
        self.media_dir
            .clone()
            .or_else(|| self.recorders.clone())
            .unwrap_or_else(|| "./wavs".to_string())
    }

    /// Effective call-records directory for serve mode (default ./records).
    pub fn records_directory(&self) -> String {
        self.records_dir
            .clone()
            .unwrap_or_else(|| "./records".to_string())
    }

    /// Find a strategy by name.
    pub fn find_strategy(&self, name: &str) -> Option<&StrategyConfig> {
        self.strategies.iter().find(|s| s.name == name)
    }

    /// Resolve the effective strategy for an account: explicit binding by
    /// name, otherwise a synthetic strategy from legacy inline account
    /// fields (backward compatible).
    pub fn strategy_for(&self, account: &AccountConfig) -> StrategyConfig {
        if let Some(name) = &account.strategy {
            if let Some(s) = self.find_strategy(name) {
                return s.clone();
            }
        }
        // Legacy inline fields → synthetic strategy
        StrategyConfig {
            name: account.strategy.clone().unwrap_or_else(|| "inline".to_string()),
            match_caller: account.match_caller.clone(),
            codecs: account.codecs.clone(),
            early_media: account.early_media.clone(),
            ring: account.ring.clone(),
            reject: account.reject.clone(),
            answer: account.answer.clone(),
            announce: account.announce.clone(),
            sdp_jump: account.sdp_jump,
            jump_codecs: account.jump_codecs.clone(),
            dtmf_flows: account.dtmf_flows.clone(),
            reinvite_flows: account.reinvite_flows.clone(),
            info_flows: account.info_flows.clone(),
            hangup: account.hangup.clone(),
        }
    }
}

/// A reusable answer-test strategy (serve mode). Accounts bind to a strategy
/// by name; one strategy can serve many accounts.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct StrategyConfig {
    /// Unique strategy name (binding key).
    pub name: String,
    /// Optional caller match (prefix list separated by `|`): only answer
    /// matching callers. None/empty = match all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_caller: Option<String>,
    /// Codec preference (ordered).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codecs: Option<Vec<String>>,

    // Stage 0: Early Media
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub early_media: Option<EarlyMediaConfig>,

    // Stage 1: Ring (180 / 183 with ringback)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ring: Option<RingConfig>,

    // Stage 1.5: Reject (tone then code; wins over answer)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reject: Option<RejectConfig>,

    // Stage 2: Answer
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<AnswerConfig>,

    // Stage 2.5: Announcement ("XX来电") + in-call stream jump
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub announce: Option<AnnounceConfig>,

    /// 200 OK answer SDP differs from the 183 SDP (new SSRC/codec/ts/seq).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdp_jump: Option<bool>,
    /// Codecs used by the jump session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jump_codecs: Option<Vec<String>>,

    /// DTMF flow after answer: "1s:2,1.5s:#"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dtmf_flows: Option<String>,
    /// Re-INVITE flow after answer: "5s:hold,10s:resume"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reinvite_flows: Option<String>,
    /// SIP INFO flow after answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info_flows: Option<String>,

    // Stage 3: Hangup
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hangup: Option<HangupConfig>,
}

impl StrategyConfig {
    /// Merge this strategy into an account (serve mode): strategy fields
    /// override the account's inline copies. Called when spawning bots so
    /// the call engine keeps reading account-level fields.
    pub fn apply_to(&self, account: &mut AccountConfig) {
        account.match_caller = self.match_caller.clone();
        account.codecs = self.codecs.clone();
        account.early_media = self.early_media.clone();
        account.ring = self.ring.clone();
        account.reject = self.reject.clone();
        account.answer = self.answer.clone();
        account.announce = self.announce.clone();
        account.sdp_jump = self.sdp_jump;
        account.jump_codecs = self.jump_codecs.clone();
        account.dtmf_flows = self.dtmf_flows.clone();
        account.reinvite_flows = self.reinvite_flows.clone();
        account.info_flows = self.info_flows.clone();
        account.hangup = self.hangup.clone();
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct AccountConfig {
    pub username: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_username: Option<String>,
    pub domain: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy: Option<String>,
    pub register: Option<bool>,              // Default to true if missing
    #[serde(default, skip_serializing_if = "Option::is_none")]
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

    // ── serve mode: transport & strategy routing ──
    /// Transport: udp | tcp | ws | wss (default udp)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<String>,
    /// Bind address override for udp/tcp (defaults to global addr)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_addr: Option<String>,
    /// Outbound WS/WSS URL override for ws/wss (defaults to global ws_url)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_ws_url: Option<String>,
    /// Bound strategy name (from `[[strategies]]`). When unset, legacy
    /// inline strategy fields on the account still apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
    /// Optional caller match (prefix list separated by `|`, or regex) — the
    /// strategy only applies to matching callers. Empty/None = match all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_caller: Option<String>,

    // Stage 1: Early Media (183)
    pub early_media: Option<EarlyMediaConfig>,

    // Stage 2: Ring (Wait with optional Ringing/Ringback)
    pub ring: Option<RingConfig>,

    // Stage 1.5: Reject (play tone then respond with code; wins over answer)
    pub reject: Option<RejectConfig>,

    // Stage 3: Answer (200 OK)
    pub answer: Option<AnswerConfig>,

    // Stage 3.5: Announcement ("XX来电") + in-call stream jump
    pub announce: Option<AnnounceConfig>,

    // 200 OK answer SDP differs from the 183 SDP (new SSRC/codec/ts/seq)
    pub sdp_jump: Option<bool>,
    /// Codecs used by the jump session (sdp_jump or announce jump)
    pub jump_codecs: Option<Vec<String>>,

    // Stage 4: Hangup
    pub hangup: Option<HangupConfig>,

    // REFER handling (for transfer testing)
    pub refer_reject: Option<u16>, // If set, reject REFER with this status code (e.g., 405)

    // Audio quality analysis configuration
    pub audio_quality: Option<AudioQualityConfig>,

    /// RTP timestamp-jump tolerance in milliseconds for the seq/ts jump
    /// (audio-glitch) statistics. Defaults to 50.
    #[serde(default = "default_ts_jump_tolerance_ms", skip_serializing_if="is_default_u32")]
    pub ts_jump_tolerance_ms: u32,

    /// DTMF flow after answer: "1s:2,1.5s:#" means send '2' after 1s, then '#' after 1.5s
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dtmf_flows: Option<String>,

    /// Re-INVITE flow after answer: "5s:hold,10s:resume" means send hold after 5s, resume after 10s
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reinvite_flows: Option<String>,

    /// SIP INFO flow after answer: "3s:application/vnd.rustpbx+json:{\"action\":\"ivr.exec\"};5s:application/dtmf-relay:Signal=5\r\nDuration=100\r\n"
    /// Entries are semicolon-separated. Each entry: <delay>:<content_type>:<body>
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info_flows: Option<String>,
}

fn is_default_u32(v: &u32) -> bool {
    *v == DEFAULT_TS_JUMP_TOLERANCE_MS
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
    fn test_parse_dtmf_flows_basic() {        let entries = parse_dtmf_flows("1s:2,1.5s:#").unwrap();
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

    fn sample_toml() -> &'static str {
        r#"
addr = "0.0.0.0:35060"
http_addr = "0.0.0.0:8080"

[[strategies]]
name = "彩铃-跳变"
sdp_jump = true
dtmf_flows = "1s:2"

[strategies.ring]
duration_secs = 3
ringback = "wavs/crbt.wav"

[strategies.hangup]
mode = "remote"

[[strategies]]
name = "拒接"
[strategies.reject]
code = 486
tone = "wavs/busy.wav"

[[accounts]]
username = "1001"
domain = "127.0.0.1"
strategy = "彩铃-跳变"

[[accounts]]
username = "1002"
domain = "127.0.0.1"
# no binding → inline defaults
"#
    }

    #[test]
    fn test_strategy_binding_parse() {
        let config: Config = toml::from_str(sample_toml()).unwrap();
        assert_eq!(config.strategies.len(), 2);
        assert_eq!(config.accounts[0].strategy.as_deref(), Some("彩铃-跳变"));

        let s = config.strategy_for(&config.accounts[0]);
        assert_eq!(s.name, "彩铃-跳变");
        assert!(s.sdp_jump == Some(true));
        assert_eq!(s.ring.as_ref().unwrap().duration_secs, Some(3));

        // unbound account → synthetic inline strategy with defaults
        let s2 = config.strategy_for(&config.accounts[1]);
        assert!(s2.ring.is_none() && s2.sdp_jump.is_none());
    }

    #[test]
    fn test_strategy_apply_to() {
        let config: Config = toml::from_str(sample_toml()).unwrap();
        let mut account = config.accounts[0].clone();
        let s = config.find_strategy("彩铃-跳变").unwrap().clone();
        s.apply_to(&mut account);
        assert_eq!(account.sdp_jump, Some(true));
        assert_eq!(account.dtmf_flows.as_deref(), Some("1s:2"));
        assert_eq!(account.hangup.as_ref().unwrap().mode.as_deref(), Some("remote"));
        assert_eq!(account.ring.as_ref().unwrap().ringback.as_deref(), Some("wavs/crbt.wav"));
    }

    #[test]
    fn test_config_roundtrip_with_strategies() {
        let config: Config = toml::from_str(sample_toml()).unwrap();
        let toml_str = config.to_toml().unwrap();
        let reparsed: Config = toml::from_str(&toml_str).unwrap();
        assert_eq!(reparsed.strategies.len(), 2);
        assert_eq!(reparsed.strategies[0].name, "彩铃-跳变");
        assert_eq!(reparsed.accounts[0].strategy.as_deref(), Some("彩铃-跳变"));
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EarlyMediaConfig {
    pub wav_file: Option<String>,
    pub local: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RingConfig {
    // Optional cap on the ring stage. If omitted with a ringback file/builtin,
    // the call is answered when playback finishes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_secs: Option<u64>,
    // Optional wav file for 183. Empty string = built-in ringing.wav.
    // None -> 180 Ringing without media.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ringback: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum AnswerConfig {
    Play { wav_file: String },
    Echo,
    Local,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RejectConfig {
    /// SIP status code to reject with (e.g. 486, 603).
    pub code: u16,
    /// Optional wav file played as early media (183 with SDP) before rejecting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tone: Option<String>,
    /// Cap on tone playback before rejecting (falls back to full playback).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_secs: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AnnounceConfig {
    /// Wav file announced right after answering ("XX来电" caller announcement).
    /// Use `{{caller}}` to interpolate the caller user part into the file name
    /// (e.g. "wavs/{{caller}}.wav"); falls back to the literal path.
    pub file: String,
    /// After the announcement, jump the outgoing RTP stream: new SSRC +
    /// fresh random sequence/timestamp bases, without any SIP signaling
    /// (dual-MediaSession media handoff).
    #[serde(default)]
    pub jump_after: bool,
    /// Optional codec for the post-jump session (must be offered by the peer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jump_codec: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HangupConfig {
    #[serde(default = "default_hangup_code", skip_serializing_if = "is_default_hangup_code")]
    pub code: u16, // SIP Code (e.g., 603, 486). If 0/200 and answered, send BYE.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_secs: Option<u64>, // Delay before hanging up
    /// hangup mode: "remote" (wait for peer BYE, never send BYE from our side),
    /// "after" (send BYE after after_secs),
    /// "playback" (hang up when media playback finishes — pre-serve behavior).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}

fn default_hangup_code() -> u16 {
    200
}

fn is_default_hangup_code(v: &u16) -> bool {
    *v == 200
}

impl HangupConfig {
    pub fn effective_mode(&self) -> HangupMode {
        match self.mode.as_deref().map(|s| s.trim().to_lowercase()) {
            Some(m) if m == "remote" => HangupMode::Remote,
            Some(m) if m == "playback" => HangupMode::Playback,
            Some(m) if m == "after" => {
                HangupMode::After(self.after_secs.unwrap_or(0).max(1))
            }
            // Legacy semantics: after_secs present => After; none => Playback
            _ => match self.after_secs {
                Some(secs) => HangupMode::After(secs),
                None => HangupMode::Playback,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HangupMode {
    /// Wait for the peer to hang up; never send BYE from our side.
    Remote,
    /// Send BYE after N seconds.
    After(u64),
    /// Hang up when media playback finishes (legacy default).
    Playback,
}

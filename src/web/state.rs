use crate::config::Config;
use crate::stats::CallStats;
use crate::status::StatusBoard;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Max SIP messages kept per call.
pub const MAX_SIP_TRACE: usize = 500;
/// Max calls retained in the registry (active + history).
pub const MAX_CALLS: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CallDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CallState {
    Trying,
    Ringing,
    EarlyMedia,
    Answered,
    Terminated,
    Rejected,
    Failed,
}

/// One SIP message in the per-call trace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SipTraceEntry {
    /// ms since call start.
    pub t_ms: u64,
    /// "in" (received) or "out" (sent).
    pub dir: String,
    /// e.g. "INVITE", "180", "200", "BYE".
    pub summary: String,
    /// remote address (for in) or destination (for out).
    pub peer: Option<String>,
    /// Raw message text.
    pub raw: String,
}

/// A media stream jump (no re-INVITE): new SSRC + fresh seq/ts bases.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JumpEvent {
    /// ms since call start.
    pub t_ms: u64,
    pub reason: String,
    pub old_ssrc: Option<u32>,
    pub new_ssrc: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DtmfEventLog {
    /// ms since call start.
    pub t_ms: u64,
    pub digit: String,
    /// "rx" (received from peer) or "tx" (we sent).
    pub dir: String,
}

/// Everything the UI needs about a single call.
#[derive(Default)]
pub struct CallRecord {
    pub call_id: String,
    pub direction: Option<CallDirection>,
    pub caller: String,
    pub callee: String,
    pub account: String,
    /// Bound strategy name (serve mode) used by the answering bot.
    pub strategy: Option<String>,
    pub state: Option<CallState>,
    pub started_at_ms: u64,
    pub ended_at_ms: Option<u64>,
    pub end_reason: Option<String>,
    pub sip_trace: Vec<SipTraceEntry>,
    pub sdp_offer: Option<String>,
    pub sdp_183: Option<String>,
    pub sdp_200: Option<String>,
    pub codec: Option<String>,
    pub jump_events: Vec<JumpEvent>,
    pub dtmf_events: Vec<DtmfEventLog>,
    pub recording: Option<String>,
    /// Acceptance-check findings for this call (fork duplicates, one-way
    /// media, hangup ownership mismatches, duration deviations...).
    pub issues: Vec<String>,
    /// UI control channel: cancelling it requests a server-side BYE.
    pub control: Option<tokio_util::sync::CancellationToken>,
    /// UI DTMF: digits sent here are transmitted on the current media session.
    pub dtmf_tx: Option<tokio::sync::mpsc::UnboundedSender<char>>,
    /// Per-call live stats (serve mode, in-memory only).
    pub stats: Option<Arc<CallStats>>,
    /// Final stats snapshot (persisted records).
    pub stats_snapshot: Option<serde_json::Value>,
    /// Already flushed to the records dir.
    pub persisted: bool,
}

impl CallRecord {
    pub fn to_json(&self) -> serde_json::Value {
        let elapsed = self
            .ended_at_ms
            .unwrap_or_else(now_ms)
            .saturating_sub(self.started_at_ms);
        serde_json::json!({
            "call_id": self.call_id,
            "direction": self.direction.map(|d| d.as_str()),
            "caller": self.caller,
            "callee": self.callee,
            "account": self.account,
            "strategy": self.strategy,
            "state": self.state.map(state_str),
            "started_at_ms": self.started_at_ms,
            "ended_at_ms": self.ended_at_ms,
            "duration_ms": elapsed,
            "end_reason": self.end_reason,
            "codec": self.codec,
            "recording": self.recording,
            "issues": self.issues.clone(),
            "trace_len": self.sip_trace.len(),
        })
    }

    pub fn to_json_full(&self) -> serde_json::Value {
        let elapsed = self
            .ended_at_ms
            .unwrap_or_else(now_ms)
            .saturating_sub(self.started_at_ms);
        let stats = self
            .stats
            .as_ref()
            .map(|s| s.snapshot_json())
            .or_else(|| self.stats_snapshot.clone());
        serde_json::json!({
            "call_id": self.call_id,
            "direction": self.direction.map(|d| d.as_str()),
            "caller": self.caller,
            "callee": self.callee,
            "account": self.account,
            "strategy": self.strategy,
            "state": self.state.map(state_str),
            "started_at_ms": self.started_at_ms,
            "ended_at_ms": self.ended_at_ms,
            "duration_ms": elapsed,
            "end_reason": self.end_reason,
            "codec": self.codec,
            "recording": self.recording,
            "issues": self.issues.clone(),
            "sip_trace": self.sip_trace,
            "sdp_offer": self.sdp_offer,
            "sdp_183": self.sdp_183,
            "sdp_200": self.sdp_200,
            "jump_events": self.jump_events,
            "dtmf_events": self.dtmf_events,
            "stats": stats,
        })
    }

    /// Reconstruct a record from its persisted JSON (history view; live
    /// stats/control channels are not restored).
    pub fn from_json(v: serde_json::Value) -> Option<CallRecord> {
        let call_id = v.get("call_id")?.as_str()?.to_string();
        Some(CallRecord {
            call_id,
            direction: v.get("direction").and_then(|d| d.as_str()).and_then(|d| match d {
                "inbound" => Some(CallDirection::Inbound),
                "outbound" => Some(CallDirection::Outbound),
                _ => None,
            }),
            caller: v.get("caller").and_then(|x| x.as_str()).unwrap_or("").to_string(),
            callee: v.get("callee").and_then(|x| x.as_str()).unwrap_or("").to_string(),
            account: v.get("account").and_then(|x| x.as_str()).unwrap_or("").to_string(),
            strategy: v.get("strategy").and_then(|x| x.as_str()).map(|s| s.to_string()),
            state: v.get("state").and_then(|s| s.as_str()).and_then(|s| match s {
                "trying" => Some(CallState::Trying),
                "ringing" => Some(CallState::Ringing),
                "early_media" => Some(CallState::EarlyMedia),
                "answered" => Some(CallState::Answered),
                "terminated" => Some(CallState::Terminated),
                "rejected" => Some(CallState::Rejected),
                "failed" => Some(CallState::Failed),
                _ => None,
            }),
            started_at_ms: v.get("started_at_ms").and_then(|x| x.as_u64()).unwrap_or(0),
            ended_at_ms: v.get("ended_at_ms").and_then(|x| x.as_u64()),
            end_reason: v.get("end_reason").and_then(|x| x.as_str()).map(|s| s.to_string()),
            sip_trace: v
                .get("sip_trace")
                .and_then(|x| serde_json::from_value(x.clone()).ok())
                .unwrap_or_default(),
            sdp_offer: v.get("sdp_offer").and_then(|x| x.as_str()).map(|s| s.to_string()),
            sdp_183: v.get("sdp_183").and_then(|x| x.as_str()).map(|s| s.to_string()),
            sdp_200: v.get("sdp_200").and_then(|x| x.as_str()).map(|s| s.to_string()),
            codec: v.get("codec").and_then(|x| x.as_str()).map(|s| s.to_string()),
            jump_events: v
                .get("jump_events")
                .and_then(|x| serde_json::from_value(x.clone()).ok())
                .unwrap_or_default(),
            dtmf_events: v
                .get("dtmf_events")
                .and_then(|x| serde_json::from_value(x.clone()).ok())
                .unwrap_or_default(),
            recording: v.get("recording").and_then(|x| x.as_str()).map(|s| s.to_string()),
            issues: v
                .get("issues")
                .and_then(|x| x.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|i| i.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default(),
            control: None,
            dtmf_tx: None,
            stats: None,
            stats_snapshot: v.get("stats").cloned().filter(|s| !s.is_null()),
            persisted: true,
        })
        .map(|mut r| {
            // started/ended need the raw values; duration_ms was baked into
            // the JSON, so recompute placeholder timestamps for display.
            let duration = v.get("duration_ms").and_then(|d| d.as_u64()).unwrap_or(0);
            let ended = v
                .get("saved_at")
                .and_then(|d| d.as_u64())
                .unwrap_or_else(now_ms);
            r.started_at_ms = ended.saturating_sub(duration);
            if matches!(r.state, Some(CallState::Terminated) | Some(CallState::Rejected)) {
                r.ended_at_ms = Some(ended);
            }
            r
        })
    }
}

impl CallDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            CallDirection::Inbound => "inbound",
            CallDirection::Outbound => "outbound",
        }
    }
}

fn state_str(s: CallState) -> &'static str {
    match s {
        CallState::Trying => "trying",
        CallState::Ringing => "ringing",
        CallState::EarlyMedia => "early_media",
        CallState::Answered => "answered",
        CallState::Terminated => "terminated",
        CallState::Rejected => "rejected",
        CallState::Failed => "failed",
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Live events pushed to WebSocket subscribers.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsEvent {
    SipMessage {
        call_id: String,
        entry: SipTraceEntry,
    },
    CallState {
        call_id: String,
        state: &'static str,
    },
    CallEnded {
        call_id: String,
        reason: Option<String>,
    },
    Dtmf {
        call_id: String,
        entry: DtmfEventLog,
    },
}

/// In-memory store of calls (bounded).
#[derive(Default)]
pub struct CallRegistry {
    calls: Mutex<VecDeque<Arc<Mutex<CallRecord>>>>,
}

impl CallRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create or fetch the record for a call id.
    pub fn get_or_create(&self, call_id: &str) -> Arc<Mutex<CallRecord>> {
        let mut calls = self.calls.lock().unwrap();
        if let Some(existing) = calls.iter().find(|c| c.lock().unwrap().call_id == call_id) {
            return existing.clone();
        }
        let record = Arc::new(Mutex::new(CallRecord {
            call_id: call_id.to_string(),
            started_at_ms: now_ms(),
            ..Default::default()
        }));
        calls.push_front(record.clone());
        // Trim terminated calls beyond the cap.
        while calls.len() > MAX_CALLS {
            if let Some(idx) = calls.iter().rposition(|c| {
                matches!(c.lock().unwrap().state, Some(CallState::Terminated) | Some(CallState::Rejected))
            }) {
                calls.remove(idx);
            } else {
                break;
            }
        }
        record
    }

    pub fn get(&self, call_id: &str) -> Option<Arc<Mutex<CallRecord>>> {
        let calls = self.calls.lock().unwrap();
        calls
            .iter()
            .find(|c| c.lock().unwrap().call_id == call_id)
            .cloned()
    }

    /// Remove one call from the registry and delete its persisted JSON
    /// (plus any recording) from `dir`/`recordings`. Returns whether the
    /// record existed.
    pub fn remove(&self, call_id: &str, records_dir: &std::path::Path, recordings_dir: Option<&std::path::Path>) -> bool {
        let removed = {
            let mut calls = self.calls.lock().unwrap();
            let before = calls.len();
            calls.retain(|c| c.lock().unwrap().call_id != call_id);
            calls.len() != before
        };
        // Delete persisted artifacts matching the sanitized call id.
        let safe: String = call_id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        if safe.len() >= 8 {
            delete_matching_files(records_dir, &safe);
            if let Some(rdir) = recordings_dir {
                delete_matching_files(rdir, &safe);
            }
        }
        removed
    }

    /// Clear every call from the registry and wipe persisted JSONs.
    pub fn clear(&self, records_dir: &std::path::Path, recordings_dir: Option<&std::path::Path>) -> usize {
        let count = {
            let mut calls = self.calls.lock().unwrap();
            let n = calls.len();
            calls.clear();
            n
        };
        delete_matching_files(records_dir, "");
        if let Some(rdir) = recordings_dir {
            delete_matching_files(rdir, "");
        }
        count
    }

    /// Detect duplicate delivery: another inbound record for the same
    /// caller/callee pair that started within `window_ms` of this one
    /// (the registrar forked the INVITE to more than one contact).
    pub fn find_duplicate_inbound(
        &self,
        call_id: &str,
        caller: &str,
        callee_user: &str,
        started_at_ms: u64,
        window_ms: u64,
    ) -> Option<String> {
        let calls = self.calls.lock().unwrap();
        calls.iter().find_map(|c| {
            let r = c.lock().unwrap();
            if r.call_id == call_id || r.direction != Some(CallDirection::Inbound) {
                return None;
            }
            if r.caller != caller {
                return None;
            }
            let this_user = r.callee.split('@').next().unwrap_or("");
            if this_user != callee_user {
                return None;
            }
            if r.started_at_ms.abs_diff(started_at_ms) <= window_ms {
                Some(r.call_id.clone())
            } else {
                None
            }
        })
    }

    /// Summaries, newest first (sorted by started_at_ms, descending), with
    /// optional filters: state group (`incall`/`ended`/`rejected`), account,
    /// strategy.
    pub fn summaries(
        &self,
        limit: usize,
        state_filter: Option<&str>,
        account: Option<&str>,
        strategy: Option<&str>,
    ) -> Vec<serde_json::Value> {
        let calls = self.calls.lock().unwrap();
        let mut items: Vec<(u64, serde_json::Value)> = calls
            .iter()
            .filter_map(|c| {
                let r = c.lock().unwrap();
                if let Some(sf) = state_filter {
                    let s = r.state.map(state_str).unwrap_or_default();
                    let group: &str = match s {
                        "trying" | "ringing" | "early_media" | "answered" => "incall",
                        "rejected" => "rejected",
                        "terminated" | "failed" => "ended",
                        _ => "incall",
                    };
                    if group != sf {
                        return None;
                    }
                }
                if let Some(acc) = account {
                    if !acc.is_empty() && r.account != acc {
                        return None;
                    }
                }
                if let Some(st) = strategy {
                    if !st.is_empty() && r.strategy.as_deref() != Some(st) {
                        return None;
                    }
                }
                Some((r.started_at_ms, r.to_json()))
            })
            .collect();
        items.sort_by(|a, b| b.0.cmp(&a.0));
        items.into_iter().take(limit).map(|(_, v)| v).collect()
    }

    pub fn active_count(&self) -> usize {
        let calls = self.calls.lock().unwrap();
        calls
            .iter()
            .filter(|c| !matches!(c.lock().unwrap().state, Some(CallState::Terminated) | Some(CallState::Rejected)))
            .count()
    }

    /// Flush a terminated call to the records dir (JSON per call).
    /// Returns the file path when a write was scheduled.
    pub fn persist_call(&self, call_id: &str, dir: &std::path::Path) -> Option<PathBuf> {
        let record = self.get(call_id)?;
        let (json, filename) = {
            let mut r = record.lock().unwrap();
            if r.persisted {
                return None;
            }
            if !matches!(
                r.state,
                Some(CallState::Terminated) | Some(CallState::Rejected) | Some(CallState::Failed)
            ) {
                return None;
            }
            r.persisted = true;
            let json = r.to_json_full();
            let safe: String = r
                .call_id
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect();
            let filename = format!("{}_{}.json", r.started_at_ms, safe);
            (json, filename)
        };
        let path = dir.join(filename);
        let write_path = path.clone();
        tokio::spawn(async move {
            if let Some(parent) = write_path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            if let Ok(text) = serde_json::to_string_pretty(&json) {
                if let Err(e) = tokio::fs::write(&write_path, text).await {
                    tracing::warn!("persist call record {:?}: {}", write_path, e);
                }
            }
        });
        Some(path)
    }

    /// Load persisted call history at startup (bounded to `limit` newest).
    pub fn load_from_dir(&self, dir: &std::path::Path, limit: usize) -> usize {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        let mut files: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
            .collect();
        // Filenames start with started_at_ms — name sort == time sort.
        files.sort();
        files.reverse(); // newest first
        let mut loaded = 0;
        for path in files.into_iter().take(limit) {
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) else {
                continue;
            };
            let Some(record) = CallRecord::from_json(json) else {
                continue;
            };
            let mut calls = self.calls.lock().unwrap();
            if calls.iter().any(|c| c.lock().unwrap().call_id == record.call_id) {
                continue;
            }
            calls.push_back(Arc::new(Mutex::new(record)));
            loaded += 1;
        }
        loaded
    }
}

/// Shared serve-mode state.
pub struct ServeState {
    pub config: RwLock<Config>,
    pub config_path: PathBuf,
    pub status_board: Arc<StatusBoard>,
    pub calls: Arc<CallRegistry>,
    pub events: broadcast::Sender<WsEvent>,
    pub cancel_token: CancellationToken,
    /// Live outbound test calls: cancel token + identity so the UI hangup
    /// endpoint can CANCEL a pending INVITE or hangup an answered call.
    pub outbound_calls: Mutex<Vec<OutboundCallHandle>>,
    /// Live account bots (for hot reload).
    pub bots: Mutex<Vec<super::BotHandle>>,
}

/// One running outbound test call (ephemeral caller).
pub struct OutboundCallHandle {
    pub cancel: CancellationToken,
    pub from_user: String,
    pub target: String,
    pub started_at_ms: u64,
}

impl ServeState {
    pub fn new(config: Config, cancel_token: CancellationToken) -> Self {
        let (events, _) = broadcast::channel(1024);
        Self {
            config: RwLock::new(config),
            config_path: default_config_path(),
            status_board: Arc::new(StatusBoard::new()),
            calls: Arc::new(CallRegistry::new()),
            events,
            cancel_token,
            outbound_calls: Mutex::new(Vec::new()),
            bots: Mutex::new(Vec::new()),
        }
    }

    pub fn with_config_path(mut self, path: PathBuf) -> Self {
        self.config_path = path;
        self
    }

    pub fn current_config(&self) -> Config {
        self.config.read().unwrap().clone()
    }

    pub fn update_config(&self, config: Config) {
        *self.config.write().unwrap() = config;
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WsEvent> {
        self.events.subscribe()
    }

    /// Persist the current config to disk (serve mode strategy persistence).
    pub fn persist_config(&self) -> Result<(), String> {
        let config = self.current_config();
        let toml = config.to_toml().map_err(|e| e.to_string())?;
        std::fs::write(&self.config_path, toml)
            .map_err(|e| format!("write {:?}: {}", self.config_path, e))
    }
}

fn default_config_path() -> PathBuf {
    if let Some(home) = std::env::home_dir() {
        return home.join(".sipbot.toml");
    }
    PathBuf::from(".sipbot.toml")
}

/// Delete files in `dir` whose names contain `needle` (all files when the
/// needle is empty). Returns the number of files removed.
fn delete_matching_files(dir: &std::path::Path, needle: &str) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if needle.is_empty() || name.contains(needle) {
            if std::fs::remove_file(&path).is_ok() {
                removed += 1;
            }
        }
    }
    removed
}

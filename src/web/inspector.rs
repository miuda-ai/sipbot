use super::state::{
    now_ms, CallDirection, CallState, CallRegistry, SipTraceEntry, WsEvent, MAX_SIP_TRACE,
};
use rsipstack::rsip::message::HeadersExt;
use rsipstack::rsip::{Method, SipMessage};
use rsipstack::sip::HasHeaders;
use rsipstack::transaction::endpoint::MessageInspector;
use rsipstack::transport::SipAddr;
use std::sync::Arc;
use tokio::sync::broadcast;

/// MessageInspector implementation that records every in/out SIP message into
/// the per-call trace and broadcasts it to WebSocket subscribers.
pub struct TraceInspector {
    pub calls: Arc<CallRegistry>,
    pub events: broadcast::Sender<WsEvent>,
    /// Call-records persistence directory (serve mode).
    pub records_dir: Option<std::path::PathBuf>,
}

impl TraceInspector {
    pub fn new(
        calls: Arc<CallRegistry>,
        events: broadcast::Sender<WsEvent>,
        records_dir: Option<std::path::PathBuf>,
    ) -> Box<Self> {
        Box::new(Self {
            calls,
            events,
            records_dir,
        })
    }

    fn record(&self, dir: &str, msg: &SipMessage, peer: Option<String>) {
        let Ok(call_id_header) = msg.call_id_header() else {
            return;
        };
        let call_id = call_id_header.value().to_string();
        let summary = match msg {
            SipMessage::Request(r) => r.method.to_string(),
            SipMessage::Response(r) => r.status_code.to_string(),
        };

        let record = self.calls.get_or_create(&call_id);
        let (t_ms, state_event, ended) = {
            let mut r = record.lock().unwrap();
            let t_ms = now_ms().saturating_sub(r.started_at_ms);

            fill_meta(&mut r, dir, msg);
            capture_sdp(&mut r, dir, &summary, msg);
            let (state_event, ended) = apply_state(&mut r, dir, &summary, msg.is_request());

            r.sip_trace.push(SipTraceEntry {
                t_ms,
                dir: dir.to_string(),
                summary: summary.clone(),
                peer: peer.clone(),
                raw: msg.to_string(),
            });
            if r.sip_trace.len() > MAX_SIP_TRACE {
                r.sip_trace.remove(0);
            }
            (t_ms, state_event, ended)
        };

        let _ = self.events.send(WsEvent::SipMessage {
            call_id: call_id.clone(),
            entry: SipTraceEntry {
                t_ms,
                dir: dir.to_string(),
                summary,
                peer,
                raw: String::new(), // full raw kept in the record; WS pushes summary only
            },
        });
        if let Some(state) = state_event {
            let _ = self.events.send(WsEvent::CallState {
                call_id: call_id.clone(),
                state,
            });
        }
        if ended {
            let _ = self.events.send(WsEvent::CallEnded {
                call_id: call_id.clone(),
                reason: None,
            });
            // Persist the finished call (delayed so final media stats land).
            if let Some(dir) = &self.records_dir {
                let calls = self.calls.clone();
                let dir = dir.clone();
                let persist_id = call_id.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
                    calls.persist_call(&persist_id, &dir);
                });
            }
        }
    }
}

impl MessageInspector for TraceInspector {
    fn before_send(&self, msg: SipMessage, dest: Option<&SipAddr>) -> SipMessage {
        self.record("out", &msg, dest.map(|d| d.to_string()));
        msg
    }

    fn after_received(&self, msg: SipMessage, from: Option<&SipAddr>) -> SipMessage {
        self.record("in", &msg, from.map(|d| d.to_string()));
        msg
    }
}

fn uri_of(msg: &SipMessage, from: bool) -> Option<String> {
    let uri = if from {
        msg.from_header().ok()?.uri().ok()?.clone()
    } else {
        msg.to_header().ok()?.uri().ok()?.clone()
    };
    Some(uri.to_string())
}

fn fill_meta(r: &mut super::state::CallRecord, dir: &str, msg: &SipMessage) {
    if r.caller.is_empty() {
        r.caller = uri_of(msg, true).unwrap_or_default();
    }
    if r.callee.is_empty() {
        r.callee = uri_of(msg, false).unwrap_or_default();
    }
    if r.direction.is_none() {
        if let SipMessage::Request(req) = msg {
            if req.method == Method::Invite {
                r.direction = Some(if dir == "in" {
                    CallDirection::Inbound
                } else {
                    CallDirection::Outbound
                });
                if dir == "in" {
                    if let Some(user) = req.uri.user() {
                        r.callee = user.to_string();
                    }
                }
            }
        }
    }
}

fn capture_sdp(r: &mut super::state::CallRecord, _dir: &str, summary: &str, msg: &SipMessage) {
    let content_type = msg.headers().iter().find_map(|h| {
        if let rsipstack::rsip::Header::ContentType(ct) = h {
            Some(ct.0.to_string())
        } else {
            None
        }
    });
    if content_type.as_deref() != Some("application/sdp") {
        return;
    }
    let body = match msg {
        SipMessage::Request(req) => String::from_utf8_lossy(req.body()).to_string(),
        SipMessage::Response(resp) => String::from_utf8_lossy(resp.body()).to_string(),
    };
    if body.is_empty() {
        return;
    }
    // First token of the summary is the method ("INVITE") or the numeric code ("200").
    let kind = summary.split(' ').next().unwrap_or("");
    match kind {
        "INVITE" if r.sdp_offer.is_none() => r.sdp_offer = Some(body),
        "183" => r.sdp_183 = Some(body),
        "200" if r.sdp_200.is_none() => r.sdp_200 = Some(body),
        _ => {}
    }
}

/// Returns (state_changed_to, call_ended)
fn apply_state(
    r: &mut super::state::CallRecord,
    dir: &str,
    summary: &str,
    is_request: bool,
) -> (Option<&'static str>, bool) {
    let Some(new_state) = derive_state(dir, summary, is_request) else {
        return (None, false);
    };
    let terminal = matches!(
        new_state,
        CallState::Terminated | CallState::Rejected | CallState::Failed
    );
    let already_past = matches!(
        r.state,
        Some(CallState::Terminated) | Some(CallState::Rejected)
    );
    if r.state == Some(new_state) || (already_past && !terminal) {
        return (None, false);
    }
    if !terminal
        && matches!(
            r.state,
            Some(CallState::Answered) | Some(CallState::EarlyMedia)
        )
        && new_state == CallState::Ringing
    {
        // Never downgrade from answered/early-media back to ringing.
        return (None, false);
    }
    r.state = Some(new_state);
    let state_event = Some(state_name(new_state));
    let ended = if terminal {
        r.ended_at_ms = Some(now_ms());
        if r.end_reason.is_none() {
            r.end_reason = Some(summary.to_string());
        }
        true
    } else {
        false
    };
    (state_event, ended)
}

fn state_name(s: CallState) -> &'static str {
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

fn derive_state(dir: &str, summary: &str, is_request: bool) -> Option<CallState> {
    if is_request {
        return match summary {
            "INVITE" if dir == "in" => Some(CallState::Trying),
            "BYE" | "CANCEL" => Some(CallState::Terminated),
            _ => None,
        };
    }
    let code: u16 = summary.split(' ').next()?.parse().ok()?;
    match code {
        100 | 180 => Some(CallState::Ringing),
        183 => Some(CallState::EarlyMedia),
        200..=299 => Some(CallState::Answered),
        404 | 403 | 480 | 486 | 603 => Some(CallState::Rejected),
        400..=699 => Some(CallState::Failed),
        _ => None,
    }
}

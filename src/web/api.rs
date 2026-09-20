use super::state::ServeState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use std::sync::Arc;

pub fn router(state: Arc<ServeState>) -> Router {
    Router::new()
        .route("/", get(ui_index))
        .route("/app.js", get(ui_app_js))
        .route("/style.css", get(ui_style_css))
        .route("/api/config", get(get_config).put(put_config))
        .route("/api/accounts", get(get_accounts))
        .route("/api/accounts/copy", post(post_account_copy))
        .route("/api/accounts/bind", post(post_account_bind))
        .route("/api/strategies", get(get_strategies))
        .route("/api/strategies/copy", post(post_strategy_copy))
        .route("/api/strategies/{name}", axum::routing::delete(delete_strategy))
        .route("/api/calls", get(get_calls).post(post_call_outbound))
        .route("/api/calls/{call_id}", get(get_call_detail))
        .route("/api/calls/{call_id}/hangup", post(post_call_hangup))
        .route("/api/calls/{call_id}/dtmf", post(post_call_dtmf))
        .route("/api/recordings", get(get_recordings))
        .route("/recordings/{file}", get(get_recording_file))
        .route("/api/media", get(super::media::api_list))
        .route(
            "/api/media/{file}",
            axum::routing::post(super::media::upload).delete(super::media::delete),
        )
        .route("/media/{file}", get(super::media::serve_file))
        .route("/ws", get(super::ws::ws_handler))
        .fallback(fallback_not_found)
        .with_state(state)
}

async fn ui_index() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        super::ui::INDEX_HTML,
    )
}

async fn ui_app_js() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/javascript; charset=utf-8")],
        super::ui::APP_JS,
    )
}

async fn ui_style_css() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/css; charset=utf-8")],
        super::ui::STYLE_CSS,
    )
}

async fn get_config(State(state): State<Arc<ServeState>>) -> impl IntoResponse {
    Json(state.current_config())
}

/// PUT /api/config — persist to toml and hot-reload the account bots.
async fn put_config(
    State(state): State<Arc<ServeState>>,
    Json(config): Json<crate::config::Config>,
) -> axum::response::Response {
    commit_config(&state, config).await;
    Json(serde_json::json!({ "ok": true, "reloading": true })).into_response()
}

/// POST /api/calls — start an outbound test call (ephemeral caller bot).
async fn post_call_outbound(
    State(state): State<Arc<ServeState>>,
    Json(body): Json<serde_json::Value>,
) -> axum::response::Response {
    let target = body
        .get("target")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if target.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "missing target" })),
        )
            .into_response();
    }
    let from_user = body
        .get("from_user")
        .and_then(|v| v.as_str())
        .unwrap_or("caller")
        .to_string();
    let action = body
        .get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("play")
        .to_string();
    let wav_file = body
        .get("wav_file")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let hangup_secs = body.get("hangup_secs").and_then(|v| v.as_u64());
    let total = body.get("total").and_then(|v| v.as_u64()).unwrap_or(1).max(1) as u32;
    let cps = body.get("cps").and_then(|v| v.as_u64()).unwrap_or(1).max(1) as u32;
    let dtmf_flows = body
        .get("dtmf_flows")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    // Optional SIP proxy + credentials so the ephemeral caller can traverse
    // a SIP server (e.g. rustpbx) instead of INVITEing the domain directly.
    let proxy = body
        .get("proxy")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let auth_user = body
        .get("auth_user")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let password = body
        .get("password")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Parse target host to use as domain.
    let target_stripped = target.trim_start_matches("sip:");
    let domain = target_stripped
        .split(['@', ';'])
        .nth(1)
        .unwrap_or("127.0.0.1")
        .to_string();

    let answer_config = match (action.as_str(), wav_file) {
        ("echo", _) => Some(crate::config::AnswerConfig::Echo),
        ("play", Some(wav)) => Some(crate::config::AnswerConfig::Play { wav_file: wav }),
        ("play", None) => Some(crate::config::AnswerConfig::Play {
            wav_file: "wavs/play.wav".to_string(),
        }),
        _ => None,
    };

    let account = crate::config::AccountConfig {
        username: from_user.clone(),
        auth_username: auth_user,
        domain: domain.clone(),
        password,
        proxy,
        target: Some(target.clone()),
        answer: answer_config,
        hangup: hangup_secs.map(|secs| crate::config::HangupConfig {
            code: 0,
            after_secs: Some(secs),
            mode: None,
        }),
        dtmf_flows,
        codecs: Some(vec!["pcmu".to_string(), "pcma".to_string(), "g722".to_string()]),
        ..Default::default()
    };

    let global_config = crate::config::Config {
        // Ephemeral caller port; keeps the serve endpoints free.
        addr: Some("0.0.0.0:0".to_string()),
        external_ip: state.current_config().external_ip,
        recorders: state.current_config().recorders,
        ..Default::default()
    };

    let mut bot = crate::sip::SipBot::new(
        account,
        global_config,
        std::sync::Arc::new(crate::stats::CallStats::new()),
        true,
        state.cancel_token.child_token(),
    );
    bot.trace_inspector = Some(super::inspector::TraceInspector::new(
        state.calls.clone(),
        state.events.clone(),
        Some(std::path::PathBuf::from(
            state.current_config().records_directory(),
        )),
    ));
    bot.call_registry = Some(state.calls.clone());
    bot.ws_events = Some(state.events.clone());

    tokio::spawn(async move {
        if let Err(e) = bot.run_call(total, cps).await {
            tracing::error!("outbound test call error: {:?}", e);
        }
    });

    Json(
        serde_json::json!({
            "ok": true,
            "from": from_user,
            "target": target,
            "action": action,
            "total": total,
            "cps": cps,
        }),
    )
        .into_response()
}

async fn get_accounts(State(state): State<Arc<ServeState>>) -> impl IntoResponse {
    let config = state.current_config();
    let statuses = state.status_board.snapshot();
    let accounts: Vec<serde_json::Value> = config
        .accounts
        .iter()
        .map(|a| {
            let st = statuses
                .iter()
                .find(|s| s.username == a.username && s.domain == a.domain);
            let strategy = config.strategy_for(a);
            serde_json::json!({
                "username": a.username,
                "domain": a.domain,
                "register": a.register.unwrap_or(false),
                "transport": a.transport.clone().unwrap_or_else(|| "udp".to_string()),
                "transport_addr": a.transport_addr,
                "transport_ws_url": a.transport_ws_url,
                "strategy": a.strategy,
                "strategy_inline": a.strategy.is_none() && (
                    a.ring.is_some() || a.answer.is_some() || a.reject.is_some()
                        || a.announce.is_some() || a.sdp_jump == Some(true)
                ),
                "summary": strategy_summary(&strategy),
                "registration": st,
            })
        })
        .collect();
    Json(serde_json::json!({ "accounts": accounts }))
}

fn strategy_summary(s: &crate::config::StrategyConfig) -> String {
    let mut parts: Vec<String> = vec![];
    if let Some(r) = &s.reject {
        parts.push(format!("reject {}", r.code));
    }
    if let Some(r) = &s.ring {
        let mode = match &r.ringback {
            None => "180".to_string(),
            Some(b) if b.is_empty() => "183+ringback(builtin)".to_string(),
            Some(_) => "183+ringback".to_string(),
        };
        parts.push(format!("{} ring {}s", mode, r.duration_secs.unwrap_or(0)));
    }
    if let Some(a) = &s.announce {
        parts.push(format!(
            "announce{}",
            if a.jump_after { "+jump" } else { "" }
        ));
    }
    if s.sdp_jump == Some(true) {
        parts.push("SDP jump".to_string());
    }
    if let Some(a) = &s.answer {
        parts.push(match a {
            crate::config::AnswerConfig::Echo => "echo".to_string(),
            crate::config::AnswerConfig::Play { .. } => "play".to_string(),
            crate::config::AnswerConfig::Local => "local".to_string(),
        });
    }
    if let Some(d) = &s.dtmf_flows {
        parts.push(format!("DTMF {}", d));
    }
    if let Some(h) = &s.hangup {
        parts.push(match h.effective_mode() {
            crate::config::HangupMode::Remote => "wait remote BYE".to_string(),
            crate::config::HangupMode::After(secs) => format!("BYE after {}s", secs),
            crate::config::HangupMode::Playback => "BYE after playback".to_string(),
        });
    }
    if parts.is_empty() {
        "default answer".to_string()
    } else {
        parts.join(" · ")
    }
}

async fn get_strategies(State(state): State<Arc<ServeState>>) -> impl IntoResponse {
    let config = state.current_config();
    let strategies: Vec<serde_json::Value> = config
        .strategies
        .iter()
        .map(|s| {
            let bound: Vec<String> = config
                .accounts
                .iter()
                .filter(|a| a.strategy.as_deref() == Some(s.name.as_str()))
                .map(|a| a.username.clone())
                .collect();
            serde_json::json!({
                "name": s.name,
                "summary": strategy_summary(s),
                "match_caller": s.match_caller,
                "codecs": s.codecs,
                "bound_accounts": bound,
                "strategy": s,
            })
        })
        .collect();
    Json(serde_json::json!({ "strategies": strategies }))
}

/// POST /api/strategies/copy {source, new_name?}
async fn post_strategy_copy(
    State(state): State<Arc<ServeState>>,
    Json(body): Json<serde_json::Value>,
) -> axum::response::Response {
    let Some(source) = body.get("source").and_then(|v| v.as_str()) else {
        return bad_request("missing source");
    };
    let mut config = state.current_config();
    let Some(src) = config.find_strategy(source).cloned() else {
        return not_found("strategy not found");
    };
    let new_name = body
        .get("new_name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| unique_strategy_name(&config, source));
    let mut copy = src.clone();
    copy.name = new_name.clone();
    config.strategies.push(copy);
    commit_config(&state, config).await;
    Json(serde_json::json!({ "ok": true, "name": new_name })).into_response()
}

/// DELETE /api/strategies/{name}
async fn delete_strategy(
    State(state): State<Arc<ServeState>>,
    Path(name): Path<String>,
) -> axum::response::Response {
    let mut config = state.current_config();
    let before = config.strategies.len();
    config.strategies.retain(|s| s.name != name);
    if config.strategies.len() == before {
        return not_found("strategy not found");
    }
    // Unbind accounts referencing the deleted strategy.
    for a in &mut config.accounts {
        if a.strategy.as_deref() == Some(name.as_str()) {
            a.strategy = None;
        }
    }
    commit_config(&state, config).await;
    Json(serde_json::json!({ "ok": true })).into_response()
}

/// POST /api/accounts/copy {username, domain, new_username?}
async fn post_account_copy(
    State(state): State<Arc<ServeState>>,
    Json(body): Json<serde_json::Value>,
) -> axum::response::Response {
    let username = body.get("username").and_then(|v| v.as_str()).unwrap_or("");
    let domain = body.get("domain").and_then(|v| v.as_str()).unwrap_or("");
    let mut config = state.current_config();
    let Some(src) = config
        .accounts
        .iter()
        .find(|a| a.username == username && a.domain == domain)
        .cloned()
    else {
        return not_found("account not found");
    };
    let new_username = body
        .get("new_username")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| unique_username(&config, username));
    let mut copy = src.clone();
    copy.username = new_username.clone();
    config.accounts.push(copy);
    commit_config(&state, config).await;
    Json(
        serde_json::json!({
            "ok": true,
            "username": new_username,
            "strategy": body.get("strategy").and_then(|v| v.as_str()),
        }),
    )
    .into_response()
}

/// POST /api/accounts/bind {username, domain, strategy}
async fn post_account_bind(
    State(state): State<Arc<ServeState>>,
    Json(body): Json<serde_json::Value>,
) -> axum::response::Response {
    let username = body.get("username").and_then(|v| v.as_str()).unwrap_or("");
    let domain = body.get("domain").and_then(|v| v.as_str()).unwrap_or("");
    let strategy = body.get("strategy").and_then(|v| v.as_str());
    let mut config = state.current_config();
    if let Some(name) = strategy {
        if !name.is_empty() && config.find_strategy(name).is_none() {
            return not_found("strategy not found");
        }
    }
    let Some(account) = config
        .accounts
        .iter_mut()
        .find(|a| a.username == username && a.domain == domain)
    else {
        return not_found("account not found");
    };
    account.strategy = match strategy {
        Some("") | None => None,
        Some(name) => Some(name.to_string()),
    };
    let bound = account.strategy.clone();
    commit_config(&state, config).await;
    Json(serde_json::json!({ "ok": true, "strategy": bound })).into_response()
}

fn unique_strategy_name(config: &crate::config::Config, source: &str) -> String {
    for i in 1..100 {
        let candidate = if i == 1 {
            format!("{}-copy", source)
        } else {
            format!("{}-copy{}", source, i)
        };
        if config.find_strategy(&candidate).is_none() {
            return candidate;
        }
    }
    format!("{}-copy{}", source, chrono::Utc::now().timestamp())
}

fn unique_username(config: &crate::config::Config, source: &str) -> String {
    let taken = |u: &str| {
        config
            .accounts
            .iter()
            .any(|a| a.username == u)
    };
    if source.chars().all(|c| c.is_ascii_digit()) && !source.is_empty() {
        // numeric: find next free integer
        let base: u64 = source.parse().unwrap_or(0);
        for i in 1..1000 {
            let candidate = (base + i).to_string();
            if !taken(&candidate) {
                return candidate;
            }
        }
    }
    for i in 1..100 {
        let candidate = if i == 1 {
            format!("{}-copy", source)
        } else {
            format!("{}-copy{}", source, i)
        };
        if !taken(&candidate) {
            return candidate;
        }
    }
    format!("{}-copy-{}", source, chrono::Utc::now().timestamp())
}

/// Persist config and hot-reload bots (shared by config-mutating endpoints).
async fn commit_config(state: &Arc<ServeState>, config: crate::config::Config) {
    state.update_config(config);
    if let Err(e) = state.persist_config() {
        tracing::error!("persist config: {}", e);
    }
    let st = state.clone();
    tokio::spawn(async move {
        super::reload_bots(&st, true).await;
    });
}

fn bad_request(msg: &str) -> axum::response::Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": msg })),
    )
        .into_response()
}

fn not_found(msg: &str) -> axum::response::Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": msg })),
    )
        .into_response()
}


async fn get_calls(
    State(state): State<Arc<ServeState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let sf = params
        .get("state")
        .map(|s| s.trim().to_lowercase())
        .filter(|s| matches!(s.as_str(), "incall" | "ended" | "rejected"));
    let account = params.get("account").map(|s| s.trim().to_string());
    let strategy = params.get("strategy").map(|s| s.trim().to_string());
    Json(serde_json::json!({
        "active": state.calls.active_count(),
        "calls": state.calls.summaries(
            200,
            sf.as_deref(),
            account.as_deref(),
            strategy.as_deref(),
        ),
    }))
}

async fn get_call_detail(
    State(state): State<Arc<ServeState>>,
    Path(call_id): Path<String>,
) -> axum::response::Response {
    match state.calls.get(&call_id) {
        Some(record) => {
            let json = record.lock().unwrap().to_json_full();
            Json(json).into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "call not found" })),
        )
            .into_response(),
    }
}

async fn post_call_hangup(
    State(state): State<Arc<ServeState>>,
    Path(call_id): Path<String>,
) -> axum::response::Response {
    match state.calls.get(&call_id) {
        Some(record) => {
            let token = record.lock().unwrap().control.clone();
            match token {
                Some(token) if !token.is_cancelled() => {
                    token.cancel();
                    Json(serde_json::json!({ "ok": true })).into_response()
                }
                _ => (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "call not controllable" })),
                )
                    .into_response(),
            }
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "call not found" })),
        )
            .into_response(),
    }
}

async fn post_call_dtmf(
    State(state): State<Arc<ServeState>>,
    Path(call_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> axum::response::Response {
    let digit = body
        .get("digit")
        .and_then(|d| d.as_str())
        .and_then(|s| s.chars().next());
    let Some(digit) = digit else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "missing digit" })),
        )
            .into_response();
    };
    let Some(record) = state.calls.get(&call_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "call not found" })),
        )
            .into_response();
    };
    let tx = record.lock().unwrap().dtmf_tx.clone();
    match tx {
        Some(tx) => match tx.send(digit) {
            Ok(_) => Json(serde_json::json!({ "ok": true, "digit": digit })).into_response(),
            Err(e) => (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": format!("call ended: {}", e) })),
            )
                .into_response(),
        },
        None => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "call not controllable" })),
        )
            .into_response(),
    }
}

async fn get_recordings(State(state): State<Arc<ServeState>>) -> impl IntoResponse {
    let dir = state.current_config().recorders;
    let Some(dir) = dir else {
        return Json(serde_json::json!({ "recordings": [] }));
    };
    let mut list = vec![];
    if let Ok(mut entries) = tokio::fs::read_dir(&dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("wav") {
                let meta = entry.metadata().await.ok();
                list.push(serde_json::json!({
                    "name": entry.file_name().to_string_lossy(),
                    "size": meta.as_ref().map(|m| m.len()),
                    "modified": meta.and_then(|m| m.modified().ok())
                        .map(|t| t.duration_since(std::time::UNIX_EPOCH).ok()
                            .map(|d| d.as_secs())),
                }));
            }
        }
    }
    list.sort_by(|a, b| {
        b.get("modified")
            .and_then(|v| v.as_u64())
            .cmp(&a.get("modified").and_then(|v| v.as_u64()))
    });
    Json(serde_json::json!({ "recordings": list }))
}

async fn get_recording_file(
    State(state): State<Arc<ServeState>>,
    Path(file): Path<String>,
) -> axum::response::Response {
    // Prevent path traversal.
    if file.contains("..") || file.contains('/') || file.contains('\\') {
        return (StatusCode::BAD_REQUEST, "invalid file name").into_response();
    }
    let Some(dir) = state.current_config().recorders else {
        return (StatusCode::NOT_FOUND, "no recorders dir").into_response();
    };
    let path = std::path::Path::new(&dir).join(&file);
    match tokio::fs::read(&path).await {
        Ok(bytes) => (
            [
                (axum::http::header::CONTENT_TYPE, "audio/wav"),
                (
                    axum::http::header::CONTENT_DISPOSITION,
                    "inline",
                ),
            ],
            bytes,
        )
            .into_response(),
        Err(_) => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}


async fn fallback_not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "not found")
}

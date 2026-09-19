pub mod api;
pub mod inspector;
pub mod media;
pub mod state;
pub mod ui;
pub mod ws;

use crate::config::Config;
use crate::sip::SipBot;
use crate::stats::CallStats;
use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// A spawned account bot.
pub struct BotHandle {
    pub key: String,
    pub token: CancellationToken,
    pub abort: tokio::task::AbortHandle,
}

/// Entry point of `sipbot serve`: spawn one SipBot per account and start the
/// web UI / API server.
pub async fn run_serve(
    config: Config,
    config_path: std::path::PathBuf,
    verbose: bool,
    cancel_token: CancellationToken,
) -> Result<()> {
    let state = state::ServeState::new(config.clone(), cancel_token.clone())
        .with_config_path(config_path);
    let state = Arc::new(state);

    // Load persisted call history (survives restarts).
    let records_dir = std::path::PathBuf::from(config.records_directory());
    let loaded = state.calls.load_from_dir(&records_dir, 1000);
    if loaded > 0 {
        info!("[records] loaded {} persisted call records", loaded);
    }

    spawn_bots(&state, verbose);

    // HTTP server (blocks)
    let app = api::router(state.clone());
    let addr = config.http_listen_addr();
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("Web UI listening on http://{}", addr);
    println!("[*] Answer Test Web UI: http://{}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

/// Spawn one bot per configured account.
pub fn spawn_bots(state: &Arc<state::ServeState>, verbose: bool) {
    let config = state.current_config();
    let mut used_ports: std::collections::HashSet<u16> = std::collections::HashSet::new();
    for account in &config.accounts {
        let key = format!("{}@{}", account.username, account.domain);
        let token = CancellationToken::new();
        let run_token = token.clone();
        // Resolve + merge the bound strategy into the account so the call
        // engine keeps reading account-level fields.
        let mut account = account.clone();
        config.strategy_for(&account).apply_to(&mut account);
        // Auto port allocation: accounts sharing the default bind need their
        // own UDP/TCP port (one socket per port).
        let transport_kind = account
            .transport
            .as_deref()
            .and_then(crate::config::TransportKind::parse)
            .unwrap_or_default();
        if account.transport_addr.is_none()
            && matches!(
                transport_kind,
                crate::config::TransportKind::Udp | crate::config::TransportKind::Tcp
            )
        {
            let base = config
                .addr
                .clone()
                .unwrap_or_else(|| "0.0.0.0:35060".to_string());
            if let Some(addr) = pick_free_bind_addr(&base, &mut used_ports, transport_kind) {
                account.transport_addr = Some(addr);
            }
        }
        let mut bot = SipBot::new(
            account,
            config.clone(),
            Arc::new(CallStats::new()),
            verbose,
            run_token,
        );
        bot.status_board = Some(state.status_board.clone());
        bot.trace_inspector = Some(inspector::TraceInspector::new(
            state.calls.clone(),
            state.events.clone(),
            Some(std::path::PathBuf::from(
                state.current_config().records_directory(),
            )),
        ));
        bot.call_registry = Some(state.calls.clone());
        bot.ws_events = Some(state.events.clone());
        let abort = tokio::spawn(async move {
            if let Err(e) = bot.run_wait().await {
                error!("Bot wait error: {:?}", e);
            }
        })
        .abort_handle();
        state.bots.lock().unwrap().push(BotHandle {
            key,
            token,
            abort,
        });
    }
}

/// Probe-bind to find a free port; returns the addr string that worked.
/// The probe socket is dropped immediately (rsipstack binds afterwards).
fn pick_free_bind_addr(
    base: &str,
    used: &mut std::collections::HashSet<u16>,
    kind: crate::config::TransportKind,
) -> Option<String> {
    let mut addr: std::net::SocketAddr = base.parse().ok()?;
    for _ in 0..50 {
        if !used.contains(&addr.port()) {
            let probe_ok = match kind {
                crate::config::TransportKind::Tcp => {
                    std::net::TcpListener::bind(addr).is_ok()
                }
                _ => std::net::UdpSocket::bind(addr).is_ok(),
            };
            if probe_ok {
                used.insert(addr.port());
                return Some(addr.to_string());
            }
        }
        addr.set_port(addr.port() + 1);
    }
    None
}

/// Serialize hot reloads (multiple rapid config saves must not interleave).
static RELOAD_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Stop all bots and respawn from the current config (hot reload).
pub async fn reload_bots(state: &Arc<state::ServeState>, verbose: bool) {
    let _guard = RELOAD_LOCK.lock().await;
    {
        let bots = state.bots.lock().unwrap();
        for bot in bots.iter() {
            info!("[reload] stopping bot {}", bot.key);
            // Graceful: run_wait watches this token and releases the
            // transport (ports) on exit. Abort is only a fallback.
            bot.token.cancel();
        }
    }
    // Give transports time to release ports (run_wait cleanup).
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    {
        let bots = state.bots.lock().unwrap();
        for bot in bots.iter() {
            bot.abort.abort();
        }
    }
    state.bots.lock().unwrap().clear();
    spawn_bots(state, verbose);
    info!("[reload] bots respawned");
}

use super::state::ServeState;
use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;

/// WebSocket endpoint: pushes live SIP messages, call state changes and
/// (in later phases) stats snapshots to the UI.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServeState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move { handle_socket(socket, state).await })
}

async fn handle_socket(socket: axum::extract::ws::WebSocket, state: Arc<ServeState>) {
    let (mut tx, mut rx) = socket.split();
    let mut event_rx = state.subscribe();

    // Forward events to the client.
    let send_task = tokio::spawn(async move {
        loop {
            match event_rx.recv().await {
                Ok(event) => {
                    let json = serde_json::to_string(&event).unwrap_or_default();
                    if tx.send(axum::extract::ws::Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            }
        }
    });

    // Receive pings / client messages; close on disconnect.
    while let Some(Ok(msg)) = rx.next().await {
        if matches!(msg, axum::extract::ws::Message::Close(_)) {
            break;
        }
    }
    send_task.abort();
}

//! Axum routing and the per-connection WebSocket lifecycles.
//!
//! Lives in the library (not just `main.rs`) so integration tests under
//! `tests/` can drive the *real* transport — frame discipline, single-writer
//! sink, disconnect bookkeeping — with mock clients over loopback sockets.

use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};

use crate::lobby::Lobby;
use crate::protocol::ClientToServer;

/// The full application router: one `/ws` endpoint over the shared lobby.
pub fn app(lobby: Arc<Lobby>) -> Router {
    Router::new().route("/ws", get(ws_handler)).with_state(lobby)
}

/// Handles the WebSocket upgrade for one client.
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(lobby): State<Arc<Lobby>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| connection(socket, lobby))
}

/// Manages one WebSocket connection lifecycle.
///
/// The socket is split into independent halves:
///   * the *send* half is driven by the session's outbox queue:
///     `Lobby::register` returns a channel whose items are serialized and
///     written here, so lobby code never blocks on (or re-enters) this task;
///   * the *receive* half is driven here: inbound JSON is deserialized and
///     routed through the lobby.
///
/// `tokio::select!` races the two halves and tears the connection down as
/// soon as either ends; `handle_disconnect` then frees the seat and notifies
/// the room. The sink stays single-writer, so no sink mutex is required.
async fn connection(socket: WebSocket, lobby: Arc<Lobby>) {
    let (mut sink, mut stream) = socket.split();
    let id = lobby.next_player_id();
    let mut outbox = lobby.register(id);

    let sends = async {
        while let Some(event) = outbox.recv().await {
            match serde_json::to_string(&event) {
                Ok(text) => {
                    if sink.send(Message::Text(text.into())).await.is_err() {
                        // Socket is unwritable (client gone): stop sending.
                        break;
                    }
                }
                Err(error) => {
                    // Unreachable for internally-constructed messages.
                    eprintln!("player {id}: outbound serialization failed: {error}");
                }
            }
        }
    };

    let recvs = {
        let lobby = Arc::clone(&lobby);
        async move {
            while let Some(frame) = stream.next().await {
                match frame {
                    Ok(Message::Text(text)) => {
                        match serde_json::from_str::<ClientToServer>(&text) {
                            Ok(event) => lobby.handle_event(id, event).await,
                            Err(error) => {
                                lobby.reject(
                                    id,
                                    format!("unrecognized message: {error}"),
                                );
                            }
                        }
                    }
                    Ok(Message::Binary(_)) => {
                        lobby.reject(id, "binary frames are not supported".to_string());
                    }
                    // Tungstenite answers pings transparently; ignore either frame.
                    Ok(Message::Ping(_) | Message::Pong(_)) => {}
                    Ok(Message::Close(_)) | Err(_) => break,
                }
            }
        }
    };

    tokio::select! {
        _ = sends => {},
        _ = recvs => {},
    }
    // The canceled half is dropped, closing the socket fully; the lobby is
    // always informed exactly once so the seat is freed and the table is
    // notified regardless of which half ended first.
    lobby.handle_disconnect(id).await;
}

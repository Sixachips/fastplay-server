//! Entry point: Axum routing and the per-connection WebSocket lifecycles.

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
use tokio::net::TcpListener;

use fastplay_server::lobby::Lobby;
use fastplay_server::protocol::ClientToServer;

const BIND_ADDR: &str = "0.0.0.0:8080";

#[tokio::main]
async fn main() {
    let lobby = Arc::new(Lobby::new());
    let app = Router::new().route("/ws", get(ws_handler)).with_state(lobby);

    let listener = TcpListener::bind(BIND_ADDR).await.expect("failed to bind 0.0.0.0:8080");
    println!("fastplay headless dominoes server listening on ws://{BIND_ADDR}/ws");

    axum::serve(listener, app).await.expect("axum server failed");
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

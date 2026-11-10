//! Entry point: bind the socket and hand everything to the library's
//! [`fastplay_server::net`] transport (which integration tests drive too).

use std::sync::Arc;

use tokio::net::TcpListener;

use fastplay_server::{lobby::Lobby, net};

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8080";

#[tokio::main]
async fn main() {
    let lobby = Arc::new(Lobby::new());

    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string());
    let listener = TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {bind_addr}: {e}"));
    println!("fastplay headless dominoes server listening on ws://{bind_addr}/ws");

    axum::serve(listener, net::app(lobby)).await.expect("axum server failed");
}

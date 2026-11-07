//! Entry point: bind the socket and hand everything to the library's
//! [`fastplay_server::net`] transport (which integration tests drive too).

use std::sync::Arc;

use tokio::net::TcpListener;

use fastplay_server::{lobby::Lobby, net};

const BIND_ADDR: &str = "0.0.0.0:8080";

#[tokio::main]
async fn main() {
    let lobby = Arc::new(Lobby::new());

    let listener = TcpListener::bind(BIND_ADDR).await.expect("failed to bind 0.0.0.0:8080");
    println!("fastplay headless dominoes server listening on ws://{BIND_ADDR}/ws");

    axum::serve(listener, net::app(lobby)).await.expect("axum server failed");
}

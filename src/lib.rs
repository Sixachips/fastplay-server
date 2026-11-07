//! fastPlay headless Dominoes server.
//!
//! Pure authoritative backend: an in-memory game state machine served over
//! WebSockets with a flat, Serde-tagged JSON protocol (see `protocol.rs`).
//! The client/frontend lives in a separate repository and only consumes the
//! wire contract documented there.
//!
//! Module layout:
//!   * [`game`] — pure, deterministic dominoes engine. No async, no I/O, no
//!     shared state; a room owns its `GameState` by value.
//!   * [`lobby`] — thread-safe room/lobby manager: player registration, seat
//!     assignment, message routing, and broadcast fan-out.
//!   * [`protocol`] — the public serde JSON contract shared with clients.
//!   * [`net`] — Axum routing and the per-connection WebSocket loops (in the
//!     library so `tests/mock_client.rs` can drive the real transport).
//!   * `main` — thin entry point: bind the address, serve [`net::app`].
//!
//! Concurrency model:
//!   * `DashMap` for global lookups (sessions, rooms, player→room routing)
//!   * one `tokio::sync::Mutex` per room guarding seats and the game; all
//!     mutations and the broadcasts they trigger happen while it is held, so
//!     every client sees a totally ordered stream of updates for its room
//!   * room mutexes are never nested (a join fully leaves its old room first)
//!     and no other await happens while one is held
//!   * one unbounded `tokio::sync::mpsc` channel per connected player for
//!     outbound messages, drained by a dedicated WebSocket send task, so the
//!     lobby never blocks on a slow client

#![forbid(unsafe_code)]

pub mod game;
pub mod lobby;
pub mod net;
pub mod protocol;

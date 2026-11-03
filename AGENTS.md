# fastplay-server — AGENTS.md

Compact guidance for working in `server/` (`fastplay-server`).

## Crate & Repository Boundary

- Standalone Cargo package (`fastplay-server`), **not** part of a Cargo workspace with `../client`. Run all `cargo` commands from this directory (`server/`).
- `#![forbid(unsafe_code)]` is enforced in `src/lib.rs`.
- `src/protocol.rs` is the serde JSON wire contract mirrored by `../client/src/protocol.rs`. Any wire shape change here must stay in sync with the client crate.

## Commands

| Task | Command |
|------|---------|
| Run server (`ws://0.0.0.0:8080/ws`) | `cargo run` |
| Run all 33 unit tests | `cargo test` |
| Run a single test | `cargo test <name>` (e.g. `cargo test play_again_redeals`) |
| Lint / typecheck | `cargo clippy --all-targets` |
| Release build | `cargo build --release` |

## Non-Obvious Quirks & Stale Docs

- **`BIND_ADDR` is hardcoded**: Despite `README.md` claiming `BIND_ADDR=127.0.0.1:8080 cargo run` works, `src/main.rs` defines `const BIND_ADDR: &str = "0.0.0.0:8080"` and never reads `std::env::var`.
- **`README.md` protocol tables are incomplete**: `README.md` omits inbound `PlayAgain`, outbound `GameClosed { hands }`, and `PublicSeat.ready`. Always treat `src/protocol.rs` as the source of truth.
- **`RoomPhase::Finished` is never emitted**: `lobby::room_phase()` only returns `RoomPhase::Playing` (when a live game is active) or `RoomPhase::Waiting` (all other states, including after a game ends). Furthermore, on the 4th player's join, `RoomState` is broadcast *before* `deal_fresh_game`, so its phase still reads `waiting`. Clients detect active/ended games from `GameStateUpdate.state.result`, not `RoomState.phase`.
- **Seats are fixed indices (`0..=3`), never compacted**: `RoomInner.seats` is `[Option<Occupant>; 4]` pinned to `North` (0), `East` (1), `South` (2), `West` (3). When a player disconnects, their slot becomes `None` (omitted from `PublicRoomState.seats`), surviving players keep their seat numbers, and the next joiner takes the first vacant index.
- **`HandDealt` is sent once per deal**: Private hands are pushed only when `deal_fresh_game` runs. Subsequent moves only broadcast `GameStateUpdate` with `hand_counts: [u8; 4]`. Opponent hands remain hidden unless a game ends in `EndReason::Blocked`, which broadcasts `GameClosed { hands }` immediately after the final `GameStateUpdate`.
- **In-place rematches (`PlayAgain`)**: Clients do not reconnect to play again. Each seated player sends `{"type": "PlayAgain"}` after a game finishes; once all 4 seats have `ready == true`, `deal_fresh_game` deals a new game in place and resets all `ready` flags to `false`.

## Engine & Concurrency Invariants

- **Untrusted input must use `Tile::try_new` / `Tile::try_from(PublicTile)`**: `Tile::new(a, b)` asserts `a <= 6 && b <= 6` and **panics** on out-of-range pips. Always validate at the `protocol` / `lobby` boundary before calling into `game.rs`.
- **Tile orientation differs between hand and board**:
  - In hands (`GameState::hand`), tiles are normalized (`a <= b`) and sorted.
  - On the board (`Board::chain`), tiles are stored in physical left-to-right orientation (`a` faces left, `b` faces right).
  - On an empty board, `Board::play` ignores `side` and `GameState::legal_moves` reports `Side::Right` for every tile.
- **Locking & fan-out**:
  - Global lookup tables in `Lobby` use `DashMap`; each `Room` has a single `tokio::sync::Mutex<RoomInner>`.
  - Never acquire another room lock or global lock while holding a room's `inner` mutex.
  - All state transitions and their `mpsc::UnboundedSender` broadcasts happen inside the room lock so every seat observes an identical event order without blocking on slow WebSocket sinks.
- **Testing with partial hands**: Use the `#[cfg(test)]` helper `GameState::from_hands([Vec<Tile>; 4])` in `src/game.rs` to stage deterministic board/block scenarios without a full 28-tile deal.

# fastplay-server — AGENTS.md

Compact guidance for working in `server/` (`fastplay-server`).

## Crate & Repository Boundary

- Standalone Cargo package (`fastplay-server`), **not** part of a Cargo workspace with `../client`. Run all `cargo` commands from this directory (`server/`).
- `#![forbid(unsafe_code)]` is enforced in `src/lib.rs`.
- `src/protocol.rs` is the serde JSON wire contract mirrored by `../client/src/protocol.rs`. Any wire shape change here must stay in sync with the client crate.

## Commands

| Task | Command |
|------|---------|
| Run server (`ws://127.0.0.1:8080/ws`) | `cargo run` |
| Run all 44 unit + 2 wire-integration tests | `cargo test` |
| Run a single test | `cargo test <name>` (e.g. `cargo test play_again_redeals`) |
| Lint / typecheck | `cargo clippy --all-targets` |
| Release build | `cargo build --release` |
| Validate the protocol doc | `npx @asyncapi/cli validate docs/asyncapi.yaml` |

## Non-Obvious Quirks & Stale Docs

- **`BIND_ADDR` env override**: `src/main.rs` reads `BIND_ADDR` and falls back to `DEFAULT_BIND_ADDR = "127.0.0.1:8080"` (localhost only, so nginx fronts it). Set `BIND_ADDR=0.0.0.0:8080` to listen on all interfaces.
- **`docs/asyncapi.yaml` mirrors the wire contract**: the full AsyncAPI 3.1 spec (messages, schemas, ordering, error table) lives in `docs/asyncapi.yaml`, validated with `npx @asyncapi/cli validate docs/asyncapi.yaml`. It is derived from `src/protocol.rs` + `src/lobby.rs` — when you change the protocol, update the spec in the same commit.
- **`README.md` protocol tables are now in sync** with `src/protocol.rs` (inbound `PlayAgain`, outbound `GameClosed { hands }`, and `PublicSeat.ready` are covered), but still treat `src/protocol.rs` as the source of truth.
- **`RoomPhase::Finished` is never emitted**: `lobby::room_phase()` only returns `RoomPhase::Playing` (when a live game is active) or `RoomPhase::Waiting` (all other states, including after a game ends). Furthermore, on the 4th player's join, `RoomState` is broadcast *before* `deal_fresh_game`, so its phase still reads `waiting`. Clients detect active/ended games from `GameStateUpdate.state.result`, not `RoomState.phase`.
- **Seats are fixed indices (`0..=3`), never compacted**: `RoomInner.seats` is `[Option<Occupant>; 4]` pinned to `North` (0), `West` (1), `South` (2), `East` (3). The index ring `0→1→2→3` walks **counter-clockwise** around the table (each turn passes to the passer's right-hand neighbour, per `../Rules.md`), so seat 1 is the *West* seat and seat 3 the *East* one. When a player disconnects, their slot becomes `None` (omitted from `PublicRoomState.seats`), surviving players keep their seat numbers, and the next joiner takes the first vacant index.
- **Team preference on join**: `ClientToServer::JoinRoom` carries an optional `team` (0 or 1). A seat's partnership is `seat % 2` (`game::team_of` / `game::team_seats`), so honoring a preference means picking a vacant seat inside that partnership's two seats, then falling back to the first vacant seat at the table. A full requested team never refuses a join — the client must confirm the actual side from `PublicSeat.team` on `RoomState`. Teams are structural: a full table is always exactly two-vs-two, so the engine, scoring, dealing, opening policy, and rematch flow need no changes.
- **`HandDealt` is sent once per deal**: Private hands are pushed only when `deal_fresh_game` runs. Subsequent moves only broadcast `GameStateUpdate` with `hand_counts: [u8; 4]`. Opponent hands stay hidden while the hand is live; a `Blocked` end broadcasts the full `GameClosed { hands }` reveal, and *every* ending publishes only the per-seat pip totals (`result.hand_pips`) — enough to audit the score without leaking tile lists of a played-out win.
- **Partnership & scoring (Rules.md: standard cumulative, 100 points)**: Teams are seat parity — team 0 = seats {0, 2} (North–South), team 1 = seats {1, 3} (West–East); `game::team_of/team_seats/rival_team`. A blocked hand is won by the team with the lower **combined** pips (individual minima are irrelevant), and winners always bank the **losing team's** pips (`GameResult.points`, computed by `GameState::settle`). `MatchScore` (pure, in `game.rs`) lives on `RoomInner.score`; `Room::commit` folds each `Outcome::Ended` into it *before* building the terminating snapshot, so `GameStateUpdate.state.score` is already up to date. `deal_fresh_game` wipes a `score.is_over()` match — "Play again" after a match win transparently starts match 2 in place. Watch the borrowck detail: `commit` destructures `&mut *guard` into `RoomInner { seats, game, score }` once; naive `inner.game`/`inner.score` field borrows through the `MutexGuard` DerefMut conflict.
- **In-place rematches (`PlayAgain`)**: Clients do not reconnect to play again. Each seated player sends `{"type": "PlayAgain"}` after a game finishes; once all 4 seats have `ready == true`, `deal_fresh_game` deals a new game in place and resets all `ready` flags to `false`.

## Engine & Concurrency Invariants

- **Opening policy lives in the room layer, not the engine**: `lobby::deal_fresh_game` picks the opener. A session's first hand (no previous game in the room) uses `GameState::first_hand`, which forces the 6|6 holder to lay the 6|6; every later hand uses `GameState::new(tiles, opener)` with the previous hand's playing-out seat (`result.out`), or — when no seat played out (blocked hand or voided game) — one seat counter-clockwise from the previous opener. A blocked hand is won by a *partnership*, never a seat, so it can never "name" next's opener. The engine only enforces a forced opening tile if one is supplied — `MoveError::WrongOpeningTile` until it is laid.
- **`opening_tile` on the wire**: `PublicGameState.opening_tile` mirrors the forced opening tile while the board is empty, so clients can keep non-6|6 tiles unpickable on the first hand. It is `null` on every later hand.
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

## Rust & WebSocket Best Practices (verified, do not regress)

Patterns already implemented in `src/` — keep them when editing:

- **Single-writer sink, no sink mutex** (`src/net.rs::connection`): the socket is `split()` into independent halves; the *send* half is owned by exactly one task draining the outbox channel, the *receive* half routes inbound frames. One writer per socket means no locking around `sink.send`.
- **`tokio::select!` races the two halves** and tears down as soon as either ends; the canceled half is dropped, closing the socket fully. `lobby::handle_disconnect(id)` runs exactly once, after the select, regardless of which half died first — never duplicate the disconnect path.
- **Backpressure policy: unbounded outbox, non-blocking fan-out** (`Room::broadcast` / `deal_private_hands` use `let _ = outbox.send(...)`): a slow or half-dead client must never stall a room's critical section. A dangling receiver silently drops the event; write errors break the send loop rather than panicking.
- **Broadcast order == commit order**: every mutation and its fan-out happen while holding the room's `tokio::Mutex<RoomInner>`, so each seat's outbox observes a totally ordered event stream for its room.
- **Two mutex tiers, chosen by critical-section shape**: `std::sync::Mutex` only for short, never-crossed-with-await sections (e.g. `Connection.room` binding, guarded with `.expect("uncontended mutex")`); `tokio::sync::Mutex` for anything held across `.await` (room `inner`). Never hold a room mutex while acquiring another room's mutex or a global table.
- **Cancel-safety around awaits**: clone outboxes / `Arc<Room>` handles *before* awaiting (see `Lobby::join`), and re-check registration after the await (`self.connections.contains_key(&id)`) to undo seating if the socket died mid-join.
- **Atomic room creation and pointer-equality pruning**: room codes are created via `DashMap::entry` (racing joiners share one `Room`); empty rooms are removed via `DashMap::remove_if` with `Arc::ptr_eq`, so a newer room under a recycled code is never pruned by mistake.
- **Server-issued identity**: `PlayerId` (monotonic `AtomicU64`, `Ordering::Relaxed` is fine — it's a key, not a clock) is minted at upgrade time and used to authenticate every command; clients can never claim another seat.
- **Input hardened at the wire boundary** (`src/protocol.rs`, `Lobby::join` / `PlayTile` arm): all client values go through `Tile::try_from(PublicTile)`, `normalize_room_code`, `parse_team`, and the `MAX_ROOM_CODE_LEN` / `MAX_PLAYER_NAME_LEN` caps before reaching the engine, which may otherwise panic on impossible internal states. `parse_team` rejects anything outside `0..TEAM_COUNT` before the room registry is touched, so a bad preference cannot leave an empty room behind.
- **Frame discipline** (`src/net.rs::recvs`): text frames only — binary frames get a `MoveRejected`, `Ping`/`Pong` are ignored (tungstenite answers pings transparently), and `Close` frames / transport errors break the receive loop. The transport lives in the library (`src/net.rs`) so `tests/mock_client.rs` drives the *real* stack over loopback WebSockets; `main.rs` is only the bind-and-serve entry point.
- **Pure engine split**: `game.rs` is sync, no I/O, no shared state — all async, locking, and channel logic lives in `lobby.rs`/`net.rs` (with `main.rs` just binding the address). Keep that boundary: engine functions stay `fn`, room/lobby/net functions stay `async fn`.

# Game type:
- read Rules.md
- Winning: Stardard / Latin rules
- Scoring System: Stardard cumulative scoring (100 points)

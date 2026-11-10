# fastplay-server

**Headless authoritative server for a 4-player dominoes game.**

A pure Rust WebSocket server that implements the game logic, lobby management, and wire protocol for a double-six dominoes game. The client/frontend lives in a separate repository and consumes the documented wire contract.

---

## Table of Contents

- [Overview](#overview)
- [Game Rules](#game-rules)
- [Protocol](#protocol)
- [Building & Running](#building--running)
- [Running Tests](#running-tests)
- [Project Structure](#project-structure)
- [License](#license)

---

## Overview

`fastplay-server` is the backend authority for a 4-player dominoes game. It:

- Manages 4-player rooms (lobby + game state) in memory
- Validates and routes player commands (join, play tile, pass turn)
- Broadcasts game state updates to all connected clients
- Handles disconnections gracefully (voids running games, frees seats)
- Provides a stable WebSocket JSON protocol for client communication

The server is **purely authoritative** — game state never leaves the server, and only per-seat visible information is transmitted to clients.

---

## Game Rules

The server implements a double-six (28-tile) **partnership** dominoes game for exactly 4 players (`Rules.md`):

- **Teams:** Two fixed partnerships — seats 0 & 2 (North–South) play against seats 1 & 3 (West–East); teammates sit opposite. The team of a seat is `seat % 2`.
- **Setup:** Each player is dealt 7 tiles from the shuffled double-six set (no boneyard).
- **Opening:** The first hand of a session is opened by the seat holding the 6–6, which must lay that tile down. Every later hand is opened by the seat that **played out** the previous hand; a hand with no playing-out seat (blocked or voided) rotates the open one seat counter-clockwise from the previous opener. Later hands may be opened with any tile.
- **Play:** Players take turns placing one tile on either end of the linear board, counter-clockwise around the table. A tile must match the pips on the end it's played against.
- **Passing:** If a player has no legal play, they must pass (voluntary passing is rejected). Four consecutive passes (from all seats) block the game.
- **Ending a hand:**
  - A player empties their hand (**HandEmpty** — their partnership wins the hand)
  - Four consecutive passes (**Blocked** — the partnership with the lower *combined* pip total wins; equal team totals are a draw)
  - A player disconnects mid-game — the server voids the hand (**Abandoned**, no points)
- **Scoring (standard cumulative, 100 points):** The winning partnership banks the pips left in the losing partnership's two hands. Points accumulate across hands; the first team to reach **100** wins the match. Draws and voided hands award nothing. Tapping "Play again" after a decided match starts a fresh match from 0–0 on the same table.

---

## Protocol

All messages are JSON objects with a top-level `"type"` discriminator. The wire format is flat and Serde-tagged.

> The machine-readable contract lives in [`docs/asyncapi.yaml`](docs/asyncapi.yaml) (AsyncAPI 3.1, validated with `npx @asyncapi/cli validate docs/asyncapi.yaml`). It documents every message, schema, constraint, delivery guarantee, and event-ordering rule. `src/protocol.rs` remains the code-level source of truth; the tables below are a quick-reference summary.

### Inbound (Client → Server)

| Type | Fields | Description |
|------|--------|-------------|
| `JoinRoom` | `room` (String), `name` (String), `team` (0 \| 1, optional) | Join a room by code; name must be 1–32 chars. `team` is a best-effort partnership preference (0 = seats 0 & 2, 1 = seats 1 & 3); a full requested team never refuses the join, and the actual side is confirmed from `RoomState`. |
| `PlayTile` | `tile` ( `{left: u8, right: u8}` ), `side` ( `"left"` \| `"right"` ) | Play a tile from your hand at the specified end. |
| `PassTurn` | *(none)* | Pass your turn (only legal when you have no playable tile). |
| `PlayAgain` | *(none)* | Signal readiness for the next game; the room re-deals in place once all four seated players are ready (no socket cycling). |

### Outbound (Server → Client)

| Type | Fields | Description |
|------|--------|-------------|
| `RoomState` | `room` (`PublicRoomState`) | Personalized roster with `you` set to the recipient's seat. Each seat carries a `ready` flag (`true` once that player tapped `PlayAgain`) and a `team` (0 = seats 0 & 2, 1 = seats 1 & 3). |
| `HandDealt` | `seat` (u8), `hand` (`Vec<PublicTile>`) | Sent privately to each seat when a game is dealt (7 tiles each, once per deal — never re-sent). |
| `GameStateUpdate` | `state` (`PublicGameState`) | Broadcast after every accepted move or pass. |
| `GameClosed` | `hands` (`Vec<ClosedHand>`) | Blocked-game reveal: every seat's remaining tiles, broadcast to all seats right after the terminating `GameStateUpdate`. Only sent when the game ends `blocked`. |
| `MoveRejected` | `reason` (String) | Server-side rejection (e.g., "it is not your turn"). Private to the offending socket. |
| `OpponentDisconnected` | `player` (String), `seat` (u8) | Notified when a player leaves the table. |

### `PublicGameState`

Fields visible to all seats:

- `turn` — whose turn it is
- `board` — played tiles, left-to-right, physically oriented
- `left_end` / `right_end` — pips at the outer ends of the board (or `null`)
- `hand_counts` — number of tiles remaining per seat (opponents' hands are hidden)
- `opening_tile` — while the board is empty, the only tile the opener may lay (the 6–6 on a session's first hand; `null` on later hands)
- `consecutive_passes` — how many passes have been recorded in a row
- `result` — present once the hand has ended; see `GameResult` below
- `score` — the running cumulative match score (`PublicMatchScore`), on every snapshot

### `GameResult` (in the terminating `GameStateUpdate`)

- `out` — the seat that emptied its hand (`hand_empty` endings only, else `null`)
- `winner_team` — the winning partnership (0 = seats 0 & 2, 1 = seats 1 & 3; `null` on draw/void)
- `points` — pips banked by the winners: the combined pips left in the losing team's hands (0 on draw/void)
- `hand_pips` — pips still held per absolute seat when the hand ended (the score's raw material, public only after the hand)
- `reason` — `hand_empty` | `blocked` | `abandoned`

### `PublicMatchScore`

- `points` — `[team0, team1]` cumulative totals for the current match
- `target` — points needed to win the match (100)
- `winner_team` — set once the match is over
- `over` — whether a team has reached `target`; the next in-place redeal starts a fresh match from 0–0

### Room Phase

- `waiting` — seats are open; a game deals once the 4th player sits. **Also reported after a game ends** (see caveat below).
- `playing` — a live game is in progress.
- `finished` — defined in the protocol enum but **never emitted** by the server today: `room_phase()` only returns `playing` while a live game exists and `waiting` otherwise. Detect ended games from `GameStateUpdate.state.result`, not from the phase.

---

## Building & Running

### Prerequisites

- [Rust](https://www.rust-lang.org/) (1.70+ recommended)
- [Cargo](https://doc.rust-lang.org/cargo/)

### Install

```bash
# From the server directory
cd server
cargo build
```

### Run

```bash
cargo run
```

The server binds to `127.0.0.1:8080` by default (localhost only). It will print:

```
fastplay headless dominoes server listening on ws://127.0.0.1:8080/ws
```

### Custom bind address

Override the bind address at runtime with the `BIND_ADDR` environment variable:

```bash
# Listen on all interfaces (exposes the server directly; no reverse proxy)
BIND_ADDR=0.0.0.0:8080 cargo run
```

---

## Running Tests

```bash
cargo test
```

All unit tests are contained within the source modules (`lobby.rs`, `protocol.rs`, `game.rs`); the WebSocket integration suite lives in `tests/mock_client.rs`. Coverage:

- Room creation, joining, and dealing hands
- Turn enforcement and move rejection
- Disconnect voids a running game and frees the seat (voided hands award no points)
- Empty-room pruning after all seats leave
- Malformed room codes and player names are rejected
- Blocked-game resolution by lowest **combined team** pip total (Latin rules)
- Team ties (equal combined pips) are a draw and bank nothing
- Playing out scores the pips left in the rival partnership's hands
- Match accumulation: hands bank points across redeals, the match ends at 100, and the next redeal starts a fresh match from 0–0
- Double-six deal produces four full hands and a deterministic starter
- Legal moves, tile placement, and board re-orientation
- A seat clearing its hand wins the hand for its partnership

---

## Project Structure

```
server/
├── Cargo.toml          # Rust package metadata & dependencies
├── Cargo.lock          # Locked dependency versions
├── src/
│   ├── main.rs         # Thin entry point: bind address + serve net::app
│   ├── lib.rs          # Module declarations + concurrency model docs
│   ├── net.rs          # Axum router + per-connection WebSocket loops (in the lib so tests can drive it)
│   ├── protocol.rs     # JSON wire contract + message types
│   ├── lobby.rs        # Thread-safe room/lobby manager (DashMap, Mutex, mpsc channels)
│   └── game.rs         # Pure dominoes engine (no networking, no async, no shared state)
├── tests/
│   └── mock_client.rs  # 4 mock clients over real WebSockets: full scored match + transport edge cases
└── README.md           # This file
```

### Key design decisions

- **`game.rs`** — pure game logic, no `async`, no `I/O`, no shared state. `GameState` is a plain value owned by the lobby.
- **`lobby.rs`** — thread-safe room management using `DashMap` for global lookups and a `Mutex` per room. All mutations happen inside the room lock, so every client sees a totally ordered stream.
- **`protocol.rs`** — the public Serde JSON contract shared with clients. Wire types are flat, Serde-tagged enums discriminated by `"type"`.
- **`net.rs`** — Axum routing and per-connection WebSocket loops (single-writer sink, `tokio::select!` teardown). Lives in the library so `tests/mock_client.rs` can drive the real transport over loopback; `main.rs` only binds and serves.
- **`tests/mock_client.rs`** — integration harness: four scripted players over real WebSockets play a full partnership match to 100 and rematch past the reset, re-verifying team scoring, points, score accumulation, tile conservation, and broadcast byte-equality at every socket — plus frame-discipline edge cases (binary/garbage rejected without killing the socket).
- **One unbounded `mpsc` channel per connected player** for outbound messages, drained by a dedicated send task so the lobby never blocks on a slow client.

---

## License

This project is open source. See the LICENSE file (or add one if you haven't already).

---

## Further Notes

- The server expects a client/frontend that knows the wire protocol documented in `protocol.rs`. The client repository is separate.
- Room codes are user-supplied strings up to 16 characters; they are trimmed and validated on join.
- Player names are 1–32 characters; empty or overly long names are rejected.
- The server binds to `127.0.0.1:8080` by default; change via the `BIND_ADDR` environment variable.
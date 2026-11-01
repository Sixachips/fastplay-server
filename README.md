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

The server implements a double-six (28-tile) dominoes game for exactly 4 players:

- **Setup:** Each player is dealt 7 tiles from the shuffled double-six set.
- **Opening:** The seat holding the highest double opens the game. If no one holds a double, the seat with the highest-pip tile opens (seat order breaks ties).
- **Play:** Players take turns placing one tile on either end of the linear board. A tile must match the pips on the end it's played against.
- **Passing:** If a player has no legal play, they must pass. Four consecutive passes (from all seats) block the game.
- **Ending:** The game ends when:
  - A player empties their hand (**HandEmpty** — that player wins)
  - Four consecutive passes occur with no legal moves for anyone (**Blocked** — the player with the fewest pips in hand wins; ties are a draw)
  - A player disconnects mid-game — the server voids the game (**Abandoned**)

---

## Protocol

All messages are JSON objects with a top-level `"type"` discriminator. The wire format is flat and Serde-tagged.

### Inbound (Client → Server)

| Type | Fields | Description |
|------|--------|-------------|
| `JoinRoom` | `room` (String), `name` (String) | Join a room by code; name must be 1–32 chars. |
| `PlayTile` | `tile` ( `{left: u8, right: u8}` ), `side` ( `"left"` \| `"right"` ) | Play a tile from your hand at the specified end. |
| `PassTurn` | *(none)* | Pass your turn (only legal when you have no playable tile). |

### Outbound (Server → Client)

| Type | Fields | Description |
|------|--------|-------------|
| `RoomState` | `room` (`PublicRoomState`) | Personalized roster with `you` set to the recipient's seat. |
| `HandDealt` | `seat` (u8), `hand` (`Vec<PublicTile>`) | Sent to each seat when a game is dealt (7 tiles each). |
| `GameStateUpdate` | `state` (`PublicGameState`) | Broadcast after every accepted move or pass. |
| `MoveRejected` | `reason` (String) | Server-side rejection (e.g., "it is not your turn"). |
| `OpponentDisconnected` | `player` (String), `seat` (u8) | Notified when a player leaves the table. |

### `PublicGameState`

Fields visible to all seats:

- `turn` — whose turn it is
- `board` — played tiles, left-to-right, physically oriented
- `left_end` / `right_end` — pips at the outer ends of the board (or `null`)
- `hand_counts` — number of tiles remaining per seat (opponents' hands are hidden)
- `consecutive_passes` — how many passes have been recorded in a row
- `result` — present once the game has ended (`winner` may be `None` for a draw or abandoned game)

### Room Phase

- `Waiting` — seats are open; game starts once the 4th player sits
- `Playing` — a game is in progress
- `Finished` — the last game ended; the room stays visible until the next redeal

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

The server binds to `0.0.0.0:8080` by default. It will print:

```
fastplay headless dominoes server listening on ws://0.0.0.0:8080/ws
```

### Custom bind address

Override the bind address at runtime:

```bash
BIND_ADDR=127.0.0.1:8080 cargo run
```

---

## Running Tests

```bash
cargo test
```

All unit tests are contained within the source modules (`lobby.rs`, `protocol.rs`, `game.rs`) and cover:

- Room creation, joining, and dealing hands
- Turn enforcement and move rejection
- Disconnect voids a running game and frees the seat
- Empty-room pruning after all seats leave
- Malformed room codes and player names are rejected
- Blocked-game resolution by lowest pip count
- Double-six deal produces four full hands and a deterministic starter
- Legal moves, tile placement, and board re-orientation
- Blocked games with tied pips are a draw
- A player clearing their hand wins the game

---

## Project Structure

```
server/
├── Cargo.toml      # Rust package metadata & dependencies
├── Cargo.lock      # Locked dependency versions
├── src/
│   ├── main.rs     # Axum router + WebSocket entry point
│   ├── lib.rs      # Module declarations + concurrency model docs
│   ├── protocol.rs # JSON wire contract + message types
│   ├── lobby.rs    # Thread-safe room/lobby manager (DashMap, Mutex, mpsc channels)
│   └── game.rs     # Pure dominoes engine (no networking, no async, no shared state)
└── README.md       # This file
```

### Key design decisions

- **`game.rs`** — pure game logic, no `async`, no `I/O`, no shared state. `GameState` is a plain value owned by the lobby.
- **`lobby.rs`** — thread-safe room management using `DashMap` for global lookups and a `Mutex` per room. All mutations happen inside the room lock, so every client sees a totally ordered stream.
- **`protocol.rs`** — the public Serde JSON contract shared with clients. Wire types are flat, Serde-tagged enums discriminated by `"type"`.
- **`main.rs`** — Axum routing and per-connection WebSocket loops. One unbounded `mpsc` channel per connected player for outbound messages, drained by a dedicated send task so the lobby never blocks on a slow client.

---

## License

This project is open source. See the LICENSE file (or add one if you haven't already).

---

## Further Notes

- The server expects a client/frontend that knows the wire protocol documented in `protocol.rs`. The client repository is separate.
- Room codes are user-supplied strings up to 16 characters; they are trimmed and validated on join.
- Player names are 1–32 characters; empty or overly long names are rejected.
- The server binds to `0.0.0.0:8080` by default; change via the `BIND_ADDR` environment variable.
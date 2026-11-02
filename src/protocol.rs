//! Public JSON protocol contract for the decoupled client repository.
//!
//! Every message is a JSON object discriminated by a top-level `"type"` field
//! (internally tagged enums), keeping the wire format flat and easy for
//! clients to dispatch on. Examples:
//!
//! ```json
//! {"type": "JoinRoom", "room": "table-42", "name": "Ada"}
//! {"type": "PlayTile", "tile": {"left": 3, "right": 4}, "side": "right"}
//! {"type": "PassTurn"}
//! ```
//!
//! ```json
//! {"type": "RoomState", "room": {"room_id": "table-42", "you": 0, "phase": "playing", "seats": [...]}}
//! {"type": "HandDealt", "seat": 0, "hand": [{"left": 6, "right": 6}, ...]}
//! {"type": "GameStateUpdate", "state": {"turn": 1, "board": [...], ...}}
//! {"type": "MoveRejected", "reason": "it is not your turn"}
//! {"type": "OpponentDisconnected", "player": "Bob", "seat": 2}
//! ```
//!
//! This module owns the serialization boundary: engine types are never sent
//! verbatim where the wire shape would leak implementation detail, and
//! client-supplied values are validated before they can reach the engine
//! (which is allowed to panic on impossible internal states).

use serde::{Deserialize, Serialize};

use crate::game::{self, GameResult, GameState, SEAT_COUNT, TableSide};

/// Which end of the linear board a tile is played on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Left,
    Right,
}

impl From<Side> for game::Side {
    fn from(s: Side) -> Self {
        match s {
            Side::Left => game::Side::Left,
            Side::Right => game::Side::Right,
        }
    }
}

/// A domino pair as exposed on the wire.
///
/// * In a hand, `left <= right` (canonical orientation).
/// * On the board, the pips are the physical orientation: `left` is the pip
///   facing the left-hand neighbour, `right` the pip facing the right-hand
///   neighbour.
/// * In `PlayTile` any orientation is accepted; the server normalizes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicTile {
    pub left: u8,
    pub right: u8,
}

impl From<game::Tile> for PublicTile {
    fn from(t: game::Tile) -> Self {
        // Board-chain tiles are stored pre-oriented and hand tiles are
        // normalized, so exposing `a`/`b` as `left`/`right` is faithful.
        Self { left: t.a, right: t.b }
    }
}

impl TryFrom<PublicTile> for game::Tile {
    type Error = &'static str;

    fn try_from(t: PublicTile) -> Result<Self, Self::Error> {
        game::Tile::try_new(t.left, t.right).ok_or("pip values must be between 0 and 6")
    }
}

/// Inbound messages from a client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ClientToServer {
    JoinRoom {
        room: String,
        name: String,
    },
    PlayTile {
        tile: PublicTile,
        side: Side,
    },
    PassTurn,
}

/// Outbound messages from the server.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerToClient {
    RoomState {
        room: PublicRoomState,
    },
    HandDealt {
        seat: u8,
        hand: Vec<PublicTile>,
    },
    GameStateUpdate {
        state: PublicGameState,
    },
    MoveRejected {
        reason: String,
    },
    OpponentDisconnected {
        player: String,
        seat: u8,
    },
}

/// Lifecycle of a room, mirrored from the lobby's view of the game.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoomPhase {
    /// Seats are still open; the game starts once the fourth player sits.
    Waiting,
    /// A game is in progress.
    Playing,
    /// The last game ended; the next full room redeal starts a new one.
    Finished,
}

/// One seat in a room. Vacant seats are simply absent from `seats`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicSeat {
    pub seat: u8,
    pub player_id: u64,
    pub name: String,
    pub table_side: TableSide,
}

/// Snapshot of a room sent on join and on every membership change. It is
/// personalized: `you` is the seat of the recipient of this message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicRoomState {
    pub room_id: String,
    pub you: usize,
    pub phase: RoomPhase,
    pub seats: Vec<PublicSeat>,
}

/// Full game view broadcast after every accepted move or pass. Opponents'
/// actual tiles are hidden information and never leave the server; only the
/// per-seat tile counts are exposed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicGameState {
    /// Seat whose turn it is.
    pub turn: usize,
    /// Played tiles, left to right, physically oriented.
    pub board: Vec<PublicTile>,
    /// Pip facing the outside of the board's left end.
    pub left_end: Option<u8>,
    /// Pip facing the outside of the board's right end.
    pub right_end: Option<u8>,
    /// Tile count per seat (`hand_counts[0]` is seat 0, etc.).
    pub hand_counts: [u8; SEAT_COUNT],
    /// How many passes were recorded in a row (resets on any play).
    pub consecutive_passes: u8,
    /// Present once the game has ended (winner `None` means a draw or an
    /// abandoned game).
    pub result: Option<GameResult>,
}

/// Builds the full public game view from the authoritative in-memory state.
pub fn game_state_view(game: &GameState) -> PublicGameState {
    PublicGameState {
        turn: game.turn(),
        board: game.board().chain().iter().copied().map(Into::into).collect(),
        left_end: game.board().left_end(),
        right_end: game.board().right_end(),
        hand_counts: game.hand_counts().map(|c| c as u8),
        consecutive_passes: game.passes_in_row() as u8,
        result: game.result(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inbound_messages_parse_from_tagged_json() {
        let join: ClientToServer = serde_json::from_str(
            r#"{"type": "JoinRoom", "room": "table-42", "name": "Ada"}"#,
        )
        .expect("valid JoinRoom");
        assert_eq!(join, ClientToServer::JoinRoom { room: "table-42".into(), name: "Ada".into() });

        let play: ClientToServer = serde_json::from_str(
            r#"{"type": "PlayTile", "tile": {"left": 3, "right": 4}, "side": "right"}"#,
        )
        .expect("valid PlayTile");
        assert_eq!(
            play,
            ClientToServer::PlayTile {
                tile: PublicTile { left: 3, right: 4 },
                side: Side::Right,
            }
        );

        let pass: ClientToServer = serde_json::from_str(r#"{"type": "PassTurn"}"#).expect("valid PassTurn");
        assert_eq!(pass, ClientToServer::PassTurn);
    }

    #[test]
    fn outbound_messages_serialize_with_type_tags() {
        let replay = ServerToClient::MoveRejected { reason: "it is not your turn".into() };
        let json = serde_json::to_string(&replay).expect("serializable");
        assert_eq!(json, r#"{"type":"MoveRejected","reason":"it is not your turn"}"#);

        let bye = ServerToClient::OpponentDisconnected { player: "Bob".into(), seat: 2 };
        let json = serde_json::to_string(&bye).expect("serializable");
        assert_eq!(json, r#"{"type":"OpponentDisconnected","player":"Bob","seat":2}"#);
    }

    #[test]
    fn public_tiles_convert_faithfully_and_reject_out_of_range_pips() {
        let tile = game::Tile::new(2, 5);
        let public = PublicTile::from(tile);
        assert_eq!(public, PublicTile { left: 2, right: 5 });
        assert_eq!(game::Tile::try_from(public).expect("in range").normalized(), tile);

        let bad = PublicTile { left: 7, right: 3 };
        assert_eq!(
            game::Tile::try_from(bad).unwrap_err(),
            "pip values must be between 0 and 6"
        );
    }

    #[test]
    fn game_state_view_hides_opponent_hands() {
        let tiles = game::Tile::full_set();
        let state = game_state_view(&GameState::new(tiles));
        assert_eq!(state.hand_counts, [7, 7, 7, 7]);
        assert!(state.board.is_empty());
        assert!(state.left_end.is_none() && state.right_end.is_none());
        assert_eq!(state.consecutive_passes, 0);
    }
}

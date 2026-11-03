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
//! {"type": "PlayAgain"}
//! ```
//!
//! ```json
//! {"type": "RoomState", "room": {"room_id": "table-42", "you": 0, "phase": "playing", "seats": [...]}}
//! {"type": "HandDealt", "seat": 0, "hand": [{"left": 6, "right": 6}, ...]}
//! {"type": "GameStateUpdate", "state": {"turn": 1, "board": [...], ...}}
//! {"type": "GameClosed", "hands": [{"seat": 0, "hand": [...]}, ...]}
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
    /// "Play again": this seated player is ready for the next game. When a
    /// full table of four ready seats shares a finished (or absent) game,
    /// the room deals a fresh one in place — no socket cycling, so no seat
    /// can be held hostage by a stale connection.
    PlayAgain,
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
    /// The game closed: every seat passed in a row, so nobody can put a tile.
    /// Broadcast to all seats right after the terminating `GameStateUpdate`,
    /// with every player's remaining tiles revealed to everyone.
    GameClosed {
        hands: Vec<ClosedHand>,
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
    /// Whether this player has tapped "Play again" since the last deal.
    pub ready: bool,
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
/// actual tiles are hidden information while the game is live — only the
/// per-seat tile counts are exposed — and they leave the server exactly once:
/// in the `GameClosed` reveal of a blocked game.
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

/// One seat's remaining tiles, made public when the game closes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosedHand {
    pub seat: u8,
    pub hand: Vec<PublicTile>,
}

/// The reveal broadcast with `GameClosed`: every seat's remaining tiles, in
/// seat order. Only legitimate once the game is finished (closed); while a
/// game is live this would leak hidden information.
pub fn closed_hands_view(game: &GameState) -> Vec<ClosedHand> {
    (0..SEAT_COUNT)
        .map(|seat| ClosedHand {
            seat: seat as u8,
            hand: game.hand(seat).iter().copied().map(Into::into).collect(),
        })
        .collect()
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

        let again: ClientToServer = serde_json::from_str(r#"{"type": "PlayAgain"}"#).expect("valid PlayAgain");
        assert_eq!(again, ClientToServer::PlayAgain);
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

    #[test]
    fn game_closed_reveals_every_seat_hand_in_seat_order() {
        let tiles = game::Tile::full_set();
        let hands = closed_hands_view(&GameState::new(tiles));
        assert_eq!(hands.len(), SEAT_COUNT);
        for (seat, entry) in hands.iter().enumerate() {
            assert_eq!(entry.seat as usize, seat, "the reveal is seat-ordered");
            assert_eq!(entry.hand.len(), game::HAND_SIZE, "a full hand is seven tiles");
        }
    }

    #[test]
    fn game_closed_message_travels_with_seat_hands() {
        let msg = ServerToClient::GameClosed {
            hands: vec![ClosedHand { seat: 2, hand: vec![PublicTile { left: 0, right: 0 }] }],
        };
        let json = serde_json::to_string(&msg).expect("serializable");
        assert_eq!(json, r#"{"type":"GameClosed","hands":[{"seat":2,"hand":[{"left":0,"right":0}]}]}"#);
    }

    #[test]
    fn view_indexes_turn_and_counts_by_absolute_seat() {
        // A deterministic ordered deal: slots 0..7 of the deck go to seat 0,
        // and so on. Seat 0 holds the highest double (6|6), so it opens;
        // after its move the count drop must show up at `hand_counts[0]`
        // and the turn at seat 1 — never shifted by who acted.
        fn hand7(tiles: &[(u8, u8)]) -> Vec<game::Tile> {
            tiles.iter().map(|&(a, b)| game::Tile::new(a, b)).collect()
        }
        let mut tiles: Vec<game::Tile> = Vec::with_capacity(game::TILE_COUNT);
        tiles.extend(hand7(&[(6, 6), (0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (0, 6)]));
        tiles.extend(hand7(&[(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (2, 2)]));
        tiles.extend(hand7(&[(2, 3), (2, 4), (2, 5), (2, 6), (3, 3), (3, 4), (3, 5)]));
        tiles.extend(hand7(&[(0, 0), (3, 6), (4, 4), (4, 5), (4, 6), (5, 5), (5, 6)]));
        assert_eq!(tiles.len(), game::TILE_COUNT);
        let mut game = GameState::new(tiles);
        assert_eq!(game_state_view(&game).turn, 0, "seat 0 holds 6|6");

        game.play(0, game::Tile::new(6, 6), game::Side::Right).expect("opens");
        let view = game_state_view(&game);
        assert_eq!(view.turn, 1, "turn sits at the absolute next seat");
        assert_eq!(view.hand_counts, [6, 7, 7, 7], "only seat 0 lost a tile");
        assert_eq!((view.left_end, view.right_end), (Some(6), Some(6)));
        assert_eq!(view.board.len(), 1);
    }

    #[test]
    fn result_winner_travels_as_a_seat_number_on_the_wire() {
        let json = serde_json::to_value(GameResult { winner: Some(3), reason: game::EndReason::Blocked })
            .expect("serializable");
        assert_eq!(json, serde_json::json!({ "winner": 3, "reason": "blocked" }));
        let draw = serde_json::to_value(GameResult { winner: None, reason: game::EndReason::Abandoned })
            .expect("serializable");
        assert_eq!(draw, serde_json::json!({ "winner": null, "reason": "abandoned" }));
    }
}

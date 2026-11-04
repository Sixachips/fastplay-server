//! # Dominoes engine
//!
//! Pure game logic for a 4-player double-six game of dominoes. No networking,
//! no async, no shared state: [`GameState`] is a plain value owned by the room
//! layer (`crate::lobby`), which mutates it through the two command entry
//! points [`GameState::play`] and [`GameState::pass`].
//!
//! Rules implemented here:
//! * 28-tile double-six set, 7 tiles dealt to each of 4 seats (the boneyard is
//!   empty, so passing is the only alternative to playing).
//! * A single linear board ("the train") with a left and a right end. The
//!   first tile played sets both ends and the requested side is ignored.
//! * The first hand of a session is opened by the seat holding the 6|6, which
//!   must lay that tile down to start the layout. Every later hand is opened
//!   by the previous hand's winner (the fewest-pips seat of a blocked hand,
//!   or the next seat counter-clockwise when a blocked hand is tied), who may
//!   open with any tile of their choice.
//! * A seat that cannot play must pass. Once every seat has passed in a row
//!   the game is blocked and the seat with the fewest pips in hand wins
//!   (a tie is a draw).

use std::fmt;

use serde::{Deserialize, Serialize};

/// Highest pip value on either half of a tile (double-six set).
pub const MAX_PIP: u8 = 6;
/// Number of player seats at a table.
pub const SEAT_COUNT: usize = 4;
/// Tiles dealt to each player at the start of a game.
pub const HAND_SIZE: usize = 7;
/// Tiles in the full double-six set.
pub const TILE_COUNT: usize = 28;

/// A seat at the table, `0..=3`.
pub type Seat = usize;

/// A domino. The unordered pair identifies the physical tile, so
/// `Tile::new(3, 4) == Tile::new(4, 3)`; the struct is kept normalized
/// (`a <= b`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Tile {
    pub a: u8,
    pub b: u8,
}

impl Tile {
    /// Builds a tile, normalizing the pip order.
    ///
    /// # Panics
    /// Only for pip values above `MAX_PIP`. Untrusted input (anything coming
    /// off the wire) must go through [`Tile::try_new`] instead.
    pub fn new(a: u8, b: u8) -> Self {
        assert!(a <= MAX_PIP && b <= MAX_PIP, "pip value out of range for a double-six set");
        if a <= b {
            Self { a, b }
        } else {
            Self { a: b, b: a }
        }
    }

    /// Like [`Tile::new`], but returns `None` instead of panicking when a pip
    /// value is outside the double-six range. This is the only constructor
    /// that may be fed values parsed from client messages.
    pub fn try_new(a: u8, b: u8) -> Option<Self> {
        if a <= MAX_PIP && b <= MAX_PIP {
            Some(if a <= b { Self { a, b } } else { Self { a: b, b: a } })
        } else {
            None
        }
    }

    /// Canonical form of this tile (both halves sorted ascending).
    pub fn normalized(self) -> Self {
        Self::new(self.a, self.b)
    }

    /// Total pip count of the tile.
    pub fn pips(self) -> u8 {
        self.a + self.b
    }

    /// Whether both halves carry the same pip value.
    pub fn is_double(self) -> bool {
        self.a == self.b
    }

    /// Whether the tile can be legally attached to an end showing `end`.
    pub fn matches(self, end: u8) -> bool {
        self.a == end || self.b == end
    }

    /// The complete double-six set (28 tiles).
    pub fn full_set() -> Vec<Tile> {
        let mut tiles = Vec::with_capacity(TILE_COUNT);
        for a in 0..=MAX_PIP {
            for b in 0..=a {
                tiles.push(Self::new(a, b));
            }
        }
        tiles
    }
}

/// Which end of the linear board a tile is placed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Left,
    Right,
}

/// A player's position around the square table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableSide {
    North,
    East,
    South,
    West,
}

/// The linear layout of played tiles. Each tile is stored in orientation:
/// `.a` is the pip facing left, `.b` the pip facing right, so the chain reads
/// naturally left to right.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Board {
    chain: Vec<Tile>,
}

impl Board {
    pub fn is_empty(&self) -> bool {
        self.chain.is_empty()
    }

    /// Pip facing the outside of the left end (`None` before the first tile).
    pub fn left_end(&self) -> Option<u8> {
        self.chain.first().map(|t| t.a)
    }

    /// Pip facing the outside of the right end (`None` before the first tile).
    pub fn right_end(&self) -> Option<u8> {
        self.chain.last().map(|t| t.b)
    }

    pub fn ends(&self) -> Option<(u8, u8)> {
        Some((self.left_end()?, self.right_end()?))
    }

    /// The played tiles, left to right, in fixed orientation.
    pub fn chain(&self) -> &[Tile] {
        &self.chain
    }

    pub fn len(&self) -> usize {
        self.chain.len()
    }

    /// Whether `tile` can legally be placed at `side` right now.
    pub fn fits(&self, side: Side, tile: Tile) -> bool {
        match self.ends() {
            None => true,
            Some((left, right)) => {
                let end = match side {
                    Side::Left => left,
                    Side::Right => right,
                };
                tile.matches(end)
            }
        }
    }

    /// Places an already normalized `tile` at `side`, re-orienting it so the
    /// chain stays consistent. An empty board accepts any tile and ignores
    /// `side`. Returns `None` if the tile does not fit the requested end.
    pub fn play(&mut self, side: Side, tile: Tile) -> Option<()> {
        if self.chain.is_empty() {
            self.chain.push(tile);
            return Some(());
        }
        match side {
            Side::Left => {
                let end = self.left_end().expect("board is not empty");
                if !tile.matches(end) {
                    return None;
                }
                // The pip matching `end` must end up on the tile's right side.
                let oriented = if tile.b == end { tile } else { Tile { a: tile.b, b: tile.a } };
                self.chain.insert(0, oriented);
            }
            Side::Right => {
                let end = self.right_end().expect("board is not empty");
                if !tile.matches(end) {
                    return None;
                }
                // The pip matching `end` must end up on the tile's left side.
                let oriented = if tile.a == end { tile } else { Tile { a: tile.b, b: tile.a } };
                self.chain.push(oriented);
            }
        }
        Some(())
    }
}

/// How a game ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    /// A seat emptied its hand.
    HandEmpty,
    /// Every seat passed in a row; the game is blocked.
    Blocked,
    /// A seat left the table mid-game and the remaining lobby would stall, so
    /// the game was voided rather than simulated for the empty seat.
    Abandoned,
}

/// The final result of a game.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GameResult {
    /// Winning seat, or `None` for a draw (e.g. a blocked game with tied pips).
    pub winner: Option<Seat>,
    pub reason: EndReason,
}

impl GameResult {
    /// Whether the game closed: it ended blocked, with no seat able to put a
    /// tile. A closed game reveals every remaining hand to every player.
    pub fn is_closed(self) -> bool {
        self.reason == EndReason::Blocked
    }
}

/// Why a command was refused by the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MoveError {
    /// No game is being played in this room (or it already ended).
    GameNotActive,
    /// It is another seat's turn.
    NotYourTurn,
    /// The tile is not in the acting seat's hand.
    TileNotInHand,
    /// The tile does not match the requested end of the board.
    TileDoesNotFit,
    /// The board is empty and this hand must be opened with its forced tile
    /// (the 6|6 on the first hand of a session).
    WrongOpeningTile,
    /// The seat has at least one legal placement and therefore cannot pass.
    IllegalPass,
}

/// What happened after an accepted command.
impl fmt::Display for MoveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            MoveError::GameNotActive => "the game is not active",
            MoveError::NotYourTurn => "it is not your turn",
            MoveError::TileNotInHand => "you do not hold that tile",
            MoveError::TileDoesNotFit => "that tile does not fit the requested end",
            MoveError::WrongOpeningTile => "this hand must be opened with the 6|6 tile",
            MoveError::IllegalPass => "you have a legal move and cannot pass",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Play continues; it is now another seat's turn.
    Continued,
    /// The game has finished; the result is also observable via
    /// [`GameState::result`].
    Ended(GameResult),
}

/// The complete, authoritative state of one running game.
#[derive(Debug, Clone)]
pub struct GameState {
    hands: Vec<Vec<Tile>>,
    board: Board,
    turn: Seat,
    /// Seat that took the opening turn of this hand.
    opener: Seat,
    /// While the board is empty, the only tile the opener may lay. `Some` on
    /// the first hand of a session (the 6|6 must start the layout); `None`
    /// for every later hand, which opens with any tile.
    opening_tile: Option<Tile>,
    passes_in_row: usize,
    result: Option<GameResult>,
}

impl GameState {
    /// Deals a full double-six set to the four seats. `opener` takes the first
    /// turn and may open with any tile of its choice: the rule for every hand
    /// after the first of a session.
    ///
    /// # Panics
    /// If `tiles` is not a full 28-tile set or `opener` is not a seat.
    pub fn new(tiles: Vec<Tile>, opener: Seat) -> Self {
        Self::deal(tiles, opener, None)
    }

    /// Deals the first hand of a session: the seat holding the **Double-Six**
    /// opens, and must lay that tile down to start the layout.
    ///
    /// # Panics
    /// If `tiles` is not a full 28-tile set (which always contains the 6|6).
    pub fn first_hand(tiles: Vec<Tile>) -> Self {
        let hands = dealt_hands(&tiles);
        let double_six = Tile::new(MAX_PIP, MAX_PIP);
        let opener = hands
            .iter()
            .position(|hand| hand.contains(&double_six))
            .expect("a full double-six set always contains the 6|6");
        Self::with_hands(hands, opener, Some(double_six))
    }

    fn deal(tiles: Vec<Tile>, opener: Seat, opening_tile: Option<Tile>) -> Self {
        Self::with_hands(dealt_hands(&tiles), opener, opening_tile)
    }

    fn with_hands(hands: Vec<Vec<Tile>>, opener: Seat, opening_tile: Option<Tile>) -> Self {
        assert!(opener < SEAT_COUNT, "the opener must be a seat at the table");
        if let Some(tile) = opening_tile {
            assert!(
                hands[opener].contains(&tile),
                "the forced opening tile must be in the opener's hand"
            );
        }
        Self {
            hands,
            board: Board::default(),
            turn: opener,
            opener,
            opening_tile,
            passes_in_row: 0,
            result: None,
        }
    }

    /// Seat whose turn it currently is.
    pub fn turn(&self) -> Seat {
        self.turn
    }

    /// Seat that took the opening turn of this hand.
    pub fn opener(&self) -> Seat {
        self.opener
    }

    /// The forced opening tile while the board is still empty (only on the
    /// first hand of a session), otherwise `None`.
    pub fn opening_tile(&self) -> Option<Tile> {
        self.opening_tile
    }

    pub fn board(&self) -> &Board {
        &self.board
    }

    /// The acting seat's private tiles.
    pub fn hand(&self, seat: Seat) -> &[Tile] {
        &self.hands[seat]
    }

    pub fn hand_counts(&self) -> [usize; SEAT_COUNT] {
        let mut counts = [0usize; SEAT_COUNT];
        for (seat, hand) in self.hands.iter().enumerate() {
            counts[seat] = hand.len();
        }
        counts
    }

    /// Total pips still held per seat; used to break blocked games.
    pub fn pip_totals(&self) -> [u8; SEAT_COUNT] {
        let mut totals = [0u8; SEAT_COUNT];
        for (seat, hand) in self.hands.iter().enumerate() {
            totals[seat] = hand.iter().map(|t| t.pips()).sum();
        }
        totals
    }

    /// Consecutive passes just recorded by the engine.
    pub fn passes_in_row(&self) -> usize {
        self.passes_in_row
    }

    pub fn result(&self) -> Option<GameResult> {
        self.result
    }

    pub fn is_finished(&self) -> bool {
        self.result.is_some()
    }

    /// Every legal `(tile, side)` placement for `seat`. On an empty board every
    /// tile is legal and `Side::Right` is reported (the side is ignored there) —
    /// except on a forced opening, where only the required tile is legal.
    pub fn legal_moves(&self, seat: Seat) -> Vec<(Tile, Side)> {
        if self.is_finished() {
            return Vec::new();
        }
        if self.board.is_empty() {
            return match self.opening_tile {
                Some(required) if self.hands[seat].contains(&required) => {
                    vec![(required, Side::Right)]
                }
                Some(_) => Vec::new(),
                None => self.hands[seat].iter().copied().map(|t| (t, Side::Right)).collect(),
            };
        }
        let (left, right) = self.board.ends().expect("non-empty board has both ends");
        self.hands[seat]
            .iter()
            .copied()
            .filter(|t| t.matches(left) || t.matches(right))
            .flat_map(|t| {
                let mut placements = Vec::with_capacity(2);
                if t.matches(left) {
                    placements.push((t, Side::Left));
                }
                if t.matches(right) {
                    placements.push((t, Side::Right));
                }
                placements
            })
            .collect()
    }

    /// Whether `seat` has no legal placement and therefore must pass.
    pub fn must_pass(&self, seat: Seat) -> bool {
        self.legal_moves(seat).is_empty()
    }

    /// Plays `tile` of `seat` at `side`. The tile is matched against the
    /// unordered pip pair, so any orientation the client sends is accepted.
    pub fn play(&mut self, seat: Seat, tile: Tile, side: Side) -> Result<Outcome, MoveError> {
        if self.is_finished() {
            return Err(MoveError::GameNotActive);
        }
        if self.turn != seat {
            return Err(MoveError::NotYourTurn);
        }
        let tile = tile.normalized();
        let pos = match self.hands[seat].iter().position(|&t| t == tile) {
            Some(pos) => pos,
            None => return Err(MoveError::TileNotInHand),
        };
        let opening_move = self.board.is_empty();
        if let Some(required) = self.opening_tile {
            if opening_move && tile != required {
                return Err(MoveError::WrongOpeningTile);
            }
        }
        self.board
            .play(side, tile)
            .ok_or(MoveError::TileDoesNotFit)?;
        self.hands[seat].remove(pos);
        if opening_move {
            // The forced opening tile has been laid; the hand is open.
            self.opening_tile = None;
        }
        self.passes_in_row = 0;
        if self.hands[seat].is_empty() {
            let result = GameResult { winner: Some(seat), reason: EndReason::HandEmpty };
            self.result = Some(result);
            return Ok(Outcome::Ended(result));
        }
        self.turn = (self.turn + 1) % SEAT_COUNT;
        Ok(Outcome::Continued)
    }

    /// Records a pass for `seat`. Only legal when the seat has no placement.
    pub fn pass(&mut self, seat: Seat) -> Result<Outcome, MoveError> {
        if self.is_finished() {
            return Err(MoveError::GameNotActive);
        }
        if self.turn != seat {
            return Err(MoveError::NotYourTurn);
        }
        if !self.legal_moves(seat).is_empty() {
            return Err(MoveError::IllegalPass);
        }
        self.passes_in_row += 1;
        if self.passes_in_row >= SEAT_COUNT {
            let totals = self.pip_totals();
            let min = totals.iter().copied().min().expect("four seats exist");
            let tied = totals.iter().filter(|&&p| p == min).count();
            let winner = if tied == 1 {
                Some(totals.iter().position(|&p| p == min).expect("minimum exists"))
            } else {
                None
            };
            let result = GameResult { winner, reason: EndReason::Blocked };
            self.result = Some(result);
            return Ok(Outcome::Ended(result));
        }
        self.turn = (self.turn + 1) % SEAT_COUNT;
        Ok(Outcome::Continued)
    }

    /// Voids an in-progress game because a seat left the table. A game that
    /// already ended stays untouched; the caller can confirm with
    /// [`GameState::is_finished`].
    pub fn abandon(&mut self) -> Outcome {
        let result = GameResult { winner: None, reason: EndReason::Abandoned };
        self.result = Some(result);
        Outcome::Ended(result)
    }

    /// Like [`GameState::abandon`], but only when the game is still live.
    /// Returns whether the game actually ended now.
    pub fn abandon_if_live(&mut self) -> bool {
        if self.result.is_some() {
            return false;
        }
        self.abandon();
        true
    }
}

/// Splits a full 28-tile set into four sorted seven-tile hands, seat order.
fn dealt_hands(tiles: &[Tile]) -> Vec<Vec<Tile>> {
    assert!(tiles.len() == TILE_COUNT, "a double-six game starts from exactly {TILE_COUNT} tiles");
    let mut hands: Vec<Vec<Tile>> = (0..SEAT_COUNT)
        .map(|seat| tiles[seat * HAND_SIZE..(seat + 1) * HAND_SIZE].to_vec())
        .collect();
    for hand in &mut hands {
        hand.sort_by_key(|t| (t.a, t.b));
    }
    hands
}

/// Test-only opener picker: the holder of the highest double, else of the
/// highest-pip tile, with seat order breaking ties. Production openers are
/// chosen by the room layer (`lobby::deal_fresh_game`), not by the engine.
#[cfg(test)]
fn starter(hands: &[Vec<Tile>]) -> Seat {
    let mut best: Option<(Seat, Tile)> = None;
    for (seat, hand) in hands.iter().enumerate() {
        for &tile in hand {
            let better = match best {
                None => true,
                Some((_, current)) => {
                    (tile.is_double(), tile.pips()) > (current.is_double(), current.pips())
                }
            };
            if better {
                best = Some((seat, tile));
            }
        }
    }
    best.expect("every dealt hand is non-empty").0
}

#[cfg(test)]
impl GameState {
    /// Test-only constructor allowing arbitrary (even partial) hands so game
    /// situations can be staged deterministically.
    pub(crate) fn from_hands(hands: [Vec<Tile>; SEAT_COUNT]) -> Self {
        let turn = starter(&hands);
        Self {
            hands: hands.into(),
            board: Board::default(),
            turn,
            opener: turn,
            opening_tile: None,
            passes_in_row: 0,
            result: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rand::seq::SliceRandom;

    use super::*;

    #[test]
    fn full_set_has_28_unique_tiles() {
        let set = Tile::full_set();
        assert_eq!(set.len(), TILE_COUNT);
        let unique: HashSet<Tile> = set.iter().map(|t| t.normalized()).collect();
        assert_eq!(unique.len(), TILE_COUNT);
        assert!(set.iter().all(|t| t.a <= MAX_PIP && t.b <= MAX_PIP));
    }

    #[test]
    fn first_hand_deals_four_full_hands_and_forces_the_double_six() {
        let mut tiles = Tile::full_set();
        tiles.shuffle(&mut rand::thread_rng());
        let game = GameState::first_hand(tiles);
        let counts = game.hand_counts();
        assert!(counts.iter().all(|&c| c == HAND_SIZE));
        assert_eq!(counts.iter().sum::<usize>(), TILE_COUNT);
        // The 6|6 holder opens, and the 6|6 is the only tile that may be laid.
        let double_six = Tile::new(6, 6);
        assert_eq!(game.opening_tile(), Some(double_six));
        assert_eq!(game.opener(), game.turn());
        assert!(game.hand(game.turn()).contains(&double_six), "the 6|6 holder opens");
        assert_eq!(game.legal_moves(game.turn()), vec![(double_six, Side::Right)]);
        assert!(!game.is_finished());
    }

    #[test]
    fn later_hands_open_with_any_tile_at_the_named_seat() {
        // Every hand after the first: the room names the opener (the previous
        // hand's winner) and there is no forced opening tile.
        let mut tiles = Tile::full_set();
        tiles.shuffle(&mut rand::thread_rng());
        let game = GameState::new(tiles, 2);
        assert_eq!(game.turn(), 2);
        assert_eq!(game.opener(), 2);
        assert_eq!(game.opening_tile(), None);
        assert_eq!(game.legal_moves(2).len(), HAND_SIZE, "any held tile may open");
    }

    #[test]
    fn forced_opening_rejects_other_tiles_until_the_double_six_lands() {
        // `full_set()` deals seat 3 the tiles at indices 21..28, which include
        // the last-built 6|6; seat 3 therefore opens the first hand.
        let mut game = GameState::first_hand(Tile::full_set());
        assert_eq!(game.turn(), 3);
        let other = game.hand(3)[0];
        assert_ne!(other, Tile::new(6, 6));
        assert_eq!(
            game.play(3, other, Side::Right),
            Err(MoveError::WrongOpeningTile),
            "only the 6|6 may start the first hand"
        );
        assert_eq!(game.play(3, Tile::new(6, 6), Side::Right), Ok(Outcome::Continued));
        assert_eq!(game.opening_tile(), None, "the restriction lifts after the opening move");
        assert_eq!(game.turn(), 0, "the turn still walks one seat from the opener");
        assert_eq!(game.board().ends(), Some((6, 6)));
    }

    #[test]
    fn highest_double_opens_the_game() {
        let game = GameState::from_hands([
            vec![Tile::new(0, 1), Tile::new(0, 2)],
            vec![Tile::new(2, 3), Tile::new(1, 0)],
            vec![Tile::new(6, 6)],
            vec![Tile::new(5, 5)],
        ]);
        assert_eq!(game.turn(), 2);
    }

    #[test]
    fn highest_pip_tile_opens_when_no_double_exists() {
        let game = GameState::from_hands([
            vec![Tile::new(1, 2)],
            vec![Tile::new(3, 4)],
            vec![Tile::new(4, 5)],
            vec![],
        ]);
        // 4|5 totals 9, the highest pip count in play.
        assert_eq!(game.turn(), 2);
    }

    #[test]
    fn empty_board_accepts_any_tile_and_sets_the_ends() {
        let game = GameState::from_hands([
            vec![Tile::new(3, 4), Tile::new(0, 0)],
            vec![],
            vec![],
            vec![],
        ]);
        let mut game = game;
        let outcome = game.play(0, Tile::new(4, 3), Side::Left).expect("first move");
        assert_eq!(outcome, Outcome::Continued);
        assert_eq!(game.board().ends(), Some((3, 4)));
        assert_eq!(game.turn(), 1);

        // Wrong seat and unknown tile are rejected.
        assert_eq!(game.play(0, Tile::new(0, 0), Side::Right), Err(MoveError::NotYourTurn));
        assert_eq!(game.play(1, Tile::new(5, 5), Side::Right), Err(MoveError::TileNotInHand));
    }

    #[test]
    fn tile_must_match_the_requested_end() {
        // Seat 0 opens (it holds the highest double); seat 1 then has no
        // matching tile for either end until the 4|6 arrives.
        let mut game = GameState::from_hands([
            vec![Tile::new(3, 4), Tile::new(5, 5)],
            vec![Tile::new(5, 6), Tile::new(4, 6)],
            vec![],
            vec![],
        ]);
        game.play(0, Tile::new(3, 4), Side::Right).expect("first move");
        // 5|6 fits neither the left (3) nor the right (4) end.
        assert_eq!(game.play(1, Tile::new(5, 6), Side::Left), Err(MoveError::TileDoesNotFit));
        assert_eq!(game.play(1, Tile::new(5, 6), Side::Right), Err(MoveError::TileDoesNotFit));
        // A matching tile is accepted on the side that fits.
        assert_eq!(game.play(1, Tile::new(4, 6).normalized(), Side::Right), Ok(Outcome::Continued));
        assert_eq!(game.turn(), 2);
    }

    #[test]
    fn illegal_pass_is_rejected_while_a_placement_exists() {
        let mut game = GameState::from_hands([
            vec![Tile::new(6, 6), Tile::new(6, 0), Tile::new(0, 1)],
            vec![],
            vec![],
            vec![],
        ]);
        assert_eq!(game.play(0, Tile::new(6, 6), Side::Right).expect("first move"), Outcome::Continued);
        // Seat 1 has an empty hand and may pass.
        assert_eq!(game.pass(1).expect("legal pass"), Outcome::Continued);
        assert_eq!(game.passes_in_row(), 1);
        // Seats 2 and 3 also empty; then around to seat 0 who holds 6|0
        // matching both ends (the board is just the double) and therefore
        // cannot pass.
        game.pass(2).expect("legal pass");
        game.pass(3).expect("legal pass");
        assert_eq!(game.pass(0), Err(MoveError::IllegalPass));
        // But the placement itself is accepted, resetting the pass counter.
        // The seat keeps a third tile, so the game does not end yet.
        assert_eq!(game.play(0, Tile::new(6, 0), Side::Left), Ok(Outcome::Continued));
        assert_eq!(game.passes_in_row(), 0);
    }

    #[test]
    fn blocked_game_resolves_by_lowest_pip_count() {
        // Seat 0 keeps a spare 0|0 so playing the double does not empty its
        // hand; the game must end by blocking, not by dominoing out.
        let mut game = GameState::from_hands([
            vec![Tile::new(6, 6), Tile::new(0, 0)],
            vec![Tile::new(1, 1)],
            vec![Tile::new(2, 2)],
            vec![Tile::new(3, 3)],
        ]);
        game.play(0, Tile::new(6, 6), Side::Right).expect("first move");
        assert_eq!(game.pass(1).expect("no six in hand"), Outcome::Continued);
        assert_eq!(game.pass(2).expect("no six in hand"), Outcome::Continued);
        assert_eq!(game.pass(3).expect("no six in hand"), Outcome::Continued);
        // Next around to seat 0, whose remaining 0|0 matches neither end (0
        // and 0 vs. ends 6 and 6 — no six anywhere else at the table), so it
        // must pass as well: the fourth consecutive pass blocks the game and
        // the empty-ish hand wins with 0 pips.
        let blocked = game.pass(0).expect("fourth pass ends the game");
        assert_eq!(
            blocked,
            Outcome::Ended(GameResult { winner: Some(0), reason: EndReason::Blocked })
        );
        assert!(game.is_finished());
        assert_eq!(game.play(1, Tile::new(1, 1), Side::Right), Err(MoveError::GameNotActive));
        assert_eq!(game.pass(1), Err(MoveError::GameNotActive));
    }

    #[test]
    fn blocked_game_with_tied_pips_is_a_draw() {
        let mut game = GameState::from_hands([
            vec![Tile::new(6, 6), Tile::new(0, 2)],
            vec![Tile::new(1, 1)],
            vec![Tile::new(1, 2)],
            vec![Tile::new(3, 3)],
        ]);
        game.play(0, Tile::new(6, 6), Side::Right).expect("first move");
        for seat in 1..SEAT_COUNT {
            game.pass(seat).expect("no pip-6 tiles remain in the other hands");
        }
        // Fourth consecutive pass blocks the game; seats 0 and 1 both hold 2
        // pips, the shared minimum, so the game is a draw.
        let draw = game.pass(0).expect("fourth pass ends the game");
        assert_eq!(draw, Outcome::Ended(GameResult { winner: None, reason: EndReason::Blocked }));
    }

    #[test]
    fn clearing_the_hand_wins_the_game() {
        // A self-draining chain: every next tile matches the growing right end.
        let chain = [
            Tile::new(0, 0),
            Tile::new(0, 1),
            Tile::new(1, 2),
            Tile::new(2, 3),
            Tile::new(3, 4),
            Tile::new(4, 5),
            Tile::new(5, 6),
        ];
        let mut game = GameState::from_hands([chain.to_vec(), vec![], vec![], vec![]]);
        for (round, expected) in chain.iter().enumerate() {
            // Seats 1..3 all have empty hands and legitimately pass between
            // seat 0's consecutive turns (not before the opening move).
            if round > 0 {
                for seat in 1..SEAT_COUNT {
                    assert!(game.must_pass(seat));
                    game.pass(seat).expect("empty hands pass");
                }
            }
            // The engine sanctions the expected placement on the right end
            // (on an empty board every tile reports `Side::Right`).
            assert!(
                game.legal_moves(0).contains(&(*expected, Side::Right)),
                "expected the placement of {expected:?} to be legal in round {round}"
            );
            let outcome = game.play(0, *expected, Side::Right).expect("engine-sanctioned move");
            if round < HAND_SIZE - 1 {
                assert_eq!(outcome, Outcome::Continued);
            } else {
                assert_eq!(
                    outcome,
                    Outcome::Ended(GameResult { winner: Some(0), reason: EndReason::HandEmpty })
                );
            }
        }
        assert!(game.is_finished());
    }

    /// Snapshot of everything a rejected command must leave untouched.
    #[derive(Debug, PartialEq)]
    struct Fingerprint {
        turn: Seat,
        chain: Vec<Tile>,
        counts: [usize; SEAT_COUNT],
        hands: [Vec<Tile>; SEAT_COUNT],
        passes_in_row: usize,
        result: Option<GameResult>,
    }

    impl Fingerprint {
        fn of(game: &GameState) -> Self {
            Self {
                turn: game.turn(),
                chain: game.board().chain().to_vec(),
                counts: game.hand_counts(),
                hands: std::array::from_fn(|seat| game.hand(seat).to_vec()),
                passes_in_row: game.passes_in_row(),
                result: game.result(),
            }
        }
    }

    #[test]
    fn rejected_commands_leave_every_field_untouched() {
        // Seat 0 opens 6|6 (keeping a spare 0|0 so the game continues); seat 1
        // then holds one fitting tile (6|5) and one that fits neither end
        // (1|2). Every guard below runs while it is seat 1's turn.
        let mut game = GameState::from_hands([
            vec![Tile::new(6, 6), Tile::new(0, 0)],
            vec![Tile::new(6, 5), Tile::new(1, 2)],
            vec![Tile::new(1, 1), Tile::new(1, 5)],
            vec![Tile::new(2, 2), Tile::new(2, 4)],
        ]);
        game.play(0, Tile::new(6, 6), Side::Right).expect("opening move");
        assert_eq!(game.turn(), 1);
        let before = Fingerprint::of(&game);

        assert_eq!(game.play(0, Tile::new(6, 5), Side::Left), Err(MoveError::NotYourTurn), "the previous actor is not up again");
        assert_eq!(game.play(2, Tile::new(1, 5), Side::Right), Err(MoveError::NotYourTurn));
        assert_eq!(game.play(3, Tile::new(2, 4), Side::Right), Err(MoveError::NotYourTurn));
        assert_eq!(
            game.play(1, Tile::new(1, 1), Side::Right),
            Err(MoveError::TileNotInHand),
            "1|1 belongs to seat 2; the acting seat's hand is the only source"
        );
        assert_eq!(game.play(1, Tile::new(1, 2), Side::Left), Err(MoveError::TileDoesNotFit));
        assert_eq!(game.play(1, Tile::new(1, 2), Side::Right), Err(MoveError::TileDoesNotFit));
        assert_eq!(game.play(1, Tile::new(3, 3), Side::Right), Err(MoveError::TileNotInHand));
        assert_eq!(game.pass(1), Err(MoveError::IllegalPass), "6|5 fits the left end");
        assert_eq!(game.pass(0), Err(MoveError::NotYourTurn));
        assert_eq!(game.pass(2), Err(MoveError::NotYourTurn));
        assert_eq!(game.pass(3), Err(MoveError::NotYourTurn));
        assert_eq!(Fingerprint::of(&game), before, "every rejected command was inert");

        // A finished game rejects both commands for anyone, including the
        // winner who just played out.
        let mut over = GameState::from_hands([
            vec![Tile::new(6, 6)],
            vec![Tile::new(1, 1)],
            vec![],
            vec![],
        ]);
        assert_eq!(
            over.play(0, Tile::new(6, 6), Side::Right),
            Ok(Outcome::Ended(GameResult { winner: Some(0), reason: EndReason::HandEmpty }))
        );
        assert_eq!(over.play(0, Tile::new(6, 6), Side::Right), Err(MoveError::GameNotActive));
        assert_eq!(over.play(1, Tile::new(1, 1), Side::Right), Err(MoveError::GameNotActive));
        assert_eq!(over.pass(1), Err(MoveError::GameNotActive));
    }

    #[test]
    fn staged_hands_break_starter_ties_deterministically() {
        // Equal-pip non-doubles: the lower seat opens, no matter which side
        // of the tie the tiles sit on.
        let a = GameState::from_hands([
            vec![Tile::new(1, 6)],
            vec![Tile::new(0, 1)],
            vec![Tile::new(0, 2)],
            vec![Tile::new(2, 5)],
        ]);
        assert_eq!(a.turn(), 0);
        let b = GameState::from_hands([
            vec![Tile::new(2, 5)],
            vec![Tile::new(0, 1)],
            vec![Tile::new(0, 2)],
            vec![Tile::new(1, 6)],
        ]);
        assert_eq!(b.turn(), 0);
        // Any double outranks a higher-pip non-double, per the staged helper.
        let c = GameState::from_hands([
            vec![Tile::new(6, 5)],
            vec![],
            vec![Tile::new(1, 1)],
            vec![],
        ]);
        assert_eq!(c.turn(), 2);
        // The same deck dealt twice opens the first hand the same way.
        let mut deck = Tile::full_set();
        deck.shuffle(&mut rand::thread_rng());
        let first = GameState::first_hand(deck.clone());
        assert_eq!(first.opener(), first.turn());
        assert!(first.hand(first.turn()).contains(&Tile::new(6, 6)));
        assert_eq!(GameState::first_hand(deck).turn(), first.turn());
    }

    #[test]
    fn every_step_walks_the_turn_exactly_one_seat_counter_clockwise() {
        // With the lobby's seat→position mapping (0=North, 1=West, 2=South,
        // 3=East) the +1 seat walk is a counter-clockwise lap of the table:
        // every seat hands the turn to its right-hand neighbour.
        // Scripted full cycle: seat 0 opens 6|6; every hand is six-blind
        // except seat 3's 5|6 (one extra placement). The turn must walk
        // 0→1→2→3→0→1→2→3 with no seat skipped or revisited, ending blocked
        // with the lowest pips at the ABSOLUTE seat 3 — a winner taken from
        // the pass order (3,0,1,2 → index 0) or from an occupied-seat list
        // would report something else here.
        let mut game = GameState::from_hands([
            vec![Tile::new(6, 6), Tile::new(1, 1), Tile::new(0, 1)], // 3 pips left after opening
            vec![Tile::new(2, 2), Tile::new(1, 2)],                  // 7 pips
            vec![Tile::new(3, 3), Tile::new(4, 4)],                  // 14 pips
            vec![Tile::new(0, 0), Tile::new(6, 5)],                  // 0 pips after the 5|6 lands
        ]);
        let start_tiles: usize = game.hand_counts().iter().sum();
        assert_eq!(game.turn(), 0, "6|6 is the highest double");

        game.play(0, Tile::new(6, 6), Side::Right).expect("opens");
        assert_eq!(game.turn(), 1);
        // Seats out of step are refused, including seat 0 (previous actor)
        // and seats that simply try to get ahead of the queue.
        assert_eq!(game.play(3, Tile::new(6, 5), Side::Left), Err(MoveError::NotYourTurn));
        for seat in [0, 2, 3] {
            assert_eq!(game.pass(seat), Err(MoveError::NotYourTurn), "seat {seat} tried early");
        }

        // Walk the table strictly +1 mod 4: passes for seats 1 and 2, the
        // legal play for seat 3, then four consecutive passes to the block.
        assert_eq!(game.pass(1).expect("six-blind hand"), Outcome::Continued);
        assert_eq!(game.turn(), 2);
        assert_eq!(game.pass(2).expect("six-blind hand"), Outcome::Continued);
        assert_eq!(game.turn(), 3);
        assert_eq!(game.play(3, Tile::new(6, 5), Side::Left), Ok(Outcome::Continued));
        assert_eq!(game.turn(), 0);
        assert_eq!(game.passes_in_row(), 0, "any play resets the pass streak");
        for seat in 0..3 {
            assert_eq!(game.pass(seat).expect("no 5 or 6 fits any remaining hand"), Outcome::Continued);
            assert_eq!(game.turn(), (seat + 1) % SEAT_COUNT, "seat {seat} passes to its right-hand neighbour");
        }
        // The fourth consecutive pass — by seat 3, completing the full circle —
        // blocks the game.
        assert_eq!(
            game.pass(3).expect("fourth consecutive pass"),
            Outcome::Ended(GameResult { winner: Some(3), reason: EndReason::Blocked })
        );

        // The blocked winner is the absolute seat 3 (0 pips vs 3/14/14):
        // pip_totals is seat-indexed, so `position` == seat here.
        assert_eq!(
            game.result(),
            Some(GameResult { winner: Some(3), reason: EndReason::Blocked })
        );
        // Conservation: nothing left the table unaccounted for, and the
        // seat-indexed counts match the hidden hands exactly after every step.
        assert_eq!(game.hand_counts(), [2, 2, 2, 1]);
        assert_eq!(start_tiles, game.hand_counts().iter().sum::<usize>() + game.board().len());
        assert_eq!(game.turn(), 3, "turn freezes at the seat that sealed the block");
        assert_eq!(game.play(3, Tile::new(0, 0), Side::Right), Err(MoveError::GameNotActive));
        assert_eq!(game.pass(3), Err(MoveError::GameNotActive));
    }

    #[test]
    fn blocked_winner_is_a_seat_number_even_when_the_opener_passes_last() {
        // The opener (seat 1) also makes the sealing fourth pass, so the
        // pip-minimum seat (0) differs from both its pass-order position
        // (second passer, would report 2) and any occupied-list index.
        let mut game = GameState::from_hands([
            vec![Tile::new(0, 0)],                                     // 0 pips, but NOT the opener
            vec![Tile::new(6, 6), Tile::new(1, 2)],                    // opener, 3 pips left
            vec![Tile::new(2, 2)],
            vec![Tile::new(3, 3)],
        ]);
        assert_eq!(game.turn(), 1);
        game.play(1, Tile::new(6, 6), Side::Right).expect("opens");
        for (order, seat) in [2usize, 3, 0, 1].into_iter().enumerate() {
            let outcome = game.pass(seat).expect("no six outside the played 6|6");
            if order == 3 {
                assert_eq!(outcome, Outcome::Ended(GameResult { winner: Some(0), reason: EndReason::Blocked }));
            } else {
                assert_eq!(outcome, Outcome::Continued);
                assert_eq!(game.turn(), (seat + 1) % SEAT_COUNT);
            }
        }
    }

    #[test]
    fn board_reorients_tiles_and_keeps_ends_consistent() {
        let mut board = Board::default();
        board.play(Side::Right, Tile::new(3, 4)).expect("empty board");
        board.play(Side::Left, Tile::new(3, 5)).expect("matches 3");
        board.play(Side::Right, Tile::new(2, 4)).expect("matches 4");
        // Chain should read 5|3 3|4 4|2 with ends 5 and 2. Orientation:
        // `.a` is the pip facing left (outward on the leftmost tile).
        assert_eq!(board.left_end(), Some(5));
        assert_eq!(board.right_end(), Some(2));
        assert_eq!(
            board.chain(),
            &[Tile { a: 5, b: 3 }, Tile { a: 3, b: 4 }, Tile { a: 4, b: 2 }]
        );
        assert!(board.fits(Side::Left, Tile::new(5, 1)));
        assert!(board.fits(Side::Right, Tile::new(1, 2)));
        assert!(!board.fits(Side::Left, Tile::new(6, 6)));
    }
}

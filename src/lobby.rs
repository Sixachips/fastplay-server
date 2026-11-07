//! # Thread-safe room / lobby management
//!
//! [`Lobby`] owns the shared routing tables — the registry of live rooms and
//! the per-connection bookkeeping — in [`DashMap`]s (sharded, cheap lookups),
//! while each [`Room`] serializes every mutation of its seats and game behind
//! an async [`tokio::sync::Mutex`]. Nothing here blocks the executor: room
//! critical sections only await on the room's own mutex, never on another
//! room's, and all event fan-out goes through `tokio::sync::mpsc` *unbounded*
//! channels whose `send` is a synchronous, non-blocking call — so fan-out
//! order == commit order, yet a slow or dead client can never stall the room.
//!
//! Concurrency contracts:
//! * Connection identities are server-issued u64s ([`PlayerId`]); clients can
//!   never claim another seat.
//! * Room creation is atomic via [`DashMap::entry`]; racing joiners on one
//!   room code land on one and the same [`Room`].
//! * Deals, moves, wins and aborts all happen inside the room lock, so every
//!   seat observes a totally ordered event stream for its room.
//! * A room mutex is never held while acquiring another room's mutex or any
//!   other global lock, ruling out deadlocks by construction.
//! * Empty rooms are pruned from the registry under pointer equality
//!   ([`DashMap::remove_if`]), so an abandoned room never outlives its last
//!   seat.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use rand::{seq::SliceRandom, thread_rng};
use tokio::sync::{mpsc, Mutex};

use crate::game::{
    GameState, MatchScore, MoveError, Outcome, Seat, SEAT_COUNT, Side, TableSide, Team, Tile,
    team_of, team_seats,
};
use crate::protocol::{
    closed_hands_view, game_state_view, parse_team, ClientToServer, PublicRoomState, PublicSeat,
    RoomPhase, ServerToClient,
};

/// A client's connection identity. Issued by [`Lobby::next_player_id`] when
/// the socket is wired up and used to authenticate every following command.
pub type PlayerId = u64;

/// Wire constraints protecting the server from unbounded client strings.
pub const MAX_ROOM_CODE_LEN: usize = 16;
pub const MAX_PLAYER_NAME_LEN: usize = 32;

/// Per-connection outbound queue. Unbounded (a slow peer must never stall a
/// room); `send` is synchronous and cannot meaningfully fail — a dangling
/// receiver simply drops the event, and [`Lobby::handle_disconnect`] provides
/// the authoritative leave path anyway.
type Outbox = mpsc::UnboundedSender<ServerToClient>;

#[derive(Debug)]
struct Connection {
    outbox: Outbox,
    /// Room code the connection is seated in (`None` until `JoinRoom`).
    /// `std` mutex: short, never-crossed-with-await critical sections.
    room: std::sync::Mutex<Option<String>>,
}

/// One connected game room: a 4-seat table plus an optional dealt game.
#[derive(Debug)]
struct Room {
    code: String,
    /// Taken for the critical sections that mutate state and enqueue events;
    /// the guards are never held across another lock acquisition.
    inner: Mutex<RoomInner>,
}

#[derive(Debug)]
struct RoomInner {
    seats: [Option<Occupant>; SEAT_COUNT],
    /// `Some` only while a dealt game exists, including finished ones (kept
    /// for the final board view; a refill triggers a fresh deal).
    game: Option<GameState>,
    /// Cumulative partnership score for the current match of hands. Survives
    /// every redeal; wiped by `deal_fresh_game` when it follows a finished
    /// match so the next four seats start a clean one.
    score: MatchScore,
}

#[derive(Debug)]
struct Occupant {
    player_id: PlayerId,
    name: String,
    outbox: Outbox,
    table_side: TableSide,
    /// Whether this player has tapped "Play again" since the last deal.
    ready: bool,
}

/// Manages all concurrent game rooms. Shared across connection tasks via
/// `Arc`; every operation is thread-safe.
#[derive(Debug)]
pub struct Lobby {
    next_id: AtomicU64,
    rooms: DashMap<String, Arc<Room>>,
    connections: DashMap<PlayerId, Connection>,
}

impl Lobby {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(0),
            rooms: DashMap::new(),
            connections: DashMap::new(),
        }
    }

    /// Mints the next connection identity. Called once per WebSocket upgrade,
    /// before anything else can happen for that socket.
    pub fn next_player_id(&self) -> PlayerId {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Registers the identity with an outbox and hands back the queue the
    /// connection's send task will drain until the socket dies (the sender
    /// held by the seat is dropped by [`Lobby::handle_disconnect`]).
    pub fn register(&self, id: PlayerId) -> mpsc::UnboundedReceiver<ServerToClient> {
        let (outbox, outbound) = mpsc::unbounded_channel();
        self.connections.insert(id, Connection { outbox, room: std::sync::Mutex::new(None) });
        outbound
    }

    /// Queues a `MoveRejected { reason }` on one connection's outbox. Used
    /// for protocol-level failures (unparseable frames, "did not join yet");
    /// the room layer routes engine-level rejections the same way.
    pub fn reject(&self, id: PlayerId, reason: String) {
        if let Some(connection) = self.connections.get(&id) {
            let _ = connection.outbox.send(ServerToClient::MoveRejected { reason });
        }
    }

    /// Whether a room code currently exists in the registry (also handy for
    /// ops/integration clients probing liveness).
    pub fn room_exists(&self, code: &str) -> bool {
        self.rooms.contains_key(code)
    }

    /// The routing heart of the lobby: validates the command against the
    /// connection's server-side room binding, converts wire shapes into
    /// engine values, and hands validated work to the owning room.
    pub async fn handle_event(&self, id: PlayerId, event: ClientToServer) {
        match event {
            ClientToServer::JoinRoom {
                room,
                name,
                team,
            } => {
                // Validate the team preference before touching any room, so a
                // malformed preference cannot leave an empty room behind.
                let Ok(preferred_team) = parse_team(team) else {
                    self.reject(id, "team must be 0 or 1".to_owned());
                    return;
                };
                let _ = self.join(id, &room, &name, preferred_team).await; // rejection is both returned and queued
            }
            ClientToServer::PlayTile { tile, side } => {
                let Ok(tile) = Tile::try_from(tile) else {
                    self.reject(id, "pip values must be between 0 and 6".to_owned());
                    return;
                };
                let side = Side::from(side);
                self.command(id, move |seat, game| game.play(seat, tile, side)).await;
            }
            ClientToServer::PassTurn => self.command(id, |seat, game| game.pass(seat)).await,
            ClientToServer::PlayAgain => {
                let bound_room = self.room_binding(&id).and_then(|code| self.room_for(&code));
                let Some(room) = bound_room else {
                    self.reject(id, "join a room before playing".to_owned());
                    return;
                };
                if let Err(reason) = room.signal_rematch(id).await {
                    self.reject(id, reason);
                }
            }
        }
    }

    /// Deregisters the connection: its room (if any) is told via broadcast
    /// that voids a running game and frees the seat; the registry drops the
    /// room when its last seat has emptied.
    pub async fn handle_disconnect(&self, id: PlayerId) {
        // Removing the registry entry drops this side of the outbox channel;
        // the seat's clone is dropped by `Room::leave` below.
        let Some((_, connection)) = self.connections.remove(&id) else { return };
        let room_code = connection.room.lock().expect("uncontended mutex").clone();
        if let Some(code) = room_code {
            if let Some(room) = self.room_for(&code) {
                room.leave(id).await;
                self.prune(&room, &code).await;
            }
        }
    }

    // ---- private routing ------------------------------------------------

    fn room_for(&self, code: &str) -> Option<Arc<Room>> {
        self.rooms.get(code).map(|room| Arc::clone(room.value()))
    }

    /// Seat registration (creating the room atomically if needed). The seat
    /// index is echoed to every seat through `RoomState` broadcasts, and the
    /// rejection reason (if any) both goes back to the caller and reaches the
    /// connection as a `MoveRejected` event.
    async fn join(
        &self,
        id: PlayerId,
        raw_code: &str,
        raw_name: &str,
        preferred_team: Option<Team>,
    ) -> Result<u8, String> {
        // A connection seats itself at most once; re-binding would leave a
        // ghost seat in the old room.
        if let Some(binding) = self.room_binding(&id) {
            let reason = format!("already seated in room {binding:?}; disconnect to leave");
            self.reject(id, reason.clone());
            return Err(reason);
        }
        let Some(code) = normalize_room_code(raw_code) else {
            let reason = format!("room codes must be 1..={MAX_ROOM_CODE_LEN} characters");
            self.reject(id, reason.clone());
            return Err(reason);
        };
        let name = raw_name.trim();
        if name.is_empty() || name.chars().count() > MAX_PLAYER_NAME_LEN {
            let reason = format!("player names must be 1..={MAX_PLAYER_NAME_LEN} characters");
            self.reject(id, reason.clone());
            return Err(reason);
        }

        // Atomic room creation; concurrent joiners share one very same Room.
        let room: Arc<Room> = match self.rooms.entry(code.clone()) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                Arc::clone(&entry.insert(Arc::new(Room::new(code.clone()))))
            }
        };

        // Clone the outbox *before* awaiting, so a disconnect racing this
        // join cannot lose the ability to seat (and then be detached).
        let Some(outbox) = self.outbox_of(&id) else {
            // The connection is already gone: nothing left to seat or rebind.
            return Err("the connection is no longer registered".to_owned());
        };
        match room.join(id, name, outbox, preferred_team).await {
            Ok(seat) => {
                if self.connections.contains_key(&id) {
                    self.set_room_binding(id, code.clone());
                    Ok(seat)
                } else {
                    // The socket died while joining; undo the seating.
                    room.leave(id).await;
                    Err("the connection vanished while joining".to_owned())
                }
            }
            Err(reason) => {
                // A failed first join may have created an empty room.
                self.prune(&room, &code).await;
                self.reject(id, reason.clone());
                Err(reason)
            }
        }
    }

    /// Routes `play_tile` / `pass_turn` work to the connection's room.
    /// Engine failures (not your turn, tile not in hand, ...) come back as
    /// `MoveRejected` on the very socket that issued the command; successes
    /// fan out as room-wide `GameStateUpdate`s.
    async fn command(&self, id: PlayerId, action: impl FnOnce(Seat, &mut GameState) -> Result<Outcome, MoveError>) {
        let bound_room = self.room_binding(&id).and_then(|code| self.room_for(&code));
        let Some(room) = bound_room else {
            self.reject(id, "join a room before playing".to_owned());
            return;
        };

        if let Err(reason) = room.commit(id, action).await {
            self.reject(id, reason);
        }
    }

    /// Removes `room` from the registry if (and only if) it is still the very
    /// instance stored under `code` and nobody remains seated in it.
    async fn prune(&self, room: &Arc<Room>, code: &str) {
        if room.is_abandoned().await {
            self.rooms.remove_if(code, |_, candidate| Arc::ptr_eq(candidate, room));
        }
    }

    // ---- connection helpers ---------------------------------------------

    fn outbox_of(&self, id: &PlayerId) -> Option<Outbox> {
        self.connections.get(id).map(|c| c.outbox.clone())
    }

    fn room_binding(&self, id: &PlayerId) -> Option<String> {
        self.connections
            .get(id)
            .and_then(|c| c.room.lock().expect("uncontended mutex").clone())
    }

    fn set_room_binding(&self, id: PlayerId, code: String) {
        if let Some(connection) = self.connections.get(&id) {
            *connection.room.lock().expect("uncontended mutex") = Some(code);
        }
    }
}

impl Room {
    fn new(code: String) -> Self {
        Self {
            code,
            inner: Mutex::new(RoomInner {
                seats: std::array::from_fn(|_| None),
                game: None,
                score: MatchScore::default(),
            }),
        }
    }

    /// A room nobody remains seated in.
    pub async fn is_abandoned(&self) -> bool {
        self.inner.lock().await.seats.iter().all(Option::is_none)
    }

    /// Seats a connection, broadcasts the personalized roster, and deals a
    /// fresh game as soon as the table is full. Per-seat private hands are
    /// sent straight into that seat's outbox; they never leave the room over
    /// anyone else's channel.
    async fn join(
        &self,
        player: PlayerId,
        name: &str,
        outbox: Outbox,
        preferred_team: Option<Team>,
    ) -> Result<u8, String> {
        let mut inner = self.inner.lock().await;

        if inner.seats.iter().any(|o| o.as_ref().is_some_and(|o| o.player_id == player)) {
            return Err("this connection already holds a seat".to_owned());
        }
        // Honor the partnership preference: pick a vacant seat inside that
        // team first, then fall back to the first vacant seat at the table.
        // Never renumber or evict an occupied seat.
        let Some(seat) = pick_seat(&inner.seats, preferred_team) else {
            return Err(format!("room {:?} already has all four seats taken", self.code));
        };
        // The seat ring walks counter-clockwise (rules.md): each seat hands the
        // turn to the player on its right-hand neighbour, so seat 1 is the WEST
        // seat of a North-facing-south table, not its East — North(0) passes to
        // West(1), West to South(2), South to East(3), East back to North.
        let table_side = match seat {
            0 => TableSide::North,
            1 => TableSide::West,
            2 => TableSide::South,
            3 => TableSide::East,
            _ => unreachable!(),
        };
        inner.seats[seat] = Some(Occupant {
            player_id: player,
            name: name.to_owned(),
            outbox,
            table_side,
            ready: false,
        });

        // Publish the room binding only after a successful seating.
        broadcast_room_state(&self.code, &inner);

        // Deal as soon as the table is full and no live game is running. When
        // a finished game lingers, the next refill starts a new one; until
        // that snapshot lands, `room_phase` reports the room as `Waiting`
        // (`Finished` is never emitted), and clients route on the game's
        // own `result` field, not the phase.
        if inner.seats.iter().all(Option::is_some)
            && inner.game.as_ref().is_none_or(|game| game.is_finished())
        {
            deal_fresh_game(&mut inner);
        }

        Ok(seat as u8)
    }

    /// Removes a player from the room. A running game is voided
    /// (`GameResult { winner_team: None, points: 0, reason: "abandoned" }`)
    /// rather than simulated on for the empty seat; the terminating result
    /// travels in the follow-up `GameStateUpdate`.
    async fn leave(&self, player: PlayerId) {
        let mut inner = self.inner.lock().await;
        let Some(seat) = inner.seats.iter().position(|o| {
            o.as_ref().is_some_and(|o| o.player_id == player)
        }) else {
            return;
        };
        let occupant = inner.seats[seat].take().expect("seat lookup matched");

        // Farewell first: remaining seats learn *who* vanished.
        broadcast(&inner.seats, ServerToClient::OpponentDisconnected {
            player: occupant.name,
            seat: seat as u8,
        });

        // Void a live game so nobody is stranded waiting on a gone seat.
        let game_ended = inner
            .game
            .as_mut()
            .is_some_and(|game| game.abandon_if_live());
        if game_ended {
            let state = game_state_view(
                inner.game.as_ref().expect("live game just ended"),
                &inner.score,
            );
            broadcast(&inner.seats, ServerToClient::GameStateUpdate { state });
        }

        // The roster refresh always follows.
        broadcast_room_state(&self.code, &inner);
    }

    /// Applies a game command inside the room lock, then fans out the public
    /// progression. Returns a human-readable rejection for engine failures.
    async fn commit(
        &self,
        player: PlayerId,
        action: impl FnOnce(Seat, &mut GameState) -> Result<Outcome, MoveError>,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock().await;
        // Split the guard once so the game, the score, and the seats are all
        // writable side by side (field borrows through `DerefMut` alone would
        // conflict).
        let RoomInner { seats, game, score } = &mut *guard;
        let seat = seats
            .iter()
            .position(|o| o.as_ref().is_some_and(|o| o.player_id == player))
            .ok_or("you are not seated in this room")?;
        let game = game.as_mut().ok_or("no game has been dealt in this room yet")?;

        match action(seat, game) {
            Ok(outcome) => {
                // A finished hand folds its points into the running match
                // score *before* the snapshot is built, so the terminating
                // update already carries the banked total.
                if let Outcome::Ended(result) = outcome {
                    score.record(result);
                }
                // Build every payload while `game` is borrowed, then fan out.
                let state = game_state_view(game, score);
                // The game closed — nobody can put a tile: prepare the
                // all-hands reveal that follows the terminating snapshot.
                let revealed = matches!(outcome, Outcome::Ended(result) if result.is_closed())
                    .then(|| closed_hands_view(game));
                // Still holding the room mutex: this update is ordered after
                // every earlier one and before any later mutation, per client.
                broadcast(seats, ServerToClient::GameStateUpdate { state });
                if let Some(hands) = revealed {
                    broadcast(seats, ServerToClient::GameClosed { hands });
                }
                Ok(())
            }
            Err(reason) => Err(reason.to_string()),
        }
    }

    /// Marks a seat ready for the next game ("Play again"). When a full
    /// table of four ready seats shares a finished (or absent) game, the
    /// room redeals in place — the sockets never cycle, so no seat can be
    /// held hostage by a connection the server has not seen close yet.
    async fn signal_rematch(&self, player: PlayerId) -> Result<(), String> {
        let mut inner = self.inner.lock().await;
        let seat = inner
            .seats
            .iter()
            .position(|o| o.as_ref().is_some_and(|o| o.player_id == player))
            .ok_or("you are not seated in this room")?;
        if inner.game.as_ref().is_some_and(|game| !game.is_finished()) {
            return Err("the current game is still in progress".to_owned());
        }
        inner.seats[seat].as_mut().expect("seat lookup matched").ready = true;

        if inner.seats.iter().all(|o| o.as_ref().is_some_and(|o| o.ready)) {
            deal_fresh_game(&mut inner);
        }
        // The roster (with readiness) lands last, so waiting seats see the
        // flags cleared by a redeal at the same time as its opening snapshot.
        broadcast_room_state(&self.code, &inner);
        Ok(())
    }
}

/// Deals a fresh game to a full table whose current game is finished (or
/// which has never had one). Hands go straight into their owners' outboxes;
/// the opening snapshot fans out to everyone afterwards.
///
/// If the match that just ended had reached its target, the running score is
/// wiped first so the new hand opens a clean match (`Rules.md`: first to 100
/// wins; tapping "Play again" after a match win starts the next one).
fn deal_fresh_game(inner: &mut RoomInner) {
    if inner.score.is_over() {
        inner.score.reset();
    }
    let game = fresh_game(inner.game.as_ref());

    clear_readiness(&mut inner.seats);
    deal_private_hands(&mut inner.seats, &game);

    let opening = game_state_view(&game, &inner.score);
    inner.game = Some(game);
    broadcast(&inner.seats, ServerToClient::GameStateUpdate { state: opening });
}

/// Builds the next game from a freshly shuffled double-six set, opening per
/// the session's opening policy (see [`next_opener`]).
fn fresh_game(previous: Option<&GameState>) -> GameState {
    let mut tiles = Tile::full_set();
    tiles.shuffle(&mut thread_rng());
    match next_opener(previous) {
        None => GameState::first_hand(tiles),
        Some(opener) => GameState::new(tiles, opener),
    }
}

/// Opening policy (`Rules.md`), for the hand dealt after `previous`:
/// * The first hand of a session (`None`) is opened by the holder of the
///   **6|6**, who must lay it down — the engine picks that seat and enforces
///   it as the forced opening tile.
/// * Every later hand is opened by the previous hand's *playing-out seat* (the
///   one who emptied their hand). A blocked hand has no playing-out seat — it
///   is won by a partnership on pips, not by a player — so the open simply
///   rotates one seat counter-clockwise from the previous opener, as does a
///   voided game or the defensive fallback for an unfinished hand.
fn next_opener(previous: Option<&GameState>) -> Option<Seat> {
    previous.map(|game| {
        game.result()
            .and_then(|result| result.out)
            .unwrap_or_else(|| (game.opener() + 1) % SEAT_COUNT)
    })
}

/// Every rematch tap is cleared: each readiness flag starts the next
/// rematch clock over.
fn clear_readiness(seats: &mut [Option<Occupant>; SEAT_COUNT]) {
    for occupant in seats.iter_mut().flatten() {
        occupant.ready = false;
    }
}

/// Sends each private hand strictly into its owner's outbox, before any
/// public state can race ahead of it — a seat must never glimpse another
/// seat's hand.
fn deal_private_hands(seats: &mut [Option<Occupant>; SEAT_COUNT], game: &GameState) {
    for (seat, occupant) in seats.iter_mut().enumerate() {
        let occupant = occupant.as_mut().expect("table is full");
        let hand = game.hand(seat).iter().copied().map(Into::into).collect();
        let _ = occupant.outbox.send(ServerToClient::HandDealt { seat: seat as u8, hand });
    }
}

/// Sends one message to every seated player. Because `join`/`leave`/`commit`
/// broadcast while holding the room mutex, each client's outbox receives a
/// totally ordered view of its room.
fn broadcast(seats: &[Option<Occupant>; SEAT_COUNT], message: ServerToClient) {
    for occupant in seats.iter().flatten() {
        let _ = occupant.outbox.send(message.clone());
    }
}

/// Personalized [`RoomState`] broadcast: each seated player receives its own
/// copy with `you` set to its seat index.
fn broadcast_room_state(code: &str, inner: &RoomInner) {
    let roster: Vec<PublicSeat> = inner
        .seats
        .iter()
        .enumerate()
        .filter_map(|(seat, occupant)| {
            occupant.as_ref().map(|o| PublicSeat {
                seat: seat as u8,
                player_id: o.player_id,
                name: o.name.clone(),
                table_side: o.table_side,
                team: team_of(seat) as u8,
                ready: o.ready,
            })
        })
        .collect();
    let phase = room_phase(inner);
    for (seat, occupant) in inner.seats.iter().enumerate() {
        let Some(occupant) = occupant else { continue };
        let _ = occupant.outbox.send(ServerToClient::RoomState {
            room: PublicRoomState {
                room_id: code.to_owned(),
                you: seat,
                phase,
                seats: roster.clone(),
            },
        });
    }
}

/// Room lifecycle: a live dealt game means `Playing`; everything else
/// (open seats, imminent redeal after a finished game) is `Waiting`.
fn room_phase(inner: &RoomInner) -> RoomPhase {
    match inner.game.as_ref() {
        Some(game) if !game.is_finished() => RoomPhase::Playing,
        _ => RoomPhase::Waiting,
    }
}

/// Pick a seat honoring a partnership preference: a vacant seat inside that
/// team first, then the first vacant seat anywhere at the table. A full
/// requested team never refuses a join — the player lands wherever a seat is
/// free. Returns `None` only when all four seats are occupied. Occupied seats
/// are never renumbered or evicted, so the table is always exactly two seats
/// per team once full (`team_of(seat) == seat % 2`).
fn pick_seat(seats: &[Option<Occupant>; SEAT_COUNT], preferred: Option<Team>) -> Option<Seat> {
    if let Some(team) = preferred {
        if let Some(seat) = team_seats(team).into_iter().find(|&seat| seats[seat].is_none()) {
            return Some(seat);
        }
    }
    seats.iter().position(Option::is_none)
}

/// Trims a client-supplied room code and enforces the length bound.
fn normalize_room_code(raw: &str) -> Option<String> {
    let code = raw.trim();
    (!code.is_empty() && code.chars().count() <= MAX_ROOM_CODE_LEN).then(|| code.to_owned())
}

impl Default for Lobby {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::game::{EndReason, GameResult, HAND_SIZE, TILE_COUNT};
    use crate::protocol::{ClosedHand, PublicGameState, PublicTile, Side};
    type Event = ServerToClient;

    /// Registers `n` fake connections and hands back each one's minted id with
    /// its live receiver. Ids come from the lobby, so no test may assume where
    /// the counting starts (they used to hardcode `1..=4` while the lobby mints
    /// from `0`, which left every one of them joining an unregistered socket).
    async fn connections(lobby: &Lobby, n: u64) -> Vec<(PlayerId, mpsc::UnboundedReceiver<Event>)> {
        (0..n)
            .map(|_| {
                let id = lobby.next_player_id();
                (id, lobby.register(id))
            })
            .collect()
    }

    /// A seated four-seat table: join order is seat order, so `seats[i]` is the
    /// occupant of seat `i`.
    async fn fill_table() -> (Lobby, Vec<(PlayerId, mpsc::UnboundedReceiver<Event>)>) {
        let lobby = Lobby::new();
        let mut seats = connections(&lobby, 4).await;
        for (id, _) in seats.iter_mut() {
            lobby.join(*id, "a", &format!("p{id}"), None).await.expect("seat available");
        }
        (lobby, seats)
    }

    #[tokio::test]
    async fn fourth_join_deals_and_every_seat_gets_its_private_hand() {
        let (_lobby, mut seats) = fill_table().await;
        for (seat, (_, rx)) in seats.iter_mut().enumerate() {
            // A seat is told the roster once per join it was already present
            // for, then handed its tiles and the opening snapshot; drain
            // everything and assert on what arrived, not on its position.
            let (mut hands, mut updates, mut roster) = (0, 0, None::<PublicRoomState>);
            while let Ok(event) = rx.try_recv() {
                match event {
                    Event::HandDealt { seat: dealt, hand } => {
                        assert_eq!(dealt as usize, seat, "a hand went to the wrong seat");
                        assert_eq!(hand.len(), 7, "a double-six hand is seven tiles");
                        hands += 1;
                    }
                    Event::GameStateUpdate { .. } => updates += 1,
                    Event::RoomState { room } => roster = Some(room),
                    other => panic!("unexpected event at seat {seat}: {other:?}"),
                }
            }
            let roster = roster.expect("every seat is told the table is full");
            assert_eq!(roster.you, seat, "the roster is personalized per recipient");
            assert_eq!(roster.seats.len(), SEAT_COUNT);
            // The roster is published a step before the deal inside the same
            // room lock, so it still reads `waiting`; the client routes on the
            // snapshot that follows it, never on this field.
            assert_eq!(roster.phase, RoomPhase::Waiting);
            assert_eq!(hands, 1, "seat {seat} holds one private hand, not a copy per seat");
            assert_eq!(updates, 1, "seat {seat} saw exactly the opening snapshot");
        }
    }

    #[tokio::test]
    async fn full_room_rejects_fifth_seeker() {
        let (lobby, mut _seats) = fill_table().await;
        let (latecomer, mut rx5) = connections(&lobby, 1).await.pop().expect("one receiver");
        lobby.join(latecomer, "a", "latecomer", None).await.expect_err("room is full");
        assert!(matches!(rx5.try_recv(), Ok(Event::MoveRejected { .. })));
    }

    #[tokio::test]
    async fn out_of_turn_play_is_rejected_to_the_offender_only() {
        let (lobby, mut seats) = fill_table().await;
        // Join order is seat order. Learn the opener from the post-deal update
        // and each hand from its recipient.
        let mut hands: [Option<Vec<PublicTile>>; 4] = std::array::from_fn(|_| None);
        let mut turn = 0usize;
        for (_, rx) in seats.iter_mut() {
            while let Ok(event) = rx.try_recv() {
                match event {
                    Event::HandDealt { seat, hand } => hands[seat as usize] = Some(hand),
                    Event::GameStateUpdate { state } => turn = state.turn,
                    _ => {}
                }
            }
        }
        let offender_seat = (turn + 1) % SEAT_COUNT;
        let offender = seats[offender_seat].0;
        let tile = hands[offender_seat].as_ref().expect("hand was dealt")[0];

        lobby
            .handle_event(offender, ClientToServer::PlayTile { tile, side: Side::Right })
            .await;

        // The offender is told exactly once; nothing fans out to anyone.
        let mut rejections = 0;
        for (seat, (_, rx)) in seats.iter_mut().enumerate() {
            while let Ok(event) = rx.try_recv() {
                match &event {
                    Event::MoveRejected { reason } if reason == "it is not your turn" => {
                        rejections += 1;
                    }
                    other => panic!("unexpected fan-out to seat {seat}: {other:?}"),
                }
            }
        }
        assert_eq!(rejections, 1, "only the offender was told");
    }

    #[tokio::test]
    async fn disconnect_voids_a_running_game_and_frees_the_seat() {
        let (lobby, mut seats) = fill_table().await;
        let north = seats[0].0;
        lobby.handle_disconnect(north).await;
        // Someone must observe the abandon flow: farewell, terminating update,
        // and a final roster that no longer lists the departed seat. The
        // departure broadcasts after the joins, so only the LAST roster may
        // show the seat as vacant.
        let (mut bye, mut voided, mut freed) = (false, false, false);
        for (_, rx) in seats.iter_mut() {
            let mut roster = None;
            while let Ok(event) = rx.try_recv() {
                match &event {
                    Event::OpponentDisconnected { seat, .. } => {
                        bye = true;
                        assert_eq!(*seat, 0, "the seat-0 occupant vacated seat 0");
                    }
                    Event::GameStateUpdate { state } => {
                        // The opening snapshot is in this queue too; only the
                        // one that carries a result is the void.
                        if let Some(result) = state.result {
                            voided = true;
                            assert_eq!(result.reason, EndReason::Abandoned);
                            assert_eq!(result.winner_team, None, "a voided hand crowns nobody");
                            assert_eq!(result.points, 0, "a voided hand scores nothing");
                        }
                    }
                    Event::RoomState { room } => roster = Some(room.clone()),
                    _ => {}
                }
            }
            // Only survivors learn the seat is free: the departed socket's
            // last roster still shows a full table.
            if roster.is_some_and(|roster| roster.seats.iter().all(|s| s.seat != 0)) {
                freed = true;
            }
        }
        assert!(bye && voided, "the abandon flow reached the table");
        assert!(freed, "a survivor's final roster drops the departed seat");
        assert!(lobby.room_exists("a"), "three seats remain");
    }

    #[tokio::test]
    async fn last_departure_prunes_the_empty_room() {
        let lobby = Lobby::new();
        let seats = connections(&lobby, 2).await;
        let solo = seats[0].0;
        lobby.join(solo, "prune", "solo", None).await.expect("seat");
        assert!(lobby.room_exists("prune"));
        lobby.handle_disconnect(solo).await;
        // Never seated, so this departure must not be what empties the room.
        lobby.handle_disconnect(seats[1].0).await;
        assert!(!lobby.room_exists("prune"), "empty rooms must be pruned");
    }

    #[tokio::test]
    async fn malformed_room_code_and_name_are_rejected() {
        let lobby = Lobby::new();
        let (id, mut rx) = connections(&lobby, 1).await.pop().expect("one receiver");
        assert!(lobby.join(id, "   ", "ok name", None).await.is_err(), "a blank room code is refused");
        assert!(matches!(rx.try_recv(), Ok(Event::MoveRejected { .. })));
        assert!(
            lobby.join(id, "waaaaaaaaaaaaaaaaaaaaaaaaaay-too-long", "ok name", None).await.is_err(),
            "an over-long room code is refused"
        );
        assert!(matches!(rx.try_recv(), Ok(Event::MoveRejected { .. })));
        assert!(lobby.join(id, "ok", "     ", None).await.is_err(), "a blank name is refused");
        assert!(matches!(rx.try_recv(), Ok(Event::MoveRejected { .. })));
        assert!(!lobby.room_exists("ok"), "a refused join must not leave a room behind");
    }

    #[tokio::test]
    async fn team_preference_seats_each_player_on_requested_partnership() {
        let lobby = Lobby::new();
        let mut seats = connections(&lobby, 4).await;
        // Interleave teams so the fall-back path is also exercised implicitly:
        // west, north, east, south — each gets its requested partnership.
        let prefs = [Some(1), Some(0), Some(1), Some(0)];
        for ((id, _), pref) in seats.iter_mut().zip(prefs) {
            lobby
                .join(*id, "a", &format!("p{id}"), pref)
                .await
                .expect("team seat available");
        }
        // Single drain per seat: confirm the side and count the private hand.
        for ((id, rx), pref) in seats.iter_mut().zip(prefs) {
            let mut team = None;
            let mut seat = None;
            let mut seat_count = 0usize;
            let mut hands = 0usize;
            while let Ok(event) = rx.try_recv() {
                match event {
                    Event::RoomState { room } => {
                        seat_count = room.seats.len();
                        if let Some(me) = room.seats.iter().find(|s| s.player_id == *id) {
                            team = Some(me.team);
                            seat = Some(me.seat);
                        }
                    }
                    Event::HandDealt { .. } => hands += 1,
                    _ => {}
                }
            }
            let (team, seat) =
                (team.expect("a roster arrived"), seat.expect("the joiner is listed"));
            assert_eq!(team, pref.unwrap() as u8, "team preference honored");
            assert_eq!(team, seat % 2, "team matches seat parity");
            assert_eq!(seat_count, SEAT_COUNT, "all four seats filled");
            assert_eq!(hands, 1, "each seat got exactly one private hand");
        }
    }

    #[tokio::test]
    async fn full_requested_team_falls_back_to_any_vacant_seat() {
        let lobby = Lobby::new();
        // Both seats of team 1 (west & east) are taken by two preference joins.
        let mut team1_ids = Vec::new();
        for _ in 0..2 {
            let (id, _) = connections(&lobby, 1).await.pop().expect("one receiver");
            lobby
                .join(id, "a", "t1", Some(1))
                .await
                .expect("team-1 seat available");
            team1_ids.push(id);
        }
        // A third team-1 seeker finds both team seats taken and is seated
        // wherever a seat is free — never refused for a full team.
        let (seeker, mut rx) = connections(&lobby, 1).await.pop().expect("one receiver");
        let seat = lobby
            .join(seeker, "a", "t1-fallback", Some(1))
            .await
            .expect("fallback seat available");
        assert!(
            seat % 2 == 0,
            "the fallback seat belongs to the other team, seat {seat}"
        );
        let mut roster = None;
        while let Ok(event) = rx.try_recv() {
            if let Event::RoomState { room } = event {
                roster = Some(room);
            }
        }
        let room = roster.expect("roster arrived");
        let me = room
            .seats
            .iter()
            .find(|s| s.player_id == seeker)
            .expect("the fallback joiner is listed");
        assert_eq!(me.team, 0, "the roster reports the seat the fallback joiner actually got");
    }

    #[tokio::test]
    async fn invalid_team_is_rejected_before_any_room_is_created() {
        let lobby = Lobby::new();
        let (id, mut rx) = connections(&lobby, 1).await.pop().expect("one receiver");
        lobby
            .handle_event(
                id,
                ClientToServer::JoinRoom {
                    room: "nope".into(),
                    name: "Ada".into(),
                    team: Some(7),
                },
            )
            .await;
        assert!(
            matches!(
                rx.try_recv(),
                Ok(Event::MoveRejected { reason }) if reason == "team must be 0 or 1"
            ),
            "an invalid team is refused on the sender's socket only"
        );
        assert!(!lobby.room_exists("nope"), "a refused join leaves no room behind");
    }

    /// First `(tile, side)` placement fitting the public ends, mirroring what
    /// a client's legality layer computes from its own hand and the forced
    /// opening tile. `None` means the seat must pass — a mismatch here is a
    /// bug if the engine disagrees.
    fn first_legal(hand: &[PublicTile], state: &PublicGameState) -> Option<(PublicTile, Side)> {
        let (Some(left), Some(right)) = (state.left_end, state.right_end) else {
            // Empty board: only the forced opening tile may be laid, if any.
            return match state.opening_tile {
                Some(opening) => Some((opening, Side::Right)),
                None => hand.first().copied().map(|tile| (tile, Side::Right)),
            };
        };
        hand.iter().find_map(|&tile| {
            if tile.left == left || tile.right == left {
                Some((tile, Side::Left))
            } else if tile.left == right || tile.right == right {
                Some((tile, Side::Right))
            } else {
                None
            }
        })
    }

    /// Pulls the pending `GameStateUpdate` (and any `GameClosed` reveal that
    /// follows it) from every seat's outbox, failing on any rejection or stray
    /// event. All seats must see equal snapshots; returns the one every seat
    /// received together with the closed-game hands, if the command closed it.
    fn unanimous_update(
        seats: &mut [(PlayerId, mpsc::UnboundedReceiver<Event>)],
    ) -> (PublicGameState, Option<Vec<ClosedHand>>) {
        let mut shared: Option<(PublicGameState, Option<Vec<ClosedHand>>)> = None;
        for (seat, (_, rx)) in seats.iter_mut().enumerate() {
            let (mut update, mut closed) = (None, None);
            while let Ok(event) = rx.try_recv() {
                match event {
                    Event::GameStateUpdate { state } => {
                        assert!(update.is_none(), "seat {seat} got two updates for one command");
                        update = Some(state);
                    }
                    Event::GameClosed { hands } => {
                        assert!(closed.is_none(), "seat {seat} was told of one closing only");
                        closed = Some(hands);
                    }
                    Event::MoveRejected { reason } => panic!("seat {seat} rejected: {reason}"),
                    other => panic!("seat {seat} saw a stray event mid-game: {other:?}"),
                }
            }
            let update = update.unwrap_or_else(|| panic!("seat {seat} got no update"));
            match &shared {
                None => shared = Some((update, closed)),
                Some((previous, previous_closed)) => {
                    assert_eq!(previous, &update, "seats observed divergent snapshots");
                    assert_eq!(previous_closed, &closed, "seats observed divergent reveals");
                }
            }
        }
        shared.expect("every seat saw the update")
    }

    #[tokio::test]
    async fn a_whole_game_over_the_wire_keeps_counts_and_turn_walk_consistent() {
        let (lobby, mut seats) = fill_table().await;

        // Track each seat's private hand exactly as its client would, from
        // the `HandDealt` it received, and cross-check every broadcast.
        let mut hands: [Vec<PublicTile>; SEAT_COUNT] = std::array::from_fn(|_| Vec::new());
        let mut state: Option<PublicGameState> = None;
        for (seat, (_, rx)) in seats.iter_mut().enumerate() {
            while let Ok(event) = rx.try_recv() {
                match event {
                    Event::HandDealt { seat: dealt, hand } => {
                        assert_eq!(dealt as usize, seat);
                        hands[seat] = hand;
                    }
                    Event::GameStateUpdate { state: snapshot } => state = Some(snapshot),
                    Event::RoomState { .. } => {}
                    other => panic!("unexpected event at seat {seat}: {other:?}"),
                }
            }
        }
        let mut state = state.expect("opening snapshot");
        assert_eq!(state.hand_counts, [7, 7, 7, 7]);
        assert!(state.result.is_none());
        // The first hand of the session: the 6|6 holder opens, and the 6|6 is
        // the only tile that can be laid.
        assert_eq!(
            state.opening_tile,
            Some(PublicTile { left: 6, right: 6 }),
            "the first hand is opened by the double six"
        );
        assert!(
            hands[state.turn].contains(&PublicTile { left: 6, right: 6 }),
            "the opener holds the 6|6"
        );

        let mut pass_run = 0usize;
        let mut rejected_off_turn = false;
        let mut ended = None;
        for _step in 0..400 {
            let actor = state.turn;

            // Once, mid-game, have a seat that is NOT up try to pass: it must
            // be refused on its own socket without disturbing anyone else.
            if !rejected_off_turn && actor != 0 {
                rejected_off_turn = true;
                let idler = (actor + 2) % SEAT_COUNT;
                lobby.handle_event(seats[idler].0, ClientToServer::PassTurn).await;
                let mut saw_rejection = false;
                for (seat, (_, rx)) in seats.iter_mut().enumerate() {
                    let mut stray = Vec::new();
                    while let Ok(event) = rx.try_recv() {
                        stray.push(event);
                    }
                    if seat == idler {
                        assert!(
                            stray.iter().any(|event| matches!(event, Event::MoveRejected { reason } if reason == "it is not your turn")),
                            "the off-turn passer was not told why"
                        );
                        saw_rejection = true;
                    } else {
                        assert!(stray.is_empty(), "an off-turn attempt must not fan out to seat {seat}");
                    }
                }
                assert!(saw_rejection);
            }

            match first_legal(&hands[actor], &state) {
                Some((tile, side)) => {
                    lobby.handle_event(seats[actor].0, ClientToServer::PlayTile { tile, side }).await;
                    let before = hands[actor].len();
                    hands[actor].retain(|held| *held != tile);
                    assert_eq!(hands[actor].len(), before - 1, "the played tile must leave the hand exactly once");
                    pass_run = 0;
                }
                None => {
                    // The engine must agree that this seat cannot play: a
                    // rejection here means its legality check and ours diverged.
                    lobby.handle_event(seats[actor].0, ClientToServer::PassTurn).await;
                    pass_run += 1;
                }
            }

            let (next, closed) = unanimous_update(&mut seats);
            for seat in 0..SEAT_COUNT {
                assert_eq!(
                    next.hand_counts[seat],
                    hands[seat].len() as u8,
                    "broadcast counts must mirror the hidden hand of seat {seat}"
                );
            }
            assert_eq!(
                next.board.len() + usize::from(next.hand_counts.iter().sum::<u8>()),
                TILE_COUNT,
                "28 tiles: on the board or in a seat's hand"
            );
            assert_eq!(usize::from(next.consecutive_passes), pass_run);
            match next.result {
                None => {
                    assert_eq!(next.turn, (actor + 1) % SEAT_COUNT, "turn walks one seat to the right");
                    assert_eq!(next.score.points, [0, 0], "the match score stays blank inside a live hand");
                }
                Some(result) => {
                    assert_eq!(next.turn, actor, "the turn freezes when the game ends");
                    let pips: [u8; SEAT_COUNT] = std::array::from_fn(|seat| {
                        hands[seat].iter().map(|tile| tile.left + tile.right).sum()
                    });
                    assert_eq!(result.hand_pips, pips, "the result must publish the pips every hand ended on");
                    let team_pips = [
                        u16::from(pips[0]) + u16::from(pips[2]),
                        u16::from(pips[1]) + u16::from(pips[3]),
                    ];
                    match result.reason {
                        EndReason::HandEmpty => {
                            assert!(closed.is_none(), "a played-out game reveals no hidden hands");
                            assert_eq!(result.out, Some(actor), "the seat that just emptied its hand played out");
                            let team = actor % 2;
                            assert_eq!(result.winner_team, Some(team), "playing out wins the hand for the seat's partnership");
                            assert_eq!(
                                result.points,
                                u8::try_from(team_pips[1 - team]).expect("two hands fit in a u8"),
                                "the winners bank the pips left in the rival partnership"
                            );
                            assert!(hands[actor].is_empty());
                        }
                        EndReason::Blocked => {
                            assert_eq!(result.out, None, "a blocked hand has no playing-out seat");
                            let (expected_team, expected_points) = if team_pips[0] == team_pips[1] {
                                (None, 0u16)
                            } else if team_pips[0] < team_pips[1] {
                                (Some(0), team_pips[1])
                            } else {
                                (Some(1), team_pips[0])
                            };
                            assert_eq!(
                                result.winner_team, expected_team,
                                "blocked winner must be the team with the lower combined pips (pips: {pips:?})"
                            );
                            assert_eq!(u16::from(result.points), expected_points, "points = the losing team's pips");
                            // A closed game also lands a `GameClosed` reveal on every
                            // seat: one entry per absolute seat, matching exactly the
                            // tiles the seats are still holding.
                            let revealed = closed.expect("a closed game reveals every hand to every seat");
                            assert_eq!(revealed.len(), SEAT_COUNT);
                            for (seat, entry) in revealed.iter().enumerate() {
                                assert_eq!(entry.seat as usize, seat, "reveal rows are seat-ordered");
                                assert_eq!(
                                    entry.hand, hands[seat],
                                    "seat {seat}: the revealed hand must match its hidden hand"
                                );
                            }
                        }
                        EndReason::Abandoned => unreachable!("nobody left in this test"),
                    }
                    // The terminating snapshot already carries the awarded points.
                    let expected_score = std::array::from_fn(|team| {
                        if result.winner_team == Some(team) { u16::from(result.points) } else { 0 }
                    });
                    assert_eq!(next.score.points, expected_score, "the ending snapshot carries the match score");
                    assert_eq!(
                        next.score.over,
                        expected_score.iter().any(|p| *p >= next.score.target),
                        "the match flips to over exactly when a team reaches the target"
                    );
                    ended = Some(result);
                    break;
                }
            }
            state = next;
        }
        assert!(ended.is_some(), "the game must terminate: every command either plays or blocks");

        // A finished game refuses any further command from any seat.
        for (id, _) in seats.iter() {
            lobby.handle_event(*id, ClientToServer::PassTurn).await;
        }
        for (seat, (_, rx)) in seats.iter_mut().enumerate() {
            assert!(
                matches!(rx.try_recv(), Ok(Event::MoveRejected { reason }) if reason == "the game is not active"),
                "seat {seat} must be told the game is over, once"
            );
            assert!(rx.try_recv().is_err(), "one rejection per offender");
        }
    }

    #[tokio::test]
    async fn seats_stay_absolute_through_disconnect_reseat_and_redeal() {
        let (lobby, mut seats) = fill_table().await;
        let departed = seats[1].0;
        let (survivor_0, survivor_2, survivor_3) = (seats[0].0, seats[2].0, seats[3].0);
        for (_, rx) in seats.iter_mut() {
            while rx.try_recv().is_ok() {} // drain the opening fan-out
        }

        lobby.handle_disconnect(departed).await;

        // The hole must stay at index 1: survivors keep their original seat
        // numbers, and the vacant seat is simply absent — never renumbered.
        for index in [0usize, 2, 3] {
            let mut bye = None;
            let mut voided = None;
            let mut roster = None;
            while let Ok(event) = seats[index].1.try_recv() {
                match event {
                    Event::OpponentDisconnected { player, seat } => {
                        assert_eq!(seat as usize, 1, "the departee vacated seat 1");
                        bye = Some(player);
                    }
                    Event::GameStateUpdate { state } => {
                        if let Some(result) = state.result {
                            assert_eq!(result.reason, EndReason::Abandoned);
                            assert_eq!(result.winner_team, None);
                            voided = Some(result);
                        }
                    }
                    Event::RoomState { room } => roster = Some(room),
                    other => panic!("seat {index} saw a stray event: {other:?}"),
                }
            }
            assert_eq!(bye.as_deref(), Some(format!("p{departed}").as_str()));
            assert!(voided.is_some(), "the live game was voided");
            let roster = roster.expect("final roster");
            let listed: Vec<usize> = roster.seats.iter().map(|seat| seat.seat as usize).collect();
            assert_eq!(listed, vec![0, 2, 3], "seat numbering survives the departure unshifted");
            for public in &roster.seats {
                let expected_id = match public.seat as usize {
                    0 => survivor_0,
                    2 => survivor_2,
                    3 => survivor_3,
                    other => panic!("seat {other} should be vacant"),
                };
                assert_eq!(public.player_id, expected_id, "survivors keep their seats");
            }
            assert_eq!(roster.you, index, "personalized rosters name the recipient's absolute seat");
        }

        // A newcomer takes the hole, seat 1 — the first vacant index, not a
        // fresh tail or a compacted slot.
        let (refugee, mut refugee_rx) = connections(&lobby, 1).await.pop().expect("one receiver");
        assert_eq!(
            lobby.join(refugee, "a", "refugee", None).await.expect("the hole is fillable"),
            1,
            "the replacement must inherit the departed seat number"
        );

        // The table is full again and the abandoned game is finished, so a
        // redeal fires: every hand lands on its owner's ABSOLUTE seat and the
        // survivors are dealt exactly where they sat before the departure.
        /// Pulls one queue's redeal fan-out: the private hand addressed to
        /// this recipient, the live opening snapshot, and the latest roster.
        fn drain_redeal(rx: &mut mpsc::UnboundedReceiver<Event>) -> (Option<(u8, Vec<PublicTile>)>, Option<PublicGameState>, Option<PublicRoomState>) {
            let (mut hand, mut opening, mut roster) = (None, None, None);
            while let Ok(event) = rx.try_recv() {
                match event {
                    Event::HandDealt { seat, hand: dealt } => {
                        assert!(hand.is_none(), "an owner was dealt twice");
                        hand = Some((seat, dealt));
                    }
                    Event::GameStateUpdate { state } => {
                        if state.result.is_none() {
                            opening = Some(state);
                        }
                    }
                    Event::RoomState { room } => roster = Some(room),
                    Event::OpponentDisconnected { .. } => panic!("nobody left during the reseat redeal"),
                    Event::GameClosed { .. } => panic!("a fresh deal cannot have closed"),
                    Event::MoveRejected { reason } => panic!("rejected during reseat: {reason}"),
                }
            }
            (hand, opening, roster)
        }

        let (refugee_hand, opening, refugee_roster) = drain_redeal(&mut refugee_rx);
        let opening = opening.expect("the redeal broadcast a fresh snapshot to the newcomer");
        let mut hands_by_seat: [Option<(u8, Vec<PublicTile>)>; SEAT_COUNT] = [None, refugee_hand, None, None];
        let mut rosters = [None, refugee_roster, None, None];
        for index in [0usize, 2, 3] {
            let (hand, survivor_opening, roster) = drain_redeal(&mut seats[index].1);
            assert_eq!(
                survivor_opening.as_ref(),
                Some(&opening),
                "seat {index}: every survivor must see the same redeal snapshot as the newcomer"
            );
            hands_by_seat[index] = hand;
            rosters[index] = roster;
        }
        for (index, entry) in hands_by_seat.into_iter().enumerate() {
            let (seat, dealt) = entry.unwrap_or_else(|| panic!("the owner of seat {index} got no redeal hand"));
            assert_eq!(seat as usize, index, "the HandDealt tag for the owner of seat {index} must be {index}");
            assert_eq!(dealt.len(), HAND_SIZE);
        }
        let roster = rosters[3].clone().expect("survivors saw the reseat roster");
        assert_eq!(rosters[0].as_ref().map(|r| r.you), Some(0), "personalization stays absolute");
        assert_eq!(rosters[2].as_ref().map(|r| r.you), Some(2));
        assert_eq!(roster.seats.len(), SEAT_COUNT);
        for public in &roster.seats {
            let expected = match public.seat as usize {
                0 => survivor_0,
                1 => refugee,
                2 => survivor_2,
                3 => survivor_3,
                other => unreachable!("{other}"),
            };
            assert_eq!(public.player_id, expected, "seat {} kept its identity across the reseat", public.seat);
            let expected_side = match public.seat as usize {
                0 => TableSide::North,
                1 => TableSide::West,
                2 => TableSide::South,
                _ => TableSide::East,
            };
            assert_eq!(public.table_side, expected_side, "table sides are pinned to seats, not occupants");
        }
        assert_eq!(opening.hand_counts, [7, 7, 7, 7]);
        assert!(opening.result.is_none(), "a live game carries no stale result");
        assert!(opening.turn < SEAT_COUNT, "the new opener is an absolute seat");
    }

    #[tokio::test]
    async fn a_blocked_game_closes_and_reveals_every_hand_to_all_seats() {
        let (lobby, mut seats) = fill_table().await;
        // Replace the randomly dealt game with a deterministic blocked
        // position: seat 0 opens 6|6 keeping 0|0, the other three seats each
        // hold one small double. No six survives outside the opening play, so
        // the whole circle passes and the fourth consecutive pass closes it.
        let room = lobby.room_for("a").expect("the filled room is registered");
        {
            let mut inner = room.inner.lock().await;
            inner.game = Some(GameState::from_hands([
                vec![Tile::new(6, 6), Tile::new(0, 0)],
                vec![Tile::new(1, 1)],
                vec![Tile::new(2, 2)],
                vec![Tile::new(3, 3)],
            ]));
        }
        // Drain the deal fan-out so only the closing round is observed.
        for (_, rx) in seats.iter_mut() {
            while rx.try_recv().is_ok() {}
        }

        // Seat 0 (the opener) lays the double six, then everyone is stuck.
        lobby
            .handle_event(
                seats[0].0,
                ClientToServer::PlayTile { tile: PublicTile { left: 6, right: 6 }, side: Side::Right },
            )
            .await;
        for (_, rx) in seats.iter_mut() {
            while rx.try_recv().is_ok() {}
        }
        // Seats 1..3 pass (three in a row), then seat 0's pass is the fourth.
        for seat in 1..SEAT_COUNT {
            lobby.handle_event(seats[seat].0, ClientToServer::PassTurn).await;
            for (_, rx) in seats.iter_mut() {
                while rx.try_recv().is_ok() {}
            }
        }
        lobby.handle_event(seats[0].0, ClientToServer::PassTurn).await;

        // Every seat is told the game closed, and the closing carries all four
        // hands in seat order — no seat is dealt a different reveal than the
        // others see.
        let expected = vec![
            ClosedHand { seat: 0, hand: vec![PublicTile { left: 0, right: 0 }] },
            ClosedHand { seat: 1, hand: vec![PublicTile { left: 1, right: 1 }] },
            ClosedHand { seat: 2, hand: vec![PublicTile { left: 2, right: 2 }] },
            ClosedHand { seat: 3, hand: vec![PublicTile { left: 3, right: 3 }] },
        ];
        for (seat, (_, rx)) in seats.iter_mut().enumerate() {
            let (mut closed_update, mut reveal) = (false, None);
            while let Ok(event) = rx.try_recv() {
                match event {
                    Event::GameStateUpdate { state } => {
                        let result = state.result.expect("the fourth pass ends the game");
                        assert_eq!(result.reason, EndReason::Blocked);
                        // Team 0 (seats 0 & 2) ends on 0 + 4 = 4 pips, team 1
                        // on 2 + 6 = 8, so the North–South pair wins and banks
                        // the rivals' 8.
                        assert_eq!(result.winner_team, Some(0), "the lower-pip partnership wins the close");
                        assert_eq!(result.points, 8, "the winners bank the losing team's pips");
                        assert_eq!(result.out, None, "a blocked hand has no playing-out seat");
                        closed_update = true;
                    }
                    Event::GameClosed { hands } => reveal = Some(hands),
                    other => panic!("seat {seat} saw a stray event on the close: {other:?}"),
                }
            }
            assert!(closed_update, "seat {seat} saw the terminating snapshot");
            assert_eq!(reveal.expect("seat {seat} was told the game closed"), expected);
        }
    }

    #[tokio::test]
    async fn play_again_redeals_in_place_once_every_seat_is_ready() {
        let (lobby, mut seats) = fill_table().await;
        for (_, rx) in seats.iter_mut() {
            while rx.try_recv().is_ok() {} // drain the opening deal fan-out
        }

        // While a game is live, tapping Play again is refused politely.
        lobby.handle_event(seats[0].0, ClientToServer::PlayAgain).await;
        let mut refused = false;
        for (seat, (_, rx)) in seats.iter_mut().enumerate() {
            while let Ok(event) = rx.try_recv() {
                match event {
                    Event::MoveRejected { reason } if seat == 0 => {
                        assert!(reason.contains("in progress"), "reason: {reason}");
                        refused = true;
                    }
                    Event::MoveRejected { reason } => panic!("seat {seat} rejected: {reason}"),
                    other => panic!("seat {seat} saw a stray event: {other:?}"),
                }
            }
        }
        assert!(refused, "the early tap was refused on its own socket only");

        // Finish the game: swap the live deal for a rigged, ended one.
        let room = lobby.room_for("a").expect("the filled room is registered");
        {
            let mut inner = room.inner.lock().await;
            let mut game = GameState::from_hands([
                vec![Tile::new(0, 0)],
                vec![Tile::new(1, 1)],
                vec![Tile::new(2, 2)],
                vec![Tile::new(3, 3)],
            ]);
            game.abandon();
            inner.game = Some(game);
        }

        // Three readiness taps must not deal on their own: they only travel
        // as roster updates, identically to every seat.
        for actor in 0..3 {
            lobby.handle_event(seats[actor].0, ClientToServer::PlayAgain).await;
        }
        for (seat, (_, rx)) in seats.iter_mut().enumerate() {
            let mut last_roster = None;
            while let Ok(event) = rx.try_recv() {
                match event {
                    Event::RoomState { room } => last_roster = Some(room),
                    other => panic!("seat {seat} saw {other:?} before the table was ready"),
                }
            }
            let roster = last_roster.expect("readiness travels in the roster");
            assert_eq!(roster.seats.iter().filter(|s| s.ready).count(), 3, "three ready seats at seat {seat}");
            assert_eq!(roster.seats.len(), SEAT_COUNT);
        }

        // The fourth tap lands everyone ready at once: the room redeals in
        // place — same sockets, same seats, a fresh live game for all.
        lobby.handle_event(seats[3].0, ClientToServer::PlayAgain).await;
        let expected_seats: Vec<(u8, PlayerId)> =
            seats.iter().enumerate().map(|(i, (id, _))| (i as u8, *id)).collect();
        for (seat, (_, rx)) in seats.iter_mut().enumerate() {
            let (mut dealt, mut opening, mut final_roster) = (None, None, None);
            while let Ok(event) = rx.try_recv() {
                match event {
                    Event::HandDealt { seat: owner, hand } => {
                        assert_eq!(owner as usize, seat, "the redeal must land on the same seat");
                        dealt = Some(hand);
                    }
                    Event::GameStateUpdate { state } => opening = Some(state),
                    Event::RoomState { room } => final_roster = Some(room),
                    other => panic!("seat {seat} saw {other:?} during the redeal"),
                }
            }
            assert_eq!(dealt.expect("redeal hand").len(), HAND_SIZE);
            let opening = opening.expect("redeal snapshot");
            assert!(opening.result.is_none(), "the fresh game is live");
            assert_eq!(opening.hand_counts, [7, 7, 7, 7]);
            let roster = final_roster.expect("final roster");
            assert!(roster.seats.iter().all(|s| !s.ready), "readiness resets with the deal");
            let listed: Vec<(u8, PlayerId)> =
                roster.seats.iter().map(|s| (s.seat, s.player_id)).collect();
            assert_eq!(listed, expected_seats, "an in-place rematch never moves a seat");
        }
    }

    /// Drains the deal fan-out and returns the shared opening snapshot plus
    /// every seat's private hand, exactly as the seats saw them.
    fn opening_of(
        seats: &mut [(PlayerId, mpsc::UnboundedReceiver<Event>)],
    ) -> (PublicGameState, [Vec<PublicTile>; SEAT_COUNT]) {
        let mut hands: [Vec<PublicTile>; SEAT_COUNT] = std::array::from_fn(|_| Vec::new());
        let mut state = None;
        for (_, rx) in seats.iter_mut() {
            while let Ok(event) = rx.try_recv() {
                match event {
                    Event::HandDealt { seat, hand } => hands[seat as usize] = hand,
                    Event::GameStateUpdate { state: snapshot } => state = Some(snapshot),
                    Event::RoomState { .. } => {}
                    other => panic!("unexpected opening event: {other:?}"),
                }
            }
        }
        (state.expect("an opening snapshot"), hands)
    }

    #[tokio::test]
    async fn the_first_hand_is_opened_by_the_double_six_and_nothing_else() {
        let (lobby, mut seats) = fill_table().await;
        let (opening, hands) = opening_of(&mut seats);

        let double_six = PublicTile { left: 6, right: 6 };
        assert_eq!(opening.opening_tile, Some(double_six));
        let opener = opening.turn;
        assert!(hands[opener].contains(&double_six), "the 6|6 holder opens the session");

        // Any other tile is refused outright — even though every tile would
        // fit an empty board.
        let other = *hands[opener].iter().find(|&&tile| tile != double_six).expect("six more tiles");
        lobby
            .handle_event(seats[opener].0, ClientToServer::PlayTile { tile: other, side: Side::Right })
            .await;
        assert!(
            matches!(seats[opener].1.try_recv(), Ok(Event::MoveRejected { reason }) if reason.contains("6|6")),
            "the opener is told the 6|6 must start the layout"
        );

        // The double six itself goes down and the forced opening is spent.
        lobby
            .handle_event(seats[opener].0, ClientToServer::PlayTile { tile: double_six, side: Side::Right })
            .await;
        let (played, _) = unanimous_update(&mut seats);
        assert_eq!(played.opening_tile, None);
        assert_eq!(played.board.len(), 1);
        assert_eq!(played.turn, (opener + 1) % SEAT_COUNT);
    }

    #[tokio::test]
    async fn a_rematch_is_opened_by_the_previous_hand_winner() {
        let (lobby, mut seats) = fill_table().await;
        for (_, rx) in seats.iter_mut() {
            while rx.try_recv().is_ok() {}
        }

        // Seat 2 plays out with the 6|6 and wins the hand by dominoing.
        let mut finished = GameState::from_hands([
            vec![Tile::new(0, 1)],
            vec![Tile::new(1, 2)],
            vec![Tile::new(6, 6)],
            vec![Tile::new(3, 4)],
        ]);
        assert_eq!(finished.turn(), 2);
        finished.play(2, Tile::new(6, 6), crate::game::Side::Right).expect("plays out");
        assert_eq!(finished.result().and_then(|result| result.out), Some(2));
        let room = lobby.room_for("a").expect("filled room");
        room.inner.lock().await.game = Some(finished);

        for (id, _) in seats.iter() {
            lobby.handle_event(*id, ClientToServer::PlayAgain).await;
        }

        let inner = room.inner.lock().await;
        let game = inner.game.as_ref().expect("the rematch dealt a fresh hand");
        assert_eq!(game.turn(), 2, "the previous winner opens the next hand");
        assert_eq!(game.opener(), 2);
        assert_eq!(game.opening_tile(), None, "later hands open with any tile");
    }

    #[tokio::test]
    async fn a_blocked_rematch_rotates_the_open_from_the_previous_opener() {
        let (lobby, mut seats) = fill_table().await;
        for (_, rx) in seats.iter_mut() {
            while rx.try_recv().is_ok() {}
        }

        // Seat 1 opens the 6|6 and keeps 1|2; seat 0 keeps 0|0. Blocked hand:
        // team 0 (seats 0 & 2 = 0 + 4 pips) beats team 1 (seats 1 & 3 = 3 + 6).
        // No single seat "wins" a blocked hand, so the open rotates one seat
        // counter-clockwise from the previous opener (seat 1 → seat 2).
        let mut finished = GameState::from_hands([
            vec![Tile::new(0, 0)],
            vec![Tile::new(6, 6), Tile::new(1, 2)],
            vec![Tile::new(2, 2)],
            vec![Tile::new(3, 3)],
        ]);
        assert_eq!(finished.turn(), 1, "6|6 is the highest double");
        finished.play(1, Tile::new(6, 6), crate::game::Side::Right).expect("opens");
        for seat in [2, 3, 0, 1] {
            finished.pass(seat).expect("no six outside the opening double");
        }
        assert_eq!(
            finished.result(),
            Some(GameResult {
                out: None,
                winner_team: Some(0),
                points: 9,
                hand_pips: [0, 3, 4, 6],
                reason: EndReason::Blocked,
            })
        );
        let room = lobby.room_for("a").expect("filled room");
        room.inner.lock().await.game = Some(finished);

        for (id, _) in seats.iter() {
            lobby.handle_event(*id, ClientToServer::PlayAgain).await;
        }

        let inner = room.inner.lock().await;
        let game = inner.game.as_ref().expect("the rematch dealt a fresh hand");
        assert_eq!(game.turn(), 2, "a blocked hand rotates the open off its opener");
        assert_eq!(game.opener(), 2);
        assert_eq!(game.opening_tile(), None);
    }

    #[tokio::test]
    async fn a_tied_blocked_rematch_rotates_the_open_counter_clockwise() {
        let (lobby, mut seats) = fill_table().await;
        for (_, rx) in seats.iter_mut() {
            while rx.try_recv().is_ok() {}
        }

        // Seat 2 opens the 6|6 and keeps 0|4. Team 0 (seats 0 & 2) and team 1
        // (seats 1 & 3) both finish on 7 combined pips, so the blocked hand is
        // a genuine partnership tie and the open rotates from seat 2 to seat 3.
        let mut finished = GameState::from_hands([
            vec![Tile::new(1, 2)],                                     // 3 pips
            vec![Tile::new(0, 3)],                                     // 3 pips
            vec![Tile::new(6, 6), Tile::new(0, 4)],                    // opener keeps 4
            vec![Tile::new(2, 2)],                                     // 4 pips
        ]);
        assert_eq!(finished.turn(), 2, "6|6 is the highest double");
        finished.play(2, Tile::new(6, 6), crate::game::Side::Right).expect("opens");
        for seat in [3, 0, 1, 2] {
            finished.pass(seat).expect("no six outside the opening double");
        }
        assert_eq!(
            finished.result(),
            Some(GameResult {
                out: None,
                winner_team: None,
                points: 0,
                hand_pips: [3, 3, 4, 4],
                reason: EndReason::Blocked,
            }),
            "tied team totals (7 = 7) make the blocked hand a draw"
        );
        let room = lobby.room_for("a").expect("filled room");
        room.inner.lock().await.game = Some(finished);

        for (id, _) in seats.iter() {
            lobby.handle_event(*id, ClientToServer::PlayAgain).await;
        }

        let inner = room.inner.lock().await;
        let game = inner.game.as_ref().expect("the rematch dealt a fresh hand");
        assert_eq!(game.turn(), 3, "a draw rotates the open one seat counter-clockwise");
        assert_eq!(game.opening_tile(), None);
    }

    /// Rigs a blocked scenario and drives the whole circuit over the wire:
    /// seat 0 lays the 6|6, then all four seats pass. Team 0 (seats 0 & 2)
    /// ends on 0 + 6 = 6 pips against team 1's 27 + 19 = 46, so the
    /// North–South partnership banks 46 points. Returns the terminating
    /// snapshot every seat observed.
    async fn play_scored_blocked_hand(
        lobby: &Lobby,
        seats: &mut [(PlayerId, mpsc::UnboundedReceiver<Event>)],
    ) -> PublicGameState {
        let room = lobby.room_for("a").expect("filled room");
        {
            let mut inner = room.inner.lock().await;
            inner.game = Some(GameState::from_hands([
                vec![Tile::new(6, 6), Tile::new(0, 0)],
                vec![Tile::new(5, 5), Tile::new(4, 4), Tile::new(5, 4)],
                vec![Tile::new(3, 3)],
                vec![Tile::new(5, 3), Tile::new(4, 3), Tile::new(2, 2)],
            ]));
        }
        lobby
            .handle_event(
                seats[0].0,
                ClientToServer::PlayTile { tile: PublicTile { left: 6, right: 6 }, side: Side::Right },
            )
            .await;
        for seat in [1usize, 2, 3, 0] {
            lobby.handle_event(seats[seat].0, ClientToServer::PassTurn).await;
        }
        let mut ending = None;
        for (_, rx) in seats.iter_mut() {
            while let Ok(event) = rx.try_recv() {
                if let Event::GameStateUpdate { state } = event {
                    if state.result.is_some() {
                        ending = Some(state);
                    }
                }
            }
        }
        ending.expect("the blocked hand ended with a result on the wire")
    }

    #[tokio::test]
    async fn a_played_out_hand_scores_the_rival_pips_without_a_full_reveal() {
        let (lobby, mut seats) = fill_table().await;
        for (_, rx) in seats.iter_mut() {
            while rx.try_recv().is_ok() {}
        }

        // Rig a one-tile domino-out: seat 2 lays its only tile (the 6|6) and
        // empties its hand. Rivals (team 1: seats 1 & 3) sit on 3 + 7 = 10
        // pips and the partner's 1 pip earns nothing.
        let room = lobby.room_for("a").expect("filled room");
        {
            let mut inner = room.inner.lock().await;
            inner.game = Some(GameState::from_hands([
                vec![Tile::new(0, 1)],
                vec![Tile::new(1, 2)],
                vec![Tile::new(6, 6)],
                vec![Tile::new(3, 4)],
            ]));
        }
        lobby
            .handle_event(
                seats[2].0,
                ClientToServer::PlayTile { tile: PublicTile { left: 6, right: 6 }, side: Side::Right },
            )
            .await;

        for (seat, (_, rx)) in seats.iter_mut().enumerate() {
            let mut ending = None;
            while let Ok(event) = rx.try_recv() {
                match event {
                    Event::GameStateUpdate { state } => {
                        if let Some(result) = state.result {
                            ending = Some((state, result));
                        }
                    }
                    Event::GameClosed { .. } => {
                        panic!("seat {seat}: a played-out hand must not reveal every hand")
                    }
                    Event::MoveRejected { reason } => panic!("seat {seat} rejected: {reason}"),
                    _ => {}
                }
            }
            let (state, result) =
                ending.unwrap_or_else(|| panic!("seat {seat} saw no terminating snapshot"));
            assert_eq!(result.reason, EndReason::HandEmpty);
            assert_eq!(result.out, Some(2));
            assert_eq!(result.winner_team, Some(0), "seat 2's partnership takes the hand");
            assert_eq!(result.points, 10, "only the rival partnership's pips are banked");
            assert_eq!(result.hand_pips, [1, 3, 0, 7], "the ending publishes every hand's pips");
            assert_eq!(state.score.points, [10, 0]);
        }
    }

    /// Pulls the live opening snapshot out of a redeal's fan-out.
    fn drain_opening(seats: &mut [(PlayerId, mpsc::UnboundedReceiver<Event>)]) -> PublicGameState {
        let mut opening = None;
        for (_, rx) in seats.iter_mut() {
            while let Ok(event) = rx.try_recv() {
                if let Event::GameStateUpdate { state } = event {
                    assert!(state.result.is_none(), "the redeal snapshot is live");
                    opening = Some(state);
                }
            }
        }
        opening.expect("the table saw the redeal snapshot")
    }

    #[tokio::test]
    async fn a_match_accumulates_points_across_hands_and_restarts_after_100() {
        let (lobby, mut seats) = fill_table().await;
        for (_, rx) in seats.iter_mut() {
            while rx.try_recv().is_ok() {}
        }

        // Hand 1 banks its 46 points straight into the terminating snapshot.
        let ending = play_scored_blocked_hand(&lobby, &mut seats).await;
        let result = ending.result.expect("hand 1 ended blocked");
        assert_eq!(result.winner_team, Some(0));
        assert_eq!(result.points, 46);
        assert_eq!(ending.score.points, [46, 0], "hand 1 is banked in the match score");
        assert!(!ending.score.over);

        // Rematch: the score rides into hand 2 — the match keeps running.
        for (id, _) in seats.iter() {
            lobby.handle_event(*id, ClientToServer::PlayAgain).await;
        }
        let opening = drain_opening(&mut seats);
        assert_eq!(opening.score.points, [46, 0], "the match score survives the redeal");

        let ending = play_scored_blocked_hand(&lobby, &mut seats).await;
        assert_eq!(ending.score.points, [92, 0], "hand 2 adds to the same match");
        assert!(!ending.score.over, "92 is short of the target of 100");

        // Hand 3 crosses 100: the match ends and names its winners.
        for (id, _) in seats.iter() {
            lobby.handle_event(*id, ClientToServer::PlayAgain).await;
        }
        drain_opening(&mut seats);
        let ending = play_scored_blocked_hand(&lobby, &mut seats).await;
        assert_eq!(ending.score.points, [138, 0]);
        assert!(ending.score.over, "reaching the target ends the match");
        assert_eq!(ending.score.winner_team, Some(0));

        // Tapping "Play again" after a decided match opens a fresh match.
        for (id, _) in seats.iter() {
            lobby.handle_event(*id, ClientToServer::PlayAgain).await;
        }
        let opening = drain_opening(&mut seats);
        assert_eq!(opening.score.points, [0, 0], "the new match starts from zero");
        assert!(!opening.score.over);
    }
}

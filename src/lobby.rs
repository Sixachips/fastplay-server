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

use crate::game::{GameState, MoveError, Outcome, Seat, SEAT_COUNT, Side, TableSide, Tile};
use crate::protocol::{
    game_state_view, ClientToServer, PublicRoomState, PublicSeat, PublicTile, RoomPhase,
    ServerToClient,
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
}

#[derive(Debug)]
struct Occupant {
    player_id: PlayerId,
    name: String,
    outbox: Outbox,
    table_side: TableSide,
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
            ClientToServer::JoinRoom { room, name } => {
                let _ = self.join(id, &room, &name).await; // rejection is both returned and queued
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
    async fn join(&self, id: PlayerId, raw_code: &str, raw_name: &str) -> Result<u8, String> {
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
        match room.join(id, name, outbox).await {
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
            inner: Mutex::new(RoomInner { seats: std::array::from_fn(|_| None), game: None }),
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
    async fn join(&self, player: PlayerId, name: &str, outbox: Outbox) -> Result<u8, String> {
        let mut inner = self.inner.lock().await;

        if inner.seats.iter().any(|o| o.as_ref().is_some_and(|o| o.player_id == player)) {
            return Err("this connection already holds a seat".to_owned());
        }
        let Some(seat) = inner.seats.iter().position(Option::is_none) else {
            return Err(format!("room {:?} already has all four seats taken", self.code));
        };
        let table_side = match seat {
            0 => TableSide::North,
            1 => TableSide::East,
            2 => TableSide::South,
            3 => TableSide::West,
            _ => unreachable!(),
        };
        inner.seats[seat] = Some(Occupant { player_id: player, name: name.to_owned(), outbox, table_side });

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
            let mut tiles = Tile::full_set();
            tiles.shuffle(&mut thread_rng());
            let game = GameState::new(tiles);

            // Hands first, strictly into their owners' outboxes.
            for (seat, occupant) in inner.seats.iter_mut().enumerate() {
                let occupant = occupant.as_mut().expect("table is full");
                let hand: Vec<PublicTile> =
                    game.hand(seat).iter().copied().map(Into::into).collect();
                let _ = occupant.outbox.send(ServerToClient::HandDealt { seat: seat as u8, hand });
            }
            let opening = game_state_view(&game);
            inner.game = Some(game);
            broadcast(&inner.seats, ServerToClient::GameStateUpdate { state: opening });
        }

        Ok(seat as u8)
    }

    /// Removes a player from the room. A running game is voided
    /// (`GameResult { winner: None, reason: "abandoned" }`) rather than
    /// simulated on for the empty seat; the terminating result travels in the
    /// follow-up `GameStateUpdate`.
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
            let state = game_state_view(inner.game.as_ref().expect("live game just ended"));
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
        let mut inner = self.inner.lock().await;
        let seat = inner
            .seats
            .iter()
            .position(|o| o.as_ref().is_some_and(|o| o.player_id == player))
            .ok_or("you are not seated in this room")?;
        let game = inner.game.as_mut().ok_or("no game has been dealt in this room yet")?;

        match action(seat, game) {
            Ok(_) => {
                let state = game_state_view(game);
                // Still holding the room mutex: this update is ordered after
                // every earlier one and before any later mutation, per client.
                broadcast(&inner.seats, ServerToClient::GameStateUpdate { state });
                Ok(())
            }
            Err(reason) => Err(reason.to_string()),
        }
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
    use crate::game::{EndReason, HAND_SIZE, TILE_COUNT};
    use crate::protocol::{PublicGameState, Side};
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
            lobby.join(*id, "a", &format!("p{id}")).await.expect("seat available");
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
        lobby.join(latecomer, "a", "latecomer").await.expect_err("room is full");
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
                            assert_eq!(result.winner, None);
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
        lobby.join(solo, "prune", "solo").await.expect("seat");
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
        assert!(lobby.join(id, "   ", "ok name").await.is_err(), "a blank room code is refused");
        assert!(matches!(rx.try_recv(), Ok(Event::MoveRejected { .. })));
        assert!(
            lobby.join(id, "waaaaaaaaaaaaaaaaaaaaaaaaaay-too-long", "ok name").await.is_err(),
            "an over-long room code is refused"
        );
        assert!(matches!(rx.try_recv(), Ok(Event::MoveRejected { .. })));
        assert!(lobby.join(id, "ok", "     ").await.is_err(), "a blank name is refused");
        assert!(matches!(rx.try_recv(), Ok(Event::MoveRejected { .. })));
        assert!(!lobby.room_exists("ok"), "a refused join must not leave a room behind");
    }

    /// First `(tile, side)` placement fitting the public ends, mirroring what
    /// a client's legality layer computes from its own hand. `None` means the
    /// seat must pass — a mismatch here is a bug if the engine disagrees.
    fn first_legal(hand: &[PublicTile], left_end: Option<u8>, right_end: Option<u8>) -> Option<(PublicTile, Side)> {
        let (left, right) = match (left_end, right_end) {
            (Some(l), Some(r)) => (l, r),
            _ => return hand.first().copied().map(|tile| (tile, Side::Right)), // empty board: any tile
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

    /// Pulls the single pending `GameStateUpdate` from every seat's outbox,
    /// failing on any rejection or stray event. All seats must see equal
    /// snapshots; returns the one every seat received.
    fn unanimous_update(seats: &mut [(PlayerId, mpsc::UnboundedReceiver<Event>)]) -> PublicGameState {
        let mut shared: Option<PublicGameState> = None;
        for (seat, (_, rx)) in seats.iter_mut().enumerate() {
            let mut pending = Vec::new();
            while let Ok(event) = rx.try_recv() {
                pending.push(event);
            }
            let mut updates = pending.iter().filter_map(|event| match event {
                Event::GameStateUpdate { state } => Some(state.clone()),
                Event::MoveRejected { reason } => panic!("seat {seat} rejected: {reason}"),
                other => panic!("seat {seat} saw a stray event mid-game: {other:?}"),
            });
            let first = updates.next().unwrap_or_else(|| panic!("seat {seat} got no update"));
            assert!(updates.next().is_none(), "seat {seat} got two updates for one command");
            match &shared {
                None => shared = Some(first),
                Some(previous) => assert_eq!(previous, &first, "seats observed divergent snapshots"),
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

            match first_legal(&hands[actor], state.left_end, state.right_end) {
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

            let next = unanimous_update(&mut seats);
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
                }
                Some(result) => {
                    assert_eq!(next.turn, actor, "the turn freezes when the game ends");
                    match result.reason {
                        EndReason::HandEmpty => {
                            assert_eq!(result.winner, Some(actor), "the seat that just emptied its hand wins");
                            assert!(hands[actor].is_empty());
                        }
                        EndReason::Blocked => {
                            // Winner = the absolute seat with the fewest pips,
                            // recomputed from the hands the clients were dealt.
                            let pips: [u8; SEAT_COUNT] = std::array::from_fn(|seat| {
                                hands[seat].iter().map(|tile| tile.left + tile.right).sum()
                            });
                            let min = pips.iter().copied().min().expect("four seats");
                            let fewest: Vec<usize> = (0..SEAT_COUNT).filter(|&s| pips[s] == min).collect();
                            assert_eq!(
                                result.winner,
                                if fewest.len() == 1 { Some(fewest[0]) } else { None },
                                "blocked winner must be the pip-minimum seat index (pips: {pips:?})"
                            );
                        }
                        EndReason::Abandoned => unreachable!("nobody left in this test"),
                    }
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
                            assert_eq!(result.winner, None);
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
            lobby.join(refugee, "a", "refugee").await.expect("the hole is fillable"),
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
                1 => TableSide::East,
                2 => TableSide::South,
                _ => TableSide::West,
            };
            assert_eq!(public.table_side, expected_side, "table sides are pinned to seats, not occupants");
        }
        assert_eq!(opening.hand_counts, [7, 7, 7, 7]);
        assert!(opening.result.is_none(), "a live game carries no stale result");
        assert!(opening.turn < SEAT_COUNT, "the new opener is an absolute seat");
    }
}

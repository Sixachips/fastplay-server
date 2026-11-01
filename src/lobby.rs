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

use crate::game::{GameState, MoveError, Outcome, Seat, SEAT_COUNT, Side, Tile};
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
        inner.seats[seat] = Some(Occupant { player_id: player, name: name.to_owned(), outbox });

        // Publish the room binding only after a successful seating.
        broadcast_room_state(&self.code, &inner);

        // Deal as soon as the table is full and no live game is running. When
        // a finished game lingers, the next refill starts a new one — the
        // room stays visible in the `Finished` phase until then.
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
    use crate::game::EndReason;
    use crate::protocol::Side;
    type Event = ServerToClient;

    /// Registers `n` fake connections and returns their active receivers.
    async fn connections(lobby: &Lobby, n: u64) -> Vec<mpsc::UnboundedReceiver<Event>> {
        (0..n)
            .map(|_| {
                let id = lobby.next_player_id();
                lobby.register(id)
            })
            .collect()
    }

    fn hand_of_7(event: &Event) -> bool {
        matches!(event, Event::HandDealt { hand, .. } if hand.len() == 7)
    }

    async fn fill_table() -> (Lobby, Vec<mpsc::UnboundedReceiver<Event>>) {
        let lobby = Lobby::new();
        let rx = connections(&lobby, 4).await;
        for id in 1..=4u64 {
            lobby.join(id, "a", &format!("p{id}")).await.expect("seat available");
        }
        (lobby, rx)
    }

    #[tokio::test]
    async fn fourth_join_deals_and_every_seat_gets_its_private_hand() {
        let (_lobby, mut rx) = fill_table().await;
        for r in rx.iter_mut() {
            let events = (0..3).map(|_| r.try_recv().expect("event")).collect::<Vec<_>>();
            assert!(events.iter().all(|e| hand_of_7(e)), "each seat got 7 tiles: {events:?}");
            assert!(events.iter().any(|e| matches!(e, Event::GameStateUpdate { .. })));
        }
    }

    #[tokio::test]
    async fn full_room_rejects_fifth_seeker() {
        let (lobby, mut _rx) = fill_table().await;
        let mut rx5 = connections(&lobby, 1).await.pop().expect("one receiver");
        lobby.join(5, "a", "latecomer").await.expect_err("room is full");
        assert!(matches!(rx5.try_recv(), Ok(Event::MoveRejected { .. })));
    }

    #[tokio::test]
    async fn out_of_turn_play_is_rejected_to_the_offender_only() {
        let (lobby, mut rx) = fill_table().await;
        // Player ids seat in join order: player i+1 sits at seat i. Learn the
        // opener from the post-deal update and each hand from its recipient.
        let mut hands: [Option<Vec<PublicTile>>; 4] = std::array::from_fn(|_| None);
        let mut turn = 0usize;
        for r in rx.iter_mut() {
            while let Ok(event) = r.try_recv() {
                match event {
                    Event::HandDealt { seat, hand } => hands[seat as usize] = Some(hand),
                    Event::GameStateUpdate { state } => turn = state.turn,
                    _ => {}
                }
            }
        }
        let offender_seat = (turn + 1) % SEAT_COUNT;
        let offender = offender_seat as u64 + 1;
        let tile = hands[offender_seat].as_ref().expect("hand was dealt")[0];

        lobby
            .handle_event(offender, ClientToServer::PlayTile { tile, side: Side::Right })
            .await;

        // The offender is told exactly once; nothing fans out to anyone.
        let mut rejections = 0;
        for (player, r) in rx.iter_mut().enumerate() {
            while let Ok(event) = r.try_recv() {
                match &event {
                    Event::MoveRejected { reason } if reason == "it is not your turn" => {
                        rejections += 1;
                    }
                    other => panic!("unexpected fan-out to player {}: {other:?}", player + 1),
                }
            }
        }
        assert_eq!(rejections, 1, "only the offender was told");
    }

    #[tokio::test]
    async fn disconnect_voids_a_running_game_and_frees_the_seat() {
        let (lobby, mut rx) = fill_table().await;
        lobby.handle_disconnect(1).await;
        // Someone must observe the abandon flow: farewell, terminating update,
        // and a roster refresh reflecting the vacant seat.
        let (mut bye, mut ended, mut refresh) = (false, false, false);
        for r in rx.iter_mut() {
            while let Ok(event) = r.try_recv() {
                match &event {
                    Event::OpponentDisconnected { seat, .. } => {
                        bye = true;
                        assert_eq!(*seat, 0, "sitting player 1 vacated seat 0");
                    }
                    Event::GameStateUpdate { state } => {
                        ended = true;
                        let result = state.result.expect("voided game reports its result");
                        assert_eq!(result.reason, EndReason::Abandoned);
                        assert_eq!(result.winner, None);
                    }
                    Event::RoomState { room } => {
                        refresh = true;
                        assert!(room.seats.iter().all(|s| s.seat != 0));
                    }
                    _ => {}
                }
            }
        }
        assert!(bye && ended && refresh, "abandon flow reached the table");
        assert!(lobby.room_exists("a"), "three seats remain");
    }

    #[tokio::test]
    async fn last_departure_prunes_the_empty_room() {
        let lobby = Lobby::new();
        let mut rx = connections(&lobby, 2).await;
        lobby.join(1, "prune", "solo").await.expect("seat");
        assert!(lobby.room_exists("prune"));
        lobby.handle_disconnect(1).await;
        lobby.handle_disconnect(2).await;
        assert!(!lobby.room_exists("prune"), "empty rooms must be pruned");
        let _ = rx.pop();
    }

    #[tokio::test]
    async fn malformed_room_code_and_name_are_rejected() {
        let lobby = Lobby::new();
        let mut rx = connections(&lobby, 1).await.pop().expect("one receiver");
        lobby.join(1, "   ", "ok name").await;
        assert!(matches!(rx.try_recv(), Ok(Event::MoveRejected { .. })));
        lobby.join(1, "waaaaaaaaaaaaaaaaaaaaaaaaaay-too-long", "ok name").await;
        assert!(matches!(rx.try_recv(), Ok(Event::MoveRejected { .. })));
        lobby.join(1, "ok", "     ").await;
        assert!(matches!(rx.try_recv(), Ok(Event::MoveRejected { .. })));
        assert!(!lobby.room_exists("ok"));
    }
}

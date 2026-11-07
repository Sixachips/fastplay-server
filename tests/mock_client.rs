//! Integration harness: four mock clients over real WebSockets, driving the
//! production transport (`fastplay_server::net`) end to end.
//!
//! Where the in-crate lobby suite swaps in-memory channels for outboxes, this
//! file speaks the *wire*: JSON text frames through the actual Axum/tungstenite
//! stack, exercising serialization, frame discipline, and fan-out ordering the
//! way the Yew client does. Coverage:
//!
//! * a full partnership match played hand after hand until one team crosses
//!   100 — every terminating snapshot re-verified independently (team winner
//!   by combined pips, points = rival pips, score accumulation, hand_pips vs
//!   each client's own tiles, tile conservation, byte-identical broadcasts);
//! * the post-match `PlayAgain` redeal starting a fresh match from 0–0;
//! * binary and malformed frames rejected without killing the socket.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use fastplay_server::game::EndReason;
use fastplay_server::lobby::Lobby;
use fastplay_server::net;
use fastplay_server::protocol::{
    ClientToServer, PublicGameState, PublicRoomState, PublicTile, ServerToClient, Side,
};

const TIMEOUT: Duration = Duration::from_secs(20);

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Bind the real router to an ephemeral loopback port and serve it in the
/// background. Returns the `ws://127.0.0.1:PORT/ws` endpoint.
async fn spawn_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, net::app(Arc::new(Lobby::new())))
            .await
            .expect("server task");
    });
    format!("ws://{addr}/ws")
}

/// One scripted player: a live socket plus everything a real client keeps —
/// its seat, roster view, private hand, and latest game snapshot.
struct MockClient {
    name: &'static str,
    tx: futures_util::stream::SplitSink<Ws, Message>,
    rx: futures_util::stream::SplitStream<Ws>,
    you: usize,
    roster: Option<PublicRoomState>,
    hand: Vec<PublicTile>,
    game: Option<PublicGameState>,
}

impl MockClient {
    async fn connect(url: &str, name: &'static str) -> Self {
        let (stream, _) = connect_async(url).await.expect("ws connect");
        let (tx, rx) = stream.split();
        Self { name, tx, rx, you: usize::MAX, roster: None, hand: Vec::new(), game: None }
    }

    async fn send(&mut self, cmd: &ClientToServer) {
        let text = serde_json::to_string(cmd).expect("serializable command");
        self.tx
            .send(Message::Text(text.into()))
            .await
            .unwrap_or_else(|e| panic!("{}: send failed: {e}", self.name));
    }

    /// Pulls the next server event, failing on timeout or a dead socket.
    async fn next_event(&mut self) -> ServerToClient {
        let frame = tokio::time::timeout(TIMEOUT, self.rx.next())
            .await
            .unwrap_or_else(|_| panic!("{}: timed out waiting for an event", self.name))
            .expect("stream alive")
            .expect("read frame");
        let text = match frame {
            Message::Text(t) => t,
            other => panic!("{}: expected a text frame, got {other:?}", self.name),
        };
        serde_json::from_str(text.as_str())
            .unwrap_or_else(|e| panic!("{}: undecodable event: {e} — {text}", self.name))
    }

    /// Drains events until a `GameStateUpdate` arrives, applying `HandDealt`
    /// and `RoomState` along the way. Returns the raw JSON of the update so
    /// callers can prove every seat saw byte-identical broadcasts.
    async fn await_update(&mut self) -> String {
        loop {
            match self.next_event().await {
                ServerToClient::RoomState { room } => {
                    self.you = room.you;
                    self.roster = Some(room);
                }
                ServerToClient::HandDealt { seat, hand } => {
                    assert!(self.you != usize::MAX, "{}: hand before any roster", self.name);
                    assert_eq!(seat as usize, self.you, "{}: hand addressed to another seat", self.name);
                    self.hand = hand;
                }
                ServerToClient::MoveRejected { reason } => {
                    panic!("{}: unexpected rejection: {reason}", self.name);
                }
                ServerToClient::GameStateUpdate { state } => {
                    let json = serde_json::to_string(&state).expect("serializable");
                    self.game = Some(state);
                    return json;
                }
                ServerToClient::GameClosed { .. } | ServerToClient::OpponentDisconnected { .. } => {}
            }
        }
    }

    /// Waits for this socket's first `RoomState` and records the seat.
    async fn await_roster(&mut self) {
        loop {
            if let ServerToClient::RoomState { room } = self.next_event().await {
                self.you = room.you;
                self.roster = Some(room);
                return;
            }
        }
    }
}

/// First legal placement for `hand` against `game`, mirroring what the real
/// client's legality layer computes. `None` means the seat must pass.
fn first_legal(hand: &[PublicTile], game: &PublicGameState) -> Option<(PublicTile, Side)> {
    let fits = |tile: &PublicTile, end: u8| tile.left == end || tile.right == end;
    if game.board.is_empty() {
        return hand
            .iter()
            .find(|tile| match game.opening_tile {
                Some(required) => tile.left == required.left && tile.right == required.right,
                None => true,
            })
            .copied()
            .map(|tile| (tile, Side::Right));
    }
    for tile in hand {
        if game.right_end.is_some_and(|end| fits(tile, end)) {
            return Some((*tile, Side::Right));
        }
        if game.left_end.is_some_and(|end| fits(tile, end)) {
            return Some((*tile, Side::Left));
        }
    }
    None
}

async fn join_all(url: &str, names: [&'static str; 4]) -> Vec<MockClient> {
    let mut clients = Vec::new();
    for (index, name) in names.iter().enumerate() {
        let mut client = MockClient::connect(url, name).await;
        let cmd = ClientToServer::JoinRoom { room: "match".into(), name: (*name).into() };
        client.send(&cmd).await;
        client.await_roster().await;
        assert_eq!(client.you, index, "joins seat players in order");
        clients.push(client);
    }
    clients
}

/// Plays one hand to its end and returns the terminating snapshot.
async fn play_hand(clients: &mut [MockClient; 4]) -> PublicGameState {
    for _step in 0..400 {
        let game = clients[0].game.clone().expect("live game");
        if game.result.is_some() {
            return game;
        }
        let actor = game.turn;
        if let Some((tile, side)) = first_legal(&clients[actor].hand, &game) {
            clients[actor].send(&ClientToServer::PlayTile { tile, side }).await;
            let pos = clients[actor]
                .hand
                .iter()
                .position(|held| *held == tile)
                .expect("played tile was held");
            clients[actor].hand.remove(pos);
        } else {
            clients[actor].send(&ClientToServer::PassTurn).await;
        }
        // Every seat must observe this update; the raw JSONs must be identical.
        let json = clients[0].await_update().await;
        for client in clients.iter_mut().skip(1) {
            let other = client.await_update().await;
            assert_eq!(other, json, "{} saw a divergent broadcast", client.name);
        }
    }
    panic!("hand did not terminate within the step budget");
}

/// Four `PlayAgain` taps, then the fresh opening snapshot.
async fn rematch(clients: &mut [MockClient; 4]) -> PublicGameState {
    for client in clients.iter_mut() {
        client.send(&ClientToServer::PlayAgain).await;
    }
    let json = clients[0].await_update().await;
    for client in clients.iter_mut().skip(1) {
        let other = client.await_update().await;
        assert_eq!(other, json, "{} saw a divergent redeal", client.name);
    }
    // The private hands rode in before the snapshot; await_update applied them.
    for client in clients.iter() {
        assert_eq!(client.hand.len(), 7, "{}: redeal hand", client.name);
    }
    clients[0].game.clone().expect("redeal snapshot")
}

#[tokio::test]
async fn a_full_match_runs_over_real_websockets_and_restarts_at_zero() {
    let url = spawn_server().await;
    let mut clients: [MockClient; 4] = join_all(&url, ["Ada", "Bob", "Cleo", "Dan"])
        .await
        .try_into()
        .ok()
        .expect("four clients");
    let mut seats: Vec<usize> = clients.iter().map(|c| c.you).collect();
    seats.sort_unstable();
    assert_eq!(seats, vec![0, 1, 2, 3]);

    // The fourth join dealt: every socket still has its own rosters to drain
    // before (and around) the private hand and the opening snapshot.
    for client in clients.iter_mut() {
        if client.game.is_none() {
            client.await_update().await;
        }
        assert_eq!(client.hand.len(), 7, "{}: private hand arrived", client.name);
    }
    let opening = clients[0].game.clone().expect("opening snapshot");
    assert_eq!(
        opening.opening_tile,
        Some(PublicTile { left: 6, right: 6 }),
        "the session's first hand forces the 6|6"
    );
    assert_eq!(opening.score.points, [0, 0], "a new match starts from zero");
    let holder = clients.iter().find(|c| c.you == opening.turn).expect("opener seated");
    assert!(
        holder.hand.contains(&PublicTile { left: 6, right: 6 }),
        "the opener holds the 6|6"
    );

    let mut hands_played = 0;
    let mut last_score = [0u16; 2];
    let terminating = loop {
        hands_played += 1;
        assert!(hands_played <= 20, "the match must reach 100 within 20 hands");

        let final_state = play_hand(&mut clients).await;
        let result = final_state.result.expect("hand ended");
        let score = final_state.score;

        // --- re-verify the team-scoring law from the public data alone ---
        let team_pips = [
            u16::from(result.hand_pips[0]) + u16::from(result.hand_pips[2]),
            u16::from(result.hand_pips[1]) + u16::from(result.hand_pips[3]),
        ];
        match result.reason {
            EndReason::Abandoned => unreachable!("nobody leaves in this test"),
            EndReason::HandEmpty => {
                let out = result.out.expect("a played-out hand names its seat");
                let team = out % 2;
                assert_eq!(result.winner_team, Some(team));
                assert_eq!(
                    result.points,
                    u8::try_from(team_pips[1 - team]).expect("two hands fit"),
                    "winners bank the rival partnership's pips"
                );
            }
            EndReason::Blocked => {
                assert_eq!(result.out, None, "a blocked hand has no playing-out seat");
                if team_pips[0] == team_pips[1] {
                    assert_eq!((result.winner_team, result.points), (None, 0), "tied teams draw");
                } else {
                    let team = usize::from(team_pips[1] < team_pips[0]);
                    assert_eq!(
                        result.winner_team, Some(team),
                        "the team with fewer combined pips wins the block"
                    );
                    assert_eq!(
                        result.points,
                        u8::try_from(team_pips[1 - team]).expect("two hands fit"),
                        "winners bank the rival partnership's pips"
                    );
                }
            }
        }

        // --- score bookkeeping: each hand moves the total by its points ---
        let mut expected = last_score;
        if let Some(team) = result.winner_team {
            expected[team] += u16::from(result.points);
        }
        assert_eq!(score.points, expected, "hand {hands_played} banked into the match score");
        assert_eq!(
            score.over,
            expected.iter().any(|p| *p >= score.target),
            "the match flips to over exactly when a team reaches the target"
        );
        if score.over {
            assert!(result.winner_team.is_some(), "a decided hand clinches the match");
            assert_eq!(
                score.winner_team,
                result.winner_team.map(|team| team as u8),
                "the clinching team wins the match"
            );
            break final_state;
        }
        last_score = expected;

        // Each client's own tiles must still match the published pip totals.
        for client in clients.iter() {
            let held: u32 = client.hand.iter().map(|t| u32::from(t.left) + u32::from(t.right)).sum();
            assert_eq!(
                held,
                u32::from(result.hand_pips[client.you]),
                "{}: my tiles disagree with the published hand_pips",
                client.name
            );
        }
        let on_board = clients[0].game.as_ref().expect("snapshot").board.len();
        let in_hands: usize = clients.iter().map(|c| c.hand.len()).sum();
        assert_eq!(on_board + in_hands, 28, "tiles conserved at every socket");

        rematch(&mut clients).await;
    };

    let score = terminating.score;
    assert!(score.over);
    let winner = usize::from(score.winner_team.expect("a decided match names its team"));
    assert!(score.points[winner] >= score.target, "the named winner did reach the target");

    // "Play again" after a decided match starts match two from zero.
    let fresh = rematch(&mut clients).await;
    assert_eq!(fresh.score.points, [0, 0], "the post-match redeal resets to 0–0");
    assert!(!fresh.score.over);

    // Sockets close cleanly.
    for client in clients.iter_mut() {
        let _ = client.tx.close().await;
    }
}

#[tokio::test]
async fn binary_and_garbage_frames_are_rejected_without_killing_the_socket() {
    let url = spawn_server().await;
    let mut client = MockClient::connect(&url, "Ada").await;

    client.tx.send(Message::Binary(vec![1, 2, 3].into())).await.expect("binary send");
    let reason = match client.next_event().await {
        ServerToClient::MoveRejected { reason } => reason,
        other => panic!("expected a rejection for a binary frame, got {other:?}"),
    };
    assert!(reason.contains("binary"), "reason: {reason}");

    client.tx.send(Message::Text("not json at all".into())).await.expect("text send");
    let reason = match client.next_event().await {
        ServerToClient::MoveRejected { reason } => reason,
        other => panic!("expected a rejection for a garbage frame, got {other:?}"),
    };
    assert!(reason.contains("unrecognized"), "reason: {reason}");

    // The socket survives both insults: a real join still seats the client.
    client
        .send(&ClientToServer::JoinRoom { room: "solid".into(), name: "Ada".into() })
        .await;
    client.await_roster().await;
    assert_eq!(client.you, 0, "still seatable after bad frames");
    assert_eq!(client.roster.expect("roster").room_id, "solid");
}

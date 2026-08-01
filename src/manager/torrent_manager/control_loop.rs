use std::{
    net::IpAddr,
    time::{Duration, SystemTime},
};

use tokio::{
    net::TcpStream,
    sync::mpsc::{self, Receiver},
    time::MissedTickBehavior,
};

use crate::{
    dht::dht_manager::{DhtToTorrentManagerMsg, ToDhtManagerMsg},
    manager::{
        peer::Peer,
        peer_handler::{
            self, FastExtensionSupport, MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER,
            PeerError, PeersToManagerMsg, ToNewIncomingPeersHandlerMsg,
        },
        piece_requestor::MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER_HARD_LIMIT,
        torrent_manager::{TorrentManager, pex::PexEvent},
    },
    tracker,
};

const TICK_INTERVAL: Duration = Duration::from_secs(1);
// this capacity is enough to hold all the possible block requests that can be inflight and all the incoming requests
// that we support before a choke, still, we need extra capacity to send "have" messages in case of fast download of pieces,
// other kind of messages are really low on volume
const TO_PEER_CHANNEL_CAPACITY: usize = MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER_HARD_LIMIT
    + MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER as usize
    + 3000;
const TO_PEER_CANCEL_CHANNEL_CAPACITY: usize =
    MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER_HARD_LIMIT + 200;

impl TorrentManager {
    pub(super) async fn control_loop(
        &mut self,
        mut dht_to_torrent_manager_rx: Receiver<DhtToTorrentManagerMsg>,
    ) {
        let mut ticker = tokio::time::interval(TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                Some(msg) = self.peers_state.peers_to_torrent_manager_rx.recv() => {
                    match msg {
                        PeersToManagerMsg::Error(peer_addr, error_type) => {
                            self.handle_peer_error(peer_addr, error_type).await;
                        }
                        PeersToManagerMsg::Receive(peer_addr, msg) => {
                            self.handle_receive_message(peer_addr, msg).await;
                        }
                        PeersToManagerMsg::NewPeer(tcp_stream, supports_fast_extension) => {
                            self.handle_new_peer(tcp_stream, supports_fast_extension).await;
                        }
                        PeersToManagerMsg::PieceBlockRequestFulfilled(peer_addr) => {
                            // should we maybe use a separate channel for this?
                            self.handle_piece_block_request_fulfilled(peer_addr);
                        },
                    }
                }
                Some(msg) = self.file_manager_state.write_responses_rx.recv() => {
                    self.handle_write_piece_block_response(msg)
                    .await;
                }
                Some(msg) = self.file_manager_state.read_responses_rx.recv() => {
                    self.handle_read_piece_block_response(msg)
                    .await;
                }
                _ = ticker.tick() => {
                    self.handle_tick().await;
                }
                Some(DhtToTorrentManagerMsg::NewPeer(ip, port)) = dht_to_torrent_manager_rx.recv() => {
                    let p = tracker::Peer{peer_id: None, ip: ip.to_string(), port};
                    let mut advertised_peers_mg = self.peers_state.advertised_peers.lock().expect("another user panicked while holding the lock");
                    advertised_peers_mg.insert(format!("{ip}:{port}"), (p, SystemTime::UNIX_EPOCH));
                    drop(advertised_peers_mg);
                }
                else => break,
            }
        }
    }

    async fn handle_peer_error(&mut self, peer_addr: String, error_type: PeerError) {
        log::debug!("removing errored peer {peer_addr}");
        if error_type == PeerError::HandshakeError {
            // todo: understand other error cases that are not recoverable and should stop trying again on this peer
            self.peers_state.bad_peers.insert(peer_addr.clone());
        }
        self.remove_peer(peer_addr).await;
    }

    fn handle_piece_block_request_fulfilled(&mut self, peer_addr: String) {
        if let Some(peer) = self.peers_state.peers.get_mut(&peer_addr) {
            peer.decrease_outstanding_incoming_piece_block_requests();
        }
    }

    async fn handle_new_peer(
        &mut self,
        tcp_stream: TcpStream,
        supports_fast_extension: FastExtensionSupport,
    ) {
        let peer_addr = match tcp_stream.peer_addr() {
            Ok(s) => {
                // send to dht manager the fact that we know a new good peer
                let peer_port = s.port();
                if let IpAddr::V4(peer_addr) = s.ip() {
                    self.dht_state
                        .to_dht_manager_tx
                        .send(ToDhtManagerMsg::ConnectedToNewPeer(
                            self.torrent_manager_config.info_hash,
                            peer_addr,
                            peer_port,
                        ))
                        .await
                        .expect("to_dht_manager_tx receiver half closed");
                }
                s.to_string()
            }
            Err(e) => {
                log::trace!(
                    "new peer initialization failed because we could not get peer_addr: {e}"
                );
                return;
            }
        };
        let (to_peer_tx, to_peer_rx) = mpsc::channel(TO_PEER_CHANNEL_CAPACITY);
        let (to_peer_cancel_tx, to_peer_cancel_rx) = mpsc::channel(TO_PEER_CANCEL_CHANNEL_CAPACITY);
        peer_handler::start_peer_msg_handlers(
            peer_addr.clone(),
            tcp_stream,
            self.peers_state.peers_to_torrent_manager_tx.clone(),
            to_peer_rx,
            to_peer_cancel_rx,
            self.rate_limiter_state
                .download_rate_limiter
                .as_ref()
                .map(|a| a.clone()),
            self.rate_limiter_state
                .upload_rate_limiter
                .as_ref()
                .map(|a| a.clone()),
        );
        self.peers_state.peers.insert(
            peer_addr.clone(),
            Peer::new(
                peer_addr.clone(),
                self.torrent_data_status.as_ref().map(|f| f.num_pieces()),
                to_peer_tx,
                to_peer_cancel_tx,
                supports_fast_extension,
            ),
        );
        log::debug!("new peer initialized: {peer_addr}");
        self.new_pex_event(peer_addr, PexEvent::Added);
        if self.peers_state.peers.len() > self.torrent_manager_config.max_connected_peers {
            log::trace!("stop accepting new peers");
            self.peers_state
                .to_new_incoming_peers_handler_tx
                .send(ToNewIncomingPeersHandlerMsg::OkToAcceptConnection(false))
                .await
                .expect("to_new_incoming_peers_handler_tx receiver half closed");
        }
    }
}

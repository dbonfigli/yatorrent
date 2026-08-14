use std::{
    net::{IpAddr, Ipv4Addr},
    time::{Duration, SystemTime},
};

use tokio::{
    net::TcpStream,
    sync::mpsc::{self, UnboundedReceiver},
    time::MissedTickBehavior,
};

use crate::{
    dht::dht_manager::DhtToTorrentManagerMsg,
    manager::{
        peer::Peer,
        peer_handler::{
            self, FastExtensionSupport, MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER,
            PeerError, PeerHandlerToManagerMsg, PeerMessage, ToNewIncomingPeersHandlerMsg,
        },
        pex_handler::PexEvent,
        piece_requestor::MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER_HARD_LIMIT,
        torrent_manager::{TorrentManager, file_manager_handler::FileManagerResponse},
    },
    tracker,
    util::HostAndPort,
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
        mut dht_to_torrent_manager_rx: UnboundedReceiver<DhtToTorrentManagerMsg>,
    ) {
        let mut ticker = tokio::time::interval(TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {

                _ = ticker.tick() => {
                    self.handle_tick().await;
                }

                Some(msg) = self.file_manager_handler.recv_response() => {
                    match msg {
                        FileManagerResponse::Write(msg) => {
                           self.handle_write_piece_block_response(msg).await;
                        }
                        FileManagerResponse::Read(msg) => {
                            self.handle_read_piece_block_response(msg).await;
                        }
                    }
                }

                Some(DhtToTorrentManagerMsg::NewPeer(ip, port)) = dht_to_torrent_manager_rx.recv() => {
                    self.handle_new_peer_from_dht(ip, port);
                }

                Some(msg) = self.peers_ctx.peer_handler_to_torrent_manager_rx.recv() => {
                    match msg {
                        PeerHandlerToManagerMsg::Error(peer_addr, error_type) => {
                            self.handle_peer_error(peer_addr, error_type);
                        }
                        PeerHandlerToManagerMsg::NewPeer { tcp_stream, supports_fast_extension, listening_torrent_protocol_port } => {
                            self.handle_new_peer(tcp_stream, supports_fast_extension, listening_torrent_protocol_port).await;
                        }
                        PeerHandlerToManagerMsg::PieceBlockRequestFulfilled(peer_addr) => {
                            self.handle_piece_block_request_fulfilled(peer_addr);
                        },
                    }
                }

                Some(PeerMessage { peer_addr, message }) = self.peers_ctx.incoming_peer_messages_rx.recv() => {
                    self.handle_receive_message(peer_addr, message).await;
                }

                else => break,
            }
        }
    }

    fn handle_new_peer_from_dht(&mut self, ip: Ipv4Addr, port: u16) {
        let p = tracker::Peer {
            peer_id: None,
            ip: ip.to_string(),
            port,
        };
        let mut advertised_peers_mg = self
            .peers_ctx
            .advertised_peers
            .lock()
            .expect("another user panicked while holding the lock");
        advertised_peers_mg
            .entry(format!("{ip}:{port}"))
            .or_insert((p, SystemTime::UNIX_EPOCH));
        drop(advertised_peers_mg);
    }

    fn handle_peer_error(&mut self, peer_addr: HostAndPort, error_type: PeerError) {
        log::debug!("removing errored peer {peer_addr}");
        if error_type == PeerError::HandshakeError {
            // todo: understand other error cases that are not recoverable and should stop trying again on this peer
            if let Some(peer) = self.peers_ctx.peers.get(&peer_addr)
                && let Some(addr) = peer.get_peer_addr_and_listening_torrent_protocol_port()
            {
                self.peers_ctx.bad_peers.insert(addr);
            }
        }
        self.remove_peer(peer_addr);
    }

    fn handle_piece_block_request_fulfilled(&mut self, peer_addr: HostAndPort) {
        if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr) {
            peer.decrease_outstanding_incoming_piece_block_requests();
        }
    }

    async fn handle_new_peer(
        &mut self,
        tcp_stream: TcpStream,
        supports_fast_extension: FastExtensionSupport,
        peer_listening_torrent_protocol_port: Option<u16>,
    ) {
        let peer_addr = match tcp_stream.peer_addr() {
            Ok(s) => {
                if let Some(peer_torrent_port) = peer_listening_torrent_protocol_port {
                    if let IpAddr::V4(peer_addr) = s.ip() {
                        // send to dht manager the fact that we know a new good peer
                        self.dht_handler
                            .new_peer_connected(peer_addr, peer_torrent_port);
                        // same for pex
                        self.pex_handler.new_pex_event(
                            format!("{peer_addr}:{peer_torrent_port}"),
                            PexEvent::Added,
                        );
                    }
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
            self.peers_ctx.peer_handler_to_torrent_manager_tx.clone(),
            self.peers_ctx.incoming_peer_messages_tx.clone(),
            to_peer_rx,
            to_peer_cancel_rx,
            self.global_rate_limiter
                .download_rate_limiter
                .as_ref()
                .map(|a| a.clone()),
            self.global_rate_limiter
                .upload_rate_limiter
                .as_ref()
                .map(|a| a.clone()),
        );
        self.peers_ctx.peers.insert(
            peer_addr.clone(),
            Peer::new(
                peer_addr.clone(),
                self.torrent_data_status.as_ref().map(|f| f.num_pieces()),
                to_peer_tx,
                to_peer_cancel_tx,
                supports_fast_extension,
                peer_listening_torrent_protocol_port,
            ),
        );
        log::debug!("new peer initialized: {peer_addr}");
        if self.peers_ctx.peers.len() > self.torrent_manager_config.max_connected_peers {
            log::trace!("stop accepting new peers");
            self.peers_ctx
                .to_new_incoming_peers_handler_tx
                .send(ToNewIncomingPeersHandlerMsg::OkToAcceptConnection(false))
                .expect("to_new_incoming_peers_handler_tx receiver half closed");
        }
    }
}

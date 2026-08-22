use std::{
    net::{IpAddr, Ipv4Addr},
    time::Duration,
};

use tokio::{net::TcpStream, sync::mpsc, time::MissedTickBehavior};

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
        torrent_manager::{
            ShutdownRequest, TorrentManager, UnrecoverableError,
            file_manager_handler::FileManagerResponse,
        },
    },
    tracker,
    util::{HostAndPort, pretty_info_hash},
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

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(windows)]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for Ctrl+C");
    }
}

impl TorrentManager {
    pub(super) async fn control_loop(&mut self) -> Result<(), UnrecoverableError> {
        let mut result = Ok(());

        let mut ticker = tokio::time::interval(TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let shutdown = shutdown_signal();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                biased;

                _ = &mut shutdown => {
                    log::info!("shutdown requested by user");
                    break;
                }

                Some(shutdown_request) = self.shutdown_request_rx.recv() => {
                    if let ShutdownRequest::Error(e) = shutdown_request {
                        result = Err(e);
                    }
                    break;
                }

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

                Some(DhtToTorrentManagerMsg::NewPeer(ip, port)) = self.dht_handler.recv_new_peer() => {
                    self.handle_new_peer_from_dht(ip, port);
                }

                Some(msg) = self.peers_channels.peer_handler_to_torrent_manager_rx.recv() => {
                    match msg {
                        PeerHandlerToManagerMsg::Error(peer_addr, error_type) => {
                            self.handle_peer_error(peer_addr, error_type);
                        }
                        PeerHandlerToManagerMsg::NewPeer { tcp_stream, peer_id, supports_fast_extension, listening_torrent_protocol_port } => {
                            self.handle_new_peer(tcp_stream, peer_id, supports_fast_extension, listening_torrent_protocol_port).await;
                        }
                        PeerHandlerToManagerMsg::PieceBlockRequestFulfilled(peer_addr) => {
                            self.handle_piece_block_request_fulfilled(peer_addr);
                        },
                    }
                }

                Some(PeerMessage { peer_addr, message }) = self.peers_channels.incoming_peer_messages_rx.recv() => {
                    self.handle_receive_message(peer_addr, message).await;
                }

                else => break,
            }
        }

        // graceful shutdown
        self.shutdown().await;
        result
    }

    fn handle_new_peer_from_dht(&mut self, ip: Ipv4Addr, port: u16) {
        self.peers_ctx.advertised_peers.insert(vec![tracker::Peer {
            peer_id: None,
            ip: ip.to_string(),
            port,
        }]);
    }

    fn handle_peer_error(&mut self, peer_addr: HostAndPort, error_type: PeerError) {
        log::debug!("removing errored peer {peer_addr}");
        if error_type == PeerError::SelfInitiatedHandshakeError {
            // todo: understand other error cases that are not recoverable and should stop trying again on this peer
            self.peers_ctx.insert_bad_peer(peer_addr.clone());
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
        peer_id: [u8; 20],
        supports_fast_extension: FastExtensionSupport,
        peer_listening_torrent_protocol_port: Option<u16>,
    ) {
        // disconnect if we detect via peer_id that we are already connected to this peer
        if self
            .peers_ctx
            .peers
            .iter()
            .find(|(_, p)| p.peer_id() == peer_id)
            .is_some()
        {
            log::debug!(
                "new peer initialization failed because we are already conneted to this peer with peer_id {}",
                pretty_info_hash(peer_id)
            );
            return;
        }

        let peer_addr = match tcp_stream.peer_addr() {
            Ok(s) => {
                if let Some(peer_torrent_port) = peer_listening_torrent_protocol_port
                    && let IpAddr::V4(peer_addr) = s.ip()
                {
                    // send to dht manager the fact that we know a new good peer
                    self.dht_handler
                        .new_peer_connected(peer_addr, peer_torrent_port);
                    // same for pex
                    self.pex_handler
                        .new_pex_event(format!("{peer_addr}:{peer_torrent_port}"), PexEvent::Added);
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
            self.peers_channels
                .peer_handler_to_torrent_manager_tx
                .clone(),
            self.peers_channels.incoming_peer_messages_tx.clone(),
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
                peer_id,
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
            self.peers_channels
                .to_new_incoming_peers_handler_tx
                .send(ToNewIncomingPeersHandlerMsg::OkToAcceptConnection(false))
                .expect("to_new_incoming_peers_handler_tx receiver half closed");
        }
    }

    pub fn send_shutdown_request(&mut self, shutdown_request: ShutdownRequest) {
        self.shutdown_request_tx
            .send(shutdown_request)
            .expect("shutdown_request_tx receiver half closed");
    }

    async fn shutdown(&mut self) {
        log::info!("sending stopped event to tracker...");

        match self
            .tracker_requestor
            .async_request_to_tracker(
                tracker::Event::Stopped,
                &self.peers_ctx.advertised_peers,
                self.torrent_data_status.as_ref().map(|f| f.bytes_left()),
                (
                    self.bandwidth_tracker.uploaded_bytes(),
                    self.bandwidth_tracker.downloaded_bytes(),
                ),
            )
            .await
        {
            Some(tracker_req_join_handle) => {
                tokio::select!(
                    _ = tokio::signal::ctrl_c() => {
                        // on a second Ctrl+C, we just give up waiting for the tracker request
                        return;
                    }
                    _ = tracker_req_join_handle => {
                        // tracker request completed
                        return;
                    }
                )
            }
            None => return,
        }
    }
}

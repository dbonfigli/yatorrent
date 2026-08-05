use std::time::{Duration, SystemTime};

use crate::{
    dht::dht_manager::ToDhtManagerMsg,
    manager::{
        peer_handler::{self, ToPeerMsg},
        torrent_manager::{DHT_NEW_PEER_COOL_OFF_PERIOD, TorrentManager, util::should_choke},
    },
    torrent_protocol::wire_protocol::Message,
};
use rand::seq::IndexedRandom;

const ENDGAME_REQUEST_TIMEOUT: Duration = Duration::from_secs(15); // request timeout during the endgame phase: this will re-request a lot of pieces, wasting bandwidth, but will make endgame faster churning slow peers
const NEW_CONNECTION_COOL_OFF_PERIOD: Duration = Duration::from_secs(180); // time to wait before attempting a new connection to a non bad (i.e. with no permanent errors) peer
const MIN_CHOKE_TIME: Duration = Duration::from_secs(10);
const ENDGAME_START_AT_COMPLETION_PERCENTAGE: f64 = 98.; // start endgame when we have this percentage of the torrent
const MAX_CONNECTED_PEERS_TO_ASK_DHT_FOR_MORE: usize = 10;

impl TorrentManager {
    pub(super) async fn handle_tick(&mut self) {
        self.update_bandwidth_stats();
        self.log_stats();
        self.log_peers_stats();
        self.connect_to_new_peers().await;
        self.send_keep_alives().await;
        self.send_status_to_tracker().await;
        self.unchoke_peers().await;
        self.check_endgame_status().await;
        self.request_new_peers_to_dht_manager().await;
        self.pex_handler
            .send_pex_messages(&mut self.peers_state.peers)
            .await;
        self.metadata_state
            .send_metadata_reqs(&mut self.peers_state.peers)
            .await;

        self.send_pieces_reqs().await;
    }

    fn update_bandwidth_stats(&mut self) {
        self.bandwidth_tracker.update();
        for (_, peer) in self.peers_state.peers.iter_mut() {
            peer.get_bandwidth_tracker_mut().update();
        }
    }

    async fn connect_to_new_peers(&mut self) {
        let current_peers_n = self.peers_state.peers.len();
        if current_peers_n < self.torrent_manager_config.max_connected_peers {
            let possible_peers_mg = self
                .peers_state
                .advertised_peers
                .lock()
                .expect("another user panicked while holding the lock");
            let possible_peers = possible_peers_mg.clone();
            let now = SystemTime::now();
            drop(possible_peers_mg);
            let possible_peers = possible_peers
                .iter()
                .filter(|(k, (_, last_connection_attempt))| {
                    // avoid selecting peers we are already connected to
                    !self.peers_state.peers.contains_key(*k)
                    // avoid selecting peers we know are bad
                    && !self.peers_state.bad_peers.contains(*k)
                    // use peers we didn't try to connect to recently
                    // this cool-off time is also important to avoid new connections to peers we attempted few secs ago
                    // and for which a connection attempt is still inflight
                    && now.duration_since(*last_connection_attempt).unwrap_or_default() > NEW_CONNECTION_COOL_OFF_PERIOD
                })
                .collect::<Vec<_>>();

            log::debug!(
                "trying to connect to {} peers",
                self.torrent_manager_config.max_connected_peers - current_peers_n
            );
            let candidates_for_new_connections: Vec<_> = possible_peers
                .sample(
                    &mut rand::rng(),
                    self.torrent_manager_config.max_connected_peers - current_peers_n,
                )
                .collect();
            // todo: better algorithm to select new peers
            for (_, (peer, _)) in candidates_for_new_connections.iter() {
                tokio::spawn(peer_handler::connect_to_new_peer(
                    peer.ip.clone(),
                    peer.port,
                    self.torrent_manager_config.info_hash,
                    self.torrent_manager_config.own_peer_id.clone(),
                    self.torrent_manager_config
                        .listening_torrent_wire_protocol_port,
                    self.torrent_data_status
                        .as_ref()
                        .map(|f| f.current_piece_completion_status()),
                    self.metadata_state.raw_metadata_size(),
                    self.peers_state.peers_to_torrent_manager_tx.clone(),
                ));
            }
            // update last connection attempt
            let mut possible_peers_mg = self
                .peers_state
                .advertised_peers
                .lock()
                .expect("another user panicked while holding the lock");
            for (peer_addr, _) in candidates_for_new_connections.iter() {
                let possible_peer_entry = possible_peers_mg
                    .get_mut(*peer_addr)
                    .expect("we filtered on this above");
                possible_peer_entry.1 = now;
            }
            drop(possible_peers_mg);
        }
    }

    async fn send_keep_alives(&mut self) {
        for (_, peer) in self.peers_state.peers.iter_mut() {
            peer.send_keepalive().await;
        }
    }

    async fn unchoke_peers(&mut self) {
        let now = SystemTime::now();
        for (_, peer) in self.peers_state.peers.iter_mut() {
            if peer.get_am_choking()
                && now
                    .duration_since(peer.get_am_choking_since())
                    .unwrap_or_default()
                    > MIN_CHOKE_TIME
                && !should_choke(
                    self.peers_state.peers_to_torrent_manager_tx.capacity(),
                    self.file_manager_state.read_requests_capacity(),
                    peer.get_outstanding_incoming_piece_block_requests(),
                    self.outstanding_read_ops,
                    self.torrent_data_status.is_some(),
                )
            {
                peer.set_am_choking(false);
                peer.send(ToPeerMsg::Send(Message::Unchoke)).await;
            }
        }
    }

    async fn check_endgame_status(&mut self) {
        // check endgame status a decrease request timeout if needed
        if let Some(torrent_data_status) = &self.torrent_data_status {
            if !torrent_data_status.completed() && self.request_timeout != ENDGAME_REQUEST_TIMEOUT {
                let completed_pieces = torrent_data_status.completed_pieces();
                let total_pieces = torrent_data_status.num_pieces();
                if (completed_pieces as f64) / (total_pieces as f64) * 100.
                    > ENDGAME_START_AT_COMPLETION_PERCENTAGE
                {
                    log::warn!("entering endgame phase");
                    self.request_timeout = ENDGAME_REQUEST_TIMEOUT;
                }
            }
        }
    }

    async fn request_new_peers_to_dht_manager(&mut self) {
        let now = SystemTime::now();
        if self.peers_state.peers.len() < MAX_CONNECTED_PEERS_TO_ASK_DHT_FOR_MORE
            && now
                .duration_since(self.dht_state.last_get_peers_requested_time)
                .unwrap_or_default()
                > DHT_NEW_PEER_COOL_OFF_PERIOD
        {
            self.dht_state.last_get_peers_requested_time = now;
            let _ = self
                .dht_state
                .to_dht_manager_tx
                .send(ToDhtManagerMsg::GetNewPeers(
                    self.torrent_manager_config.info_hash,
                ))
                .await;
        }
    }

    async fn send_pieces_reqs(&mut self) {
        let torrent_data_status = match &self.torrent_data_status {
            Some(torrent_data_status) => torrent_data_status,
            None => return,
        };

        // remove requests to choked peers that have been lingering for some time
        // or requests that have not been fulfilled for some time, even if unchoked,
        // most probably they have been silently dropped by the peer even if it is still alive
        let expired_piece_blocks_requests = self
            .piece_requestor
            .remove_stale_requests(self.request_timeout, &self.peers_state.peers);

        if self.request_timeout != ENDGAME_REQUEST_TIMEOUT {
            // during endgame we shorten the timeout, we cannot really affort canceling requests since they could come later with such short timeout
            // todo: this is really bad, in theory "cancel" exists basically to avoid being horribly inefficent during the endgame, we are really misbheaving here
            // we should find a way to play nicer here
            for (peer_addr, req) in expired_piece_blocks_requests {
                if let Some(peer) = self.peers_state.peers.get_mut(&peer_addr) {
                    peer.send(ToPeerMsg::Send(Message::Cancel(req))).await;
                }
            }
        }

        // compute requests from piece requestor
        let reqs_to_send = self
            .piece_requestor
            .generate_requests_to_send(&self.peers_state.peers, torrent_data_status);

        // finally send requests
        for (peer_addr, block_requests) in reqs_to_send {
            if let Some(peer) = self.peers_state.peers.get_mut(&peer_addr) {
                for block_request in block_requests {
                    peer.send(ToPeerMsg::Send(Message::Request(block_request)))
                        .await;
                }
            } else {
                log::warn!(
                    "could not find peer for which piece_requestor assigned requests, this should never happen"
                );
                self.piece_requestor.remove_assigments_to_peer(&peer_addr);
            }
        }
    }

    async fn send_status_to_tracker(&mut self) {
        self.tracker_state
            .async_update_to_tracker(
                self.peers_state.advertised_peers.clone(),
                self.torrent_data_status.as_ref().map(|f| f.bytes_left()),
                (
                    self.bandwidth_tracker.uploaded_bytes(),
                    self.bandwidth_tracker.downloaded_bytes(),
                ),
            )
            .await;
    }
}

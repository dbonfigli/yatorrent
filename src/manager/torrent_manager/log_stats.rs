use std::time::{Duration, SystemTime};

use colored::Colorize;
use size::{Size, Style};

use crate::manager::{
    peer::Peer,
    torrent_manager::{INCOMING_PEER_MESSAGES_CHANNEL_CAPACITY, TorrentManager},
};

const TIME_TO_CONSIDER_DOWNLOAD_STALLED: Duration = Duration::from_secs(15);

impl TorrentManager {
    pub(super) fn log_stats(&self) {
        log::info!(
            "left: {left}, pieces: {completed_pieces}/{total_pieces}{metadata_pieces} | {bandwidth_tracker}{wasted} | known peers: {known_peers} (bad: {bad_peers}), connected: {connected_peers}, unchoked: {unchoked_peers} | pending msgs: incoming_peer_messages {cur_ch_cap}; read_reqs {read_reqs} (inflight: {inflight_read_ops}); write_reqs {write_reqs}",
            left = self
                .torrent_data_status
                .as_ref()
                .map(|f| Size::from_bytes(f.bytes_left()).to_string())
                .unwrap_or("?".to_string()),
            completed_pieces = self
                .torrent_data_status
                .as_ref()
                .map(|f| f.completed_pieces())
                .unwrap_or(0),
            total_pieces = self
                .torrent_data_status
                .as_ref()
                .map(|f| f.num_pieces().to_string())
                .unwrap_or("?".to_string()),
            metadata_pieces = match self.metadata_handler.total_metadata_pieces() {
                0 => format!(
                    ", metadata pieces: {}/?",
                    self.metadata_handler.total_metadata_pieces_downloaded()
                ),
                total_metadata_pieces => {
                    let total_downloaded = self.metadata_handler.total_metadata_pieces_downloaded();
                    if total_metadata_pieces == total_downloaded {
                        "".to_string()
                    } else {
                        format!(", metadata pieces: {total_downloaded}/{total_metadata_pieces}")
                    }
                }
            },
            bandwidth_tracker = self.bandwidth_tracker,
            wasted = match self
                .torrent_data_status
                .as_ref()
                .map(|f| f.wasted_bytes())
                .unwrap_or(0)
            {
                0 => "".to_string(),
                w => format!(
                    ", wasted: {}",
                    Size::from_bytes(w).format().with_style(Style::Abbreviated)
                ),
            },
            known_peers = self.peers_ctx.advertised_peers.len(),
            bad_peers = self.peers_ctx.bad_peers.len(),
            connected_peers = self.peers_ctx.peers.len(),
            unchoked_peers =
                self.peers_ctx
                    .peers
                    .iter()
                    .fold(0, |acc, (_, p)| if !p.is_peer_choking() {
                        acc + 1
                    } else {
                        acc
                    }),
            cur_ch_cap = INCOMING_PEER_MESSAGES_CHANNEL_CAPACITY
                - self.peers_ctx.incoming_peer_messages_tx.capacity(),
            read_reqs = self.file_manager_handler.inflight_read_reqs(),
            inflight_read_ops = self.file_manager_handler.outstanding_read_ops(),
            write_reqs = self.file_manager_handler.inflight_write_reqs(),
        );
    }

    pub(super) fn log_peers_stats(&self) {
        if !self.torrent_manager_config.show_peers_stats {
            return;
        }

        for (peer_addr, peer) in self.peers_ctx.peers.iter() {
            let pending_block_requests = self
                .piece_requestor
                .get_pending_block_requests_for_peer(peer_addr);
            let bandwidth_up = peer.get_bandwidth_tracker().bandwidth_up();
            let bandwidth_down = peer.get_bandwidth_tracker().bandwidth_down();
            if (!peer.is_peer_choking() && pending_block_requests > 0)
                || bandwidth_down > 0.0
                || bandwidth_up > 0.0
            {
                log::info!(
                    "{peer_addr:>22} rtt: {rtt}, {bandwidth_tracker}, pending blocks: {pending_block_requests} (on {assigned_pieces} pieces), haves: {have}, client: {client_version}{choking}{stalled}",
                    bandwidth_tracker = peer.get_bandwidth_tracker(),
                    assigned_pieces = self.piece_requestor.get_assigned_pieces_for_peer(peer_addr),
                    client_version = peer.get_client_version(),
                    choking = if peer.is_peer_choking() {
                        format!("{}{}", ", ".normal(), "choked".yellow())
                    } else {
                        "".to_string()
                    },
                    have = peer.have_count(),
                    rtt = peer.get_rtt().as_ref().map_or("?".normal(), |rtt| {
                        if *rtt > Duration::from_secs(10) {
                            format!("{:.3}s", rtt.as_secs_f64()).yellow()
                        } else {
                            format!("{:.3}s", rtt.as_secs_f64()).normal()
                        }
                    }),
                    stalled = if self.peer_download_stalled(peer) {
                        format!("{}{}", ", ".normal(), "stalled".red())
                    } else {
                        "".to_string()
                    },
                )
            }
        }
    }

    fn peer_download_stalled(&self, peer: &Peer) -> bool {
        self.piece_requestor
            .get_pending_block_requests_for_peer(&peer.get_peer_addr())
            > 0
            && SystemTime::now()
                .duration_since(peer.get_bandwidth_tracker().last_download_increase_time())
                .unwrap_or_default()
                > TIME_TO_CONSIDER_DOWNLOAD_STALLED
    }
}

use crate::manager::{
    peer_handler::{
        MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER, ToNewIncomingPeersHandlerMsg,
    },
    torrent_manager::{PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY, TorrentManager, pex::PexEvent},
};

// maximum allowed number of read operations in flight across all peers; i.e., read requests that have been sent to the file manager and whose responses have not yet been handled by the torrent manager.
// We use this to limit memory usage since read_responses channel is unbounded to avoid deadlock.
// See also MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER that is a similar limit (but different: it includes the time to also send the data), per peer.
const MAX_OUTSTANDING_READ_OPS: usize = 3000;

pub(super) fn should_choke(
    peers_to_torrent_manager_channel_capacity: usize,
    read_requests_channel_capacity: usize,
    outstanding_incoming_piece_block_requests_for_this_peer: usize,
    outstanding_read_ops: usize,
    file_manager_initialized: bool,
) -> bool {
    peers_to_torrent_manager_channel_capacity < PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY / 2
        || !file_manager_initialized
        || outstanding_incoming_piece_block_requests_for_this_peer
            > MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER as usize
        || read_requests_channel_capacity == 0
        || outstanding_read_ops > MAX_OUTSTANDING_READ_OPS
}

impl TorrentManager {
    pub(super) async fn remove_peer(&mut self, peer_addr: String) {
        self.pex_handler
            .new_pex_event(peer_addr.clone(), PexEvent::Dropped);
        if let Some(_) = self.peers_state.peers.remove(&peer_addr) {
            self.piece_requestor.remove_assigments_to_peer(&peer_addr);
        }
        if self.peers_state.peers.len() < self.torrent_manager_config.max_connected_peers {
            self.peers_state
                .to_new_incoming_peers_handler_tx
                .send(ToNewIncomingPeersHandlerMsg::OkToAcceptConnection(true))
                .await
                .expect("to_new_incoming_peers_handler_tx receiver half closed");
        }
    }
}

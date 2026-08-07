use crate::{
    manager::{
        peer_handler::{
            MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER, ToNewIncomingPeersHandlerMsg,
        },
        pex_handler::PexEvent,
        torrent_manager::{
            FileManagerHandler, PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY, TorrentManager,
        },
    },
    util::HostAndPort,
};

pub(super) fn should_choke(
    peers_to_torrent_manager_channel_capacity: usize,
    outstanding_incoming_piece_block_requests_for_this_peer: usize,
    file_manager_initialized: bool,
    file_manager_handler: &FileManagerHandler,
) -> bool {
    peers_to_torrent_manager_channel_capacity < PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY / 2
        || !file_manager_initialized
        || outstanding_incoming_piece_block_requests_for_this_peer
            > MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER as usize
        || file_manager_handler.read_saturated()
}

impl TorrentManager {
    pub(super) async fn remove_peer(&mut self, peer_addr: HostAndPort) {
        self.pex_handler
            .new_pex_event(peer_addr.clone(), PexEvent::Dropped);
        if self.peers_ctx.peers.remove(&peer_addr).is_some() {
            self.piece_requestor.remove_assigments_to_peer(&peer_addr);
        }
        if self.peers_ctx.peers.len() < self.torrent_manager_config.max_connected_peers {
            self.peers_ctx
                .to_new_incoming_peers_handler_tx
                .send(ToNewIncomingPeersHandlerMsg::OkToAcceptConnection(true))
                .await
                .expect("to_new_incoming_peers_handler_tx receiver half closed");
        }
    }
}

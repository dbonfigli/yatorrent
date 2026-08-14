use crate::{
    manager::{
        peer_handler::{
            MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER, ToNewIncomingPeersHandlerMsg,
        },
        pex_handler::PexEvent,
        torrent_manager::{
            FileManagerHandler, INCOMING_PEER_MESSAGES_CHANNEL_CAPACITY, TorrentManager,
        },
    },
    util::HostAndPort,
};

pub(super) fn should_choke(
    incoming_peer_messages_channel_capacity: usize,
    outstanding_incoming_piece_block_requests_for_this_peer: usize,
    file_manager_initialized: bool,
    file_manager_handler: &FileManagerHandler,
) -> bool {
    incoming_peer_messages_channel_capacity < INCOMING_PEER_MESSAGES_CHANNEL_CAPACITY / 2
        || !file_manager_initialized
        || outstanding_incoming_piece_block_requests_for_this_peer
            > MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER as usize
        || file_manager_handler.read_saturated()
}

impl TorrentManager {
    pub(super) fn remove_peer(&mut self, peer_addr: HostAndPort) {
        if let Some(peer) = self.peers_ctx.peers.remove(&peer_addr) {
            self.piece_requestor.remove_assigments_to_peer(&peer_addr);
            if let Some(addr) = peer.get_peer_addr_and_listening_torrent_protocol_port() {
                self.pex_handler.new_pex_event(addr, PexEvent::Dropped);
                // todo we should also inform dht handler about this? maybe not, since it could be that the removal was "benign"
            }
        }

        if self.peers_ctx.peers.len() < self.torrent_manager_config.max_connected_peers {
            self.peers_ctx
                .to_new_incoming_peers_handler_tx
                .send(ToNewIncomingPeersHandlerMsg::OkToAcceptConnection(true))
                .expect("to_new_incoming_peers_handler_tx receiver half closed");
        }
    }
}

use std::process;

use crate::{
    manager::{
        peer::Peer,
        peer_handler::{ToNewIncomingPeersHandlerMsg, ToPeerMsg},
        torrent_manager::TorrentManager,
    },
    persistence::{
        file_manager::{ReadPieceBlockResponse, ShaCorruptedError, WritePieceBlockResponse},
        torrent_data_status::TorrentDataStatus,
    },
    torrent_protocol::wire_protocol::Message,
    tracker::Event,
};

const MAX_CORRUPTION_ERRORS: u32 = 20; // max sha1 corruption errors on blocks a peer can have before marking it as bad

impl TorrentManager {
    pub(super) async fn handle_write_piece_block_response(
        &mut self,
        write_piece_block_response: WritePieceBlockResponse,
    ) {
        match self.torrent_data_status.as_mut() {
            Some(f) => f.update(&write_piece_block_response),
            None => return,
        }

        let piece_idx = write_piece_block_response.request.piece_idx;
        let peer_addr = write_piece_block_response.request.requestor_peer_addr;
        match write_piece_block_response.response {
            Ok(torrent_data_status_updates) => {
                if torrent_data_status_updates.piece_is_completed {
                    self.piece_requestor
                        .piece_request_completed(&peer_addr, piece_idx);

                    if self
                        .torrent_data_status
                        .as_mut()
                        .expect("invariant checked above")
                        .completed()
                        && !self.tracker_state.completed_sent_to_tracker
                    {
                        log::warn!("torrent download completed");
                        self.tracker_state.completed_sent_to_tracker = true;
                        self.async_request_to_tracker(Event::Completed).await;
                        if self.torrent_manager_config.exit_when_complete {
                            log::warn!("Exiting...");
                            process::exit(0);
                        }
                    }

                    let _ = self
                        .peers_state
                        .to_new_incoming_peers_handler_tx
                        .send(ToNewIncomingPeersHandlerMsg::PieceCompleted(piece_idx))
                        .await;

                    for (_, peer) in self.peers_state.peers.iter_mut() {
                        // send "have" to all peers.
                        // it can happen on very fast downloads that "have" messages overwhelm the channel,
                        // discard the message in those cases to not block the loop
                        peer.try_send(ToPeerMsg::Send(Message::Have(piece_idx as u32)));

                        // send "not interested" if needed
                        if peer.get_am_interested()
                            && !peer_has_pieces_we_dont_have(
                                peer,
                                self.torrent_data_status
                                    .as_mut()
                                    .expect("invariant checked above"),
                            )
                        {
                            log::trace!("sending not interested to {}", peer.get_peer_addr());
                            peer.set_am_interested(false);
                            peer.send(ToPeerMsg::Send(Message::NotInterested)).await;
                        }
                    }
                }
            }
            Err(e) => {
                log::error!("cannot write block received from {}: {e}", peer_addr);

                // keep track of corruptions, remove if too many
                if let Some(_) = e.downcast_ref::<ShaCorruptedError>() {
                    let peer = match self.peers_state.peers.get_mut(&peer_addr) {
                        Some(peer) => peer,
                        None => return,
                    };
                    peer.increase_corruption_errors();
                    if peer.get_corruption_errors() > MAX_CORRUPTION_ERRORS {
                        log::warn!(
                            "removing peer {peer_addr} due to too many corrupted pieces received",
                        );
                        peer.send(ToPeerMsg::Disconnect()).await;
                        self.remove_peer(peer_addr).await;
                    }
                }
            }
        }
    }

    pub(super) async fn handle_read_piece_block_response(
        &mut self,
        read_piece_block_response: ReadPieceBlockResponse,
    ) {
        self.outstanding_read_ops = self.outstanding_read_ops.saturating_sub(1);

        let peer = match self
            .peers_state
            .peers
            .get_mut(&read_piece_block_response.request.requestor_peer_addr)
        {
            Some(peer) => peer,
            None => return,
        };

        match read_piece_block_response.response {
            Err(e) => {
                log::error!("error reading block: {e}");
                peer.decrease_outstanding_incoming_piece_block_requests();
            }

            Ok(data) => {
                let data_len = data.len() as u64;
                peer.send(ToPeerMsg::Send(Message::Piece(
                    read_piece_block_response.request.piece_idx as u32,
                    read_piece_block_response.request.block_begin as u32,
                    data,
                )))
                .await;
                peer.get_bandwidth_tracker_mut()
                    .add_uploaded_bytes(data_len);
                self.bandwidth_tracker.add_uploaded_bytes(data_len); // todo: we are not keeping track of cancelled pieces
            }
        }
    }
}

fn peer_has_pieces_we_dont_have(peer: &Peer, torrent_data_status: &TorrentDataStatus) -> bool {
    for piece_idx in torrent_data_status.missing_pieces() {
        if peer.have_piece(*piece_idx) {
            return true;
        }
    }
    false
}

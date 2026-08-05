use std::collections::HashMap;
use std::time::SystemTime;

use anyhow::Error;
use sha1::{Digest, Sha1};

use crate::manager::metadata_handler::MetadataHandler;
use crate::manager::peer::{
    METADATA_MESSAGE_DATA, METADATA_MESSAGE_REJECT, METADATA_MESSAGE_REQUEST, MetadataMessage, Peer,
};
use crate::manager::peer_handler::{PeerAddr, ToNewIncomingPeersHandlerMsg};
use crate::manager::torrent_manager::{FileManagerState, PeersState, TorrentManagerConfig};
use crate::metadata::infodict;
use crate::persistence::file_manager::start_file_manager;
use crate::persistence::torrent_data_status::TorrentDataStatus;
use crate::{
    bencoding::Value::{self, Dict, Int},
    metadata::metainfo::get_files,
};

pub(super) struct ExtendedMetadataMessageContext<'a, 'b, 'c, 'd> {
    pub(super) torrent_manager_config: &'a TorrentManagerConfig,
    pub(super) peers_state: &'b mut PeersState,
    pub(super) file_manager_state: &'c mut FileManagerState,
    pub(super) torrent_data_status: &'d mut Option<TorrentDataStatus>,
}

pub(super) struct MetadataState {
    metadata_handler: MetadataHandler,
}

impl MetadataState {
    pub(super) fn new(raw_metadata_size: Option<i64>, raw_metadata: Option<Vec<u8>>) -> Self {
        MetadataState {
            metadata_handler: MetadataHandler::new(raw_metadata_size, raw_metadata),
        }
    }

    pub(super) fn update_raw_metadata_size(&mut self, metadata_size: i64) {
        if self.metadata_handler.raw_metadata_size().is_none() {
            // we do not know the metadata size yet, take notes
            self.metadata_handler = MetadataHandler::new(Some(metadata_size), None);
        }
    }

    pub(super) fn raw_metadata_size(&self) -> Option<i64> {
        self.metadata_handler.raw_metadata_size()
    }

    pub(super) fn total_metadata_pieces(&self) -> usize {
        self.metadata_handler.total_metadata_pieces()
    }

    pub(super) fn total_metadata_pieces_downloaded(&self) -> usize {
        self.metadata_handler.total_metadata_pieces_downloaded()
    }

    pub(super) async fn send_metadata_reqs(&mut self, peers: &mut HashMap<PeerAddr, Peer>) {
        if self.metadata_handler.full_metadata_known() {
            return;
        }
        // we still have to download the metadata, ask metadata pieces to peers
        let new_medatada_piece_requests = self.metadata_handler.generate_metadata_piece_reqs(peers);
        for (peer_addr, piece_to_request) in new_medatada_piece_requests {
            if let Some(peer) = peers.get_mut(&peer_addr) {
                log::debug!("sending metadata piece request to {peer_addr}: {piece_to_request}");
                peer.send_metadata_extension_message(MetadataMessage::Request(
                    piece_to_request as u64,
                ))
                .await;
            }
        }
    }

    pub(super) async fn handle_receive_extended_message_ut_metadata<'a, 'b, 'c, 'd>(
        &mut self,
        value: Value,
        peer_addr: String,
        additional_data: Vec<u8>,
        context: ExtendedMetadataMessageContext<'a, 'b, 'c, 'd>,
    ) {
        let d = match value {
            Dict(d, _, _) => d,
            _ => {
                log::debug!(
                    "got an ut_metadataextension message from {peer_addr}, it was a bencoded value but not a dict dict, ignoring it"
                );
                return;
            }
        };
        let msg_type = match d.get(&b"msg_type".to_vec()) {
            Some(Int(msg_type)) => msg_type,
            _ => {
                log::debug!(
                    "got an ut_metadataextension message from {peer_addr} but no msg_type key was found in the bencoded dict, ignoring it"
                );
                return;
            }
        };
        let piece = match d.get(&b"piece".to_vec()) {
            Some(Int(piece)) => piece,
            _ => {
                log::debug!(
                    "got an ut_metadataextension message from {peer_addr} but no piece key was found in the bencoded dict, ignoring it"
                );
                return;
            }
        };
        match *msg_type {
            METADATA_MESSAGE_REQUEST => {
                let peer = match context.peers_state.peers.get_mut(&peer_addr) {
                    Some(peer) => peer,
                    None => return,
                };
                self.handle_receive_extended_message_metadata_message_request(peer, *piece)
                    .await;
            }
            METADATA_MESSAGE_DATA => {
                let metadata_size = match d.get(&b"total_size".to_vec()) {
                    Some(Int(metadata_size)) => metadata_size,
                    _ => {
                        log::debug!(
                            "got a metadata message data without the required total_size field or total_size is not an integer, ignoring it"
                        );
                        return;
                    }
                };
                self.handle_receive_extended_message_metadata_message_data(
                    *metadata_size,
                    *piece,
                    additional_data,
                    context,
                )
                .await;
            }
            METADATA_MESSAGE_REJECT => {
                if let Some(peer) = context.peers_state.peers.get_mut(&peer_addr) {
                    peer.set_last_metadata_request_rejection(SystemTime::now());
                }
            }
            _ => {
                log::debug!(
                    "got an ut_metadataextension message from {peer_addr} but msg_type was not recognized as an extension we registered: {msg_type}"
                );
            }
        }
    }

    async fn handle_receive_extended_message_metadata_message_request(
        &mut self,
        peer: &mut Peer,
        piece_idx: i64,
    ) {
        match self.metadata_handler.get_piece(piece_idx as usize) {
            None => {
                log::debug!(
                    "rejecting metadata message request for {piece_idx} piece from {}: full metadata not yet known or requested pieces is out of range",
                    peer.get_peer_addr()
                );
                peer.send_metadata_extension_message(MetadataMessage::Reject(piece_idx as u64))
                    .await
            }
            Some((piece, raw_metadata_size)) => {
                peer.send_metadata_extension_message(MetadataMessage::Data(
                    piece_idx as u64,
                    raw_metadata_size as u64,
                    piece,
                ))
                .await
            }
        }
    }

    async fn handle_receive_extended_message_metadata_message_data<'a, 'b, 'c, 'd>(
        &mut self,
        raw_metadata_size: i64,
        piece_idx: i64,
        piece_data: Vec<u8>,
        context: ExtendedMetadataMessageContext<'a, 'b, 'c, 'd>,
    ) {
        let raw_metadata_size = match self.metadata_handler.raw_metadata_size() {
            Some(raw_metadata_size) => raw_metadata_size,
            None => {
                // we do not know the metadata size yet, take notes
                self.metadata_handler = MetadataHandler::new(Some(raw_metadata_size), None);
                raw_metadata_size
            }
        };

        if self.metadata_handler.full_metadata_known() || piece_idx < 0 {
            // we are not interested in this message
            return;
        }

        self.metadata_handler
            .insert_piece(piece_idx as usize, piece_data);

        // check if metadata is complete
        if !self.metadata_handler.full_metadata_known() {
            return;
        }

        let raw_metadata = self
            .metadata_handler
            .get_raw_metadata()
            .as_ref()
            .expect("it must exist, full metadata is known");

        // check hash
        let info_hash: [u8; 20] = Sha1::digest(raw_metadata).into();
        if info_hash != context.torrent_manager_config.info_hash {
            self.corrupted_metadata(Error::msg("hash mismatch"));
            return;
        }

        let info_dict = match Value::new(raw_metadata) {
            Dict(info_dict, _, _) => info_dict,
            _ => {
                self.corrupted_metadata(Error::msg("not a bencoded dict"));
                return;
            }
        };

        match infodict::get_infodict(&info_dict) {
            Ok((piece_length, piece_hashes, m)) => {
                log::warn!(
                    "metadata download completed, we can now start downloading the actual torrent data..."
                );

                // start file manager
                let read_requests_rx = context
                    .file_manager_state
                    .read_requests_rx
                    .take()
                    .expect("no read_requests_rx, has start been called twice?");
                let write_requests_rx = context
                    .file_manager_state
                    .write_requests_rx
                    .take()
                    .expect("no write_requests_rx, has start been called twice?");
                let read_responses_tx = context.file_manager_state.read_responses_tx.clone();
                let write_responses_tx = context.file_manager_state.write_responses_tx.clone();
                *context.torrent_data_status = Some(start_file_manager(
                    context.torrent_manager_config.base_path.as_path(),
                    get_files(&m),
                    piece_length,
                    piece_hashes,
                    read_requests_rx,
                    read_responses_tx,
                    write_requests_rx,
                    write_responses_tx,
                ));

                // update new incoming peers handler with new data info
                context
                    .peers_state
                    .to_new_incoming_peers_handler_tx
                    .send(ToNewIncomingPeersHandlerMsg::TorrentDataInitialized((
                        raw_metadata_size,
                        context
                            .torrent_data_status
                            .as_mut()
                            .expect("initialized few lines above")
                            .current_piece_completion_status(),
                    )))
                    .await
                    .expect("to_new_incoming_peers_handler_tx receiver half closed");

                // we finally have the metadata and can exchange files
                // we discarded have messages (we could not save them because we could not know how many pieces there were in total)
                // and, most importantly, bitfield messages from peers till now, so, let's disconnect from the current peers:
                // the reconnection will trigger the necessary bitfield message that we need to request pieces
                // let's also ignore the last connection attempt for connected peers
                log::warn!(
                    "resetting current connected peers to retrieve which block each peer has..."
                );
                let mut advertised_peers_mg = context
                    .peers_state
                    .advertised_peers
                    .lock()
                    .expect("another user panicked while holding the lock");
                for (peer_addr, _) in context.peers_state.peers.iter() {
                    if let Some((advertised_peer, _)) = advertised_peers_mg.remove(peer_addr) {
                        advertised_peers_mg
                            .insert(peer_addr.clone(), (advertised_peer, SystemTime::UNIX_EPOCH));
                    }
                }
                drop(advertised_peers_mg);
                context.peers_state.peers = HashMap::new();
            }
            Err(e) => {
                self.corrupted_metadata(e);
            }
        }
    }

    fn corrupted_metadata(&mut self, error: Error) {
        log::warn!(
            "downloaded metadata is corrupted ({}), starting over its download...",
            error
        );
        self.metadata_handler = MetadataHandler::new(None, None);
    }
}

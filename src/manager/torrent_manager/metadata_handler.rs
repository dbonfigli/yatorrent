use std::collections::HashMap;
use std::time::Instant;

use anyhow::Error;
use sha1::{Digest, Sha1};

use crate::bencoding::Value::{self, Dict, Int};
use crate::manager::metadata_store::{
    MetadataPieceReqResponse, MetadataPieceRequest, MetadataStore,
};
use crate::manager::peer::{
    METADATA_MESSAGE_DATA, METADATA_MESSAGE_REJECT, METADATA_MESSAGE_REQUEST, MetadataMessage, Peer,
};
use crate::manager::peer_handler::ToNewIncomingPeersHandlerMsg;
use crate::manager::torrent_manager::{ShutdownRequest, TorrentManager, UnrecoverableError};
use crate::metadata::infodict::{self, ParsedInfodict};
use crate::util::{FileEntry, HostAndPort};
use anyhow::Result;

enum MetadataMessageHandlingOutcome {
    MetadataComplete {
        piece_length: u64,
        piece_hashes: Vec<[u8; 20]>,
        file_list: Vec<FileEntry>,
    },
    Other,
}

pub(super) struct MetadataHandler {
    torrent_info_hash: [u8; 20],
    metadata_store: MetadataStore,
}

impl MetadataHandler {
    pub(super) fn new(
        raw_metadata_size: Option<i64>,
        raw_metadata: Option<Vec<u8>>,
        torrent_info_hash: [u8; 20],
    ) -> Result<Self> {
        Ok(MetadataHandler {
            torrent_info_hash,
            metadata_store: MetadataStore::new(raw_metadata_size, raw_metadata)?,
        })
    }

    pub(super) fn update_raw_metadata_size(&mut self, metadata_size: i64) {
        if self.metadata_store.raw_metadata_size().is_none() {
            // we do not know the metadata size yet, take notes
            if let Ok(metadata_store) = MetadataStore::new(Some(metadata_size), None) {
                self.metadata_store = metadata_store;
            }
        }
    }

    pub(super) fn raw_metadata_size(&self) -> Option<i64> {
        self.metadata_store.raw_metadata_size()
    }

    pub(super) fn total_metadata_pieces(&self) -> usize {
        self.metadata_store.total_metadata_pieces()
    }

    pub(super) fn total_metadata_pieces_downloaded(&self) -> usize {
        self.metadata_store.total_metadata_pieces_downloaded()
    }

    pub(super) async fn send_metadata_reqs(&mut self, peers: &mut HashMap<HostAndPort, Peer>) {
        if self.metadata_store.full_metadata_known() {
            return;
        }
        // we still have to download the metadata, ask metadata pieces to peers
        let new_medatada_piece_requests = self.metadata_store.generate_metadata_piece_reqs(peers);
        for MetadataPieceRequest {
            destination_peer,
            piece_index,
        } in new_medatada_piece_requests
        {
            if let Some(peer) = peers.get_mut(&destination_peer) {
                log::debug!("sending metadata piece request to {destination_peer}: {piece_index}");
                peer.send_metadata_extension_message(MetadataMessage::Request(piece_index as u64))
                    .await;
            }
        }
    }

    async fn handle_receive_extended_message_ut_metadata(
        &mut self,
        value: Value,
        peer_addr: HostAndPort,
        additional_data: Vec<u8>,
        peers: &mut HashMap<HostAndPort, Peer>,
    ) -> MetadataMessageHandlingOutcome {
        let d = match value {
            Dict { dict: d, .. } => d,
            _ => {
                log::debug!(
                    "got an ut_metadataextension message from {peer_addr}, it was a bencoded value but not a dict dict, ignoring it"
                );
                return MetadataMessageHandlingOutcome::Other;
            }
        };
        let msg_type = match d.get(b"msg_type".as_slice()) {
            Some(Int(msg_type)) => msg_type,
            _ => {
                log::debug!(
                    "got an ut_metadataextension message from {peer_addr} but no msg_type key was found in the bencoded dict, ignoring it"
                );
                return MetadataMessageHandlingOutcome::Other;
            }
        };
        let piece = match d.get(b"piece".as_slice()) {
            Some(Int(piece)) => piece,
            _ => {
                log::debug!(
                    "got an ut_metadataextension message from {peer_addr} but no piece key was found in the bencoded dict, ignoring it"
                );
                return MetadataMessageHandlingOutcome::Other;
            }
        };
        match *msg_type {
            METADATA_MESSAGE_REQUEST => {
                let peer = match peers.get_mut(&peer_addr) {
                    Some(peer) => peer,
                    None => return MetadataMessageHandlingOutcome::Other,
                };
                self.handle_receive_extended_message_metadata_message_request(peer, *piece)
                    .await;
            }
            METADATA_MESSAGE_DATA => {
                let metadata_size = match d.get(b"total_size".as_slice()) {
                    Some(Int(metadata_size)) => metadata_size,
                    _ => {
                        log::debug!(
                            "got a metadata message data without the required total_size field or total_size is not an integer, ignoring it"
                        );
                        return MetadataMessageHandlingOutcome::Other;
                    }
                };
                return self
                    .handle_receive_extended_message_metadata_message_data(
                        *metadata_size,
                        *piece,
                        additional_data,
                    )
                    .await;
            }
            METADATA_MESSAGE_REJECT => {
                if let Some(peer) = peers.get_mut(&peer_addr) {
                    peer.set_last_metadata_request_rejection(Instant::now());
                }
            }
            _ => {
                log::debug!(
                    "got an ut_metadataextension message from {peer_addr} but msg_type was not recognized as an extension we registered: {msg_type}"
                );
            }
        }
        MetadataMessageHandlingOutcome::Other
    }

    async fn handle_receive_extended_message_metadata_message_request(
        &mut self,
        peer: &mut Peer,
        piece_idx: i64,
    ) {
        match self
            .metadata_store
            .generate_metadata_piece_req_response(piece_idx as usize)
        {
            None => {
                log::debug!(
                    "rejecting metadata message request for {piece_idx} piece from {}: full metadata not yet known or requested pieces is out of range",
                    peer.get_peer_addr()
                );
                peer.send_metadata_extension_message(MetadataMessage::Reject(piece_idx as u64))
                    .await
            }
            Some(MetadataPieceReqResponse {
                piece,
                raw_metadata_size,
            }) => {
                peer.send_metadata_extension_message(MetadataMessage::Data {
                    piece_idx: piece_idx as u64,
                    metadata_total_size: raw_metadata_size as u64,
                    data: piece,
                })
                .await
            }
        }
    }

    async fn handle_receive_extended_message_metadata_message_data(
        &mut self,
        raw_metadata_size: i64,
        piece_idx: i64,
        piece_data: Vec<u8>,
    ) -> MetadataMessageHandlingOutcome {
        if self.metadata_store.full_metadata_known() || piece_idx < 0 {
            // we are not interested in this message
            return MetadataMessageHandlingOutcome::Other;
        }

        if self.metadata_store.raw_metadata_size().is_none() {
            // we do not know the metadata size yet, take notes
            if let Ok(metadata_store) = MetadataStore::new(Some(raw_metadata_size), None) {
                self.metadata_store = metadata_store;
            } else {
                return MetadataMessageHandlingOutcome::Other;
            }
        }

        self.metadata_store
            .insert_piece(piece_idx as usize, piece_data);

        // check if metadata is complete
        if !self.metadata_store.full_metadata_known() {
            return MetadataMessageHandlingOutcome::Other;
        }

        let raw_metadata = self
            .metadata_store
            .get_raw_metadata()
            .as_ref()
            .expect("it must exist, full metadata is known");

        // check hash
        let info_hash: [u8; 20] = Sha1::digest(raw_metadata).into();
        if info_hash != self.torrent_info_hash {
            self.corrupted_metadata(Error::msg("hash mismatch"));
            return MetadataMessageHandlingOutcome::Other;
        }

        let info_dict = match Value::new(raw_metadata) {
            Dict {
                dict: info_dict, ..
            } => info_dict,
            _ => {
                self.corrupted_metadata(Error::msg("not a bencoded dict"));
                return MetadataMessageHandlingOutcome::Other;
            }
        };

        match infodict::parse_infodict(&info_dict) {
            Ok(ParsedInfodict {
                piece_length,
                piece_hashes,
                metainfo_file,
            }) => MetadataMessageHandlingOutcome::MetadataComplete {
                piece_length,
                piece_hashes,
                file_list: infodict::get_files(&metainfo_file),
            },
            Err(e) => {
                self.corrupted_metadata(e);
                MetadataMessageHandlingOutcome::Other
            }
        }
    }

    fn corrupted_metadata(&mut self, error: Error) {
        log::warn!(
            "downloaded metadata is corrupted ({}), starting over its download...",
            error
        );
        if let Ok(metadata_store) = MetadataStore::new(None, None) {
            self.metadata_store = metadata_store;
        };
    }
}

impl TorrentManager {
    pub(super) async fn handle_receive_extended_message_ut_metadata(
        &mut self,
        value: Value,
        peer_addr: HostAndPort,
        additional_data: Vec<u8>,
    ) {
        let outcome = self
            .metadata_handler
            .handle_receive_extended_message_ut_metadata(
                value,
                peer_addr,
                additional_data,
                &mut self.peers_ctx.peers,
            )
            .await;

        if let MetadataMessageHandlingOutcome::MetadataComplete {
            piece_length,
            piece_hashes,
            file_list,
        } = outcome
        {
            log::warn!(
                "metadata download completed, we can now start downloading the actual torrent data..."
            );

            // start file manager
            self.torrent_data_status = match self.file_manager_handler.start(
                self.torrent_manager_config.base_path.as_path(),
                file_list,
                piece_length,
                piece_hashes,
            ) {
                Ok(t) => {
                    if t.completed() && self.torrent_manager_config.exit_when_complete {
                        log::info!("torrent fully downloaded; exiting");
                        self.send_shutdown_request(ShutdownRequest::Success);
                    }
                    Some(t)
                }
                Err(e) => {
                    log::error!("initialization of torrent data failed: {e}");
                    self.send_shutdown_request(ShutdownRequest::Error(UnrecoverableError::new(
                        e.to_string(),
                    )));
                    return;
                }
            };

            // update new incoming peers handler with new data info
            _ = self.peers_channels.to_new_incoming_peers_handler_tx.send(
                ToNewIncomingPeersHandlerMsg::TorrentDataInitialized {
                    metadata_size: self
                        .metadata_handler
                        .raw_metadata_size()
                        .expect("it must exist, full metadata is known"),
                    piece_completion_status: self
                        .torrent_data_status
                        .as_ref()
                        .expect("initialized few lines above")
                        .current_piece_completion_status(),
                },
            );

            // we finally have the metadata and can exchange files
            // we discarded have messages (we could not save them because we could not know how many pieces there were in total)
            // and, most importantly, bitfield messages from peers till now, so, let's disconnect from the current peers:
            // the reconnection will trigger the necessary bitfield message that we need to request pieces
            // let's also ignore the last connection attempt for connected peers
            log::warn!(
                "resetting current connected peers to retrieve which block each peer has..."
            );

            let connected_peers = self
                .peers_ctx
                .peers
                .values()
                .filter_map(|p| p.get_peer_addr_and_listening_torrent_protocol_port())
                .collect();
            self.peers_ctx
                .advertised_peers
                .wipe_last_connection_attempt(connected_peers);

            self.peers_ctx.peers = HashMap::new();
        }
    }
}

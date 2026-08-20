use std::net::SocketAddr;

use crate::{
    bencoding::Value::{self, Dict, Int},
    manager::{
        BLOCK_SIZE_B,
        peer_handler::ToPeerMsg,
        pex_handler::{self, PexEvent},
        torrent_manager::{TorrentManager, util},
    },
    persistence::file_manager::{ReadPieceBlockRequest, WritePieceBlockRequest},
    torrent_protocol::wire_protocol::{BlockRequest, Message},
    util::{HostAndPort, force_string},
};

// the max request size we allow from peers, in bytes. In theory none should ask for more than 16KB, here we are a bit lenient.
// Se also MAX_MESSAGE_SIZE_B
const MAX_ALLOWED_BLOCK_REQUEST_SIZE_B: u32 = BLOCK_SIZE_B as u32 * 4;

impl TorrentManager {
    pub(super) async fn handle_receive_message(&mut self, peer_addr: HostAndPort, msg: Message) {
        log::trace!("received message from peer {peer_addr}: {msg}");
        match msg {
            Message::KeepAlive => {}
            Message::Choke => self.handle_receive_choke_message(peer_addr).await,
            Message::Unchoke => {
                if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr) {
                    peer.set_peer_choking(false);
                    log::trace!("received unchoke from {peer_addr}");
                    // since we received an unchoke, we can try to send more requests immediately to this peer, without waiting for a tick
                    self.send_pieces_reqs_to_peer(peer_addr).await;
                }
            }
            Message::Interested => {
                if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr) {
                    peer.set_peer_interested(true);
                }
            }
            Message::NotInterested => {
                if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr) {
                    peer.set_peer_interested(false);
                }
            }
            Message::Have(piece_idx) => {
                self.handle_receive_have_message(peer_addr, piece_idx).await
            }
            Message::Bitfield(bitfield) => {
                self.handle_receive_bitfield_message(peer_addr, bitfield)
                    .await
            }
            Message::Request(block_request) => {
                self.handle_receive_request_message(peer_addr, block_request)
                    .await
            }
            Message::Piece {
                piece_idx,
                begin,
                data,
            } => {
                self.handle_receive_piece_message(peer_addr, piece_idx, begin, data)
                    .await
            }
            Message::Cancel(block_request) => {
                if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr) {
                    peer.send_cancel(block_request);
                }
            }
            Message::Port(port) => {
                match peer_addr.parse::<SocketAddr>() {
                    Ok(SocketAddr::V4(socket_addr)) => {
                        if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr) {
                            peer.set_listening_torrent_protocol_port(port);
                        }
                        let peer_ip_addr = *socket_addr.ip();
                        // we know the peer supports DHT, send this to dht as new node
                        self.dht_handler.new_node_discovered(peer_ip_addr, port);
                        // also update pex events
                        self.pex_handler
                            .new_pex_event(format!("{peer_ip_addr}:{port}"), PexEvent::Added);
                    }
                    _ => log::trace!(
                        "ignoring \"port\" message from {peer_addr}: it is not an ipv4 peer and our pex / dht implementation is ipv4 only"
                    ),
                }
            }
            Message::Suggest(piece_idx) => {
                self.handle_suggest_message(peer_addr, piece_idx).await;
            }
            Message::HaveAll => {
                self.handle_have_all_message(peer_addr).await;
            }
            Message::HaveNone => {
                self.handle_have_none_message(peer_addr).await;
            }
            Message::Reject(block_request) => {
                self.handle_reject_message(peer_addr, block_request).await;
            }
            Message::AllowedFast(piece_idx) => {
                self.handle_allow_fast_message(peer_addr, piece_idx as usize)
                    .await;
            }
            Message::Extended {
                extension_protocol_id: extension_id,
                bencoded_message: extended_message,
                additional_raw_data: additional_data,
            } => {
                self.handle_receive_extended_message(
                    peer_addr,
                    extension_id,
                    extended_message,
                    additional_data,
                )
                .await;
            }
        }
    }

    async fn handle_receive_have_message(&mut self, peer_addr: HostAndPort, piece_idx: u32) {
        let peer = match self.peers_ctx.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };

        let torrent_data_status = match &mut self.torrent_data_status {
            Some(torrent_data_status) => torrent_data_status,
            None => return,
        };

        let piece_idx = piece_idx as usize;
        if piece_idx < torrent_data_status.num_pieces() {
            peer.set_have(piece_idx);
            // send interest if needed
            if !peer.get_am_interested() && !torrent_data_status.piece_is_completed(piece_idx) {
                peer.set_am_interested(true);
                peer.send(ToPeerMsg::Send(Message::Interested)).await;
            }
        } else {
            log::warn!(
                "got message \"have\" {piece_idx} from peer {peer_addr} but the torrent have only {} pieces",
                torrent_data_status.num_pieces()
            );
            peer.send(ToPeerMsg::Disconnect()).await;
            self.peers_ctx.insert_bad_peer(peer_addr.clone());
            self.remove_peer(peer_addr);
        }
    }

    async fn handle_receive_bitfield_message(
        &mut self,
        peer_addr: HostAndPort,
        bitfield: Vec<bool>,
    ) {
        let torrent_data_status = match &mut self.torrent_data_status {
            Some(torrent_data_status) => torrent_data_status,
            None => return,
        };

        if bitfield.len() < torrent_data_status.num_pieces() {
            log::warn!(
                "received wrongly sized bitfield from peer {peer_addr}: received {} bits but expected {}",
                bitfield.len(),
                torrent_data_status.num_pieces()
            );
            self.peers_ctx.insert_bad_peer(peer_addr.clone());
            if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr) {
                peer.send(ToPeerMsg::Disconnect()).await;
            }
            self.remove_peer(peer_addr);
        } else if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr) {
            // ignore bitfield if we don't have the torrent file yet, we cannot trust the bitfield from the peer
            if peer.get_haves().is_none() {
                return;
            }

            // bitfield is byte aligned, it could contain more bits than pieces in the torrent
            peer.set_haves(Some(bitfield[0..torrent_data_status.num_pieces()].to_vec()));
            let haves = peer.get_haves().expect("just added above");
            log::trace!(
                "received bitfield from peer {peer_addr}: it has {}/{} pieces",
                haves
                    .iter()
                    .fold(0, |acc, v| if *v { acc + 1 } else { acc }),
                haves.len()
            );

            // check if we need to send interest
            if !peer.get_am_interested() {
                for piece_idx in 0..haves.len() {
                    if !torrent_data_status.piece_is_completed(piece_idx) && haves[piece_idx] {
                        peer.set_am_interested(true);
                        peer.send(ToPeerMsg::Send(Message::Interested)).await;
                        break;
                    }
                }
            }
        }
        // todo: maybe re-compute assignations immediately here instead of waiting tick
    }

    async fn handle_receive_choke_message(&mut self, peer_addr: HostAndPort) {
        let peer = match self.peers_ctx.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };

        log::trace!(
            "received choke from peer {peer_addr} with {} outstanding piece block requests",
            self.piece_requestor
                .outstanding_piece_block_request_count_for_peer(&peer_addr)
        );
        peer.set_peer_choking(true);
    }

    async fn handle_receive_request_message(
        &mut self,
        peer_addr: HostAndPort,
        block_request: BlockRequest,
    ) {
        if self.torrent_data_status.is_none() {
            return;
        }

        let peer = match self.peers_ctx.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };

        if block_request.data_len > MAX_ALLOWED_BLOCK_REQUEST_SIZE_B {
            log::warn!(
                "removing peer {peer_addr}: it requested a block of {} bytes, bigger than the allowed ({MAX_ALLOWED_BLOCK_REQUEST_SIZE_B})",
                block_request.data_len
            );
            peer.send(ToPeerMsg::Disconnect()).await;
            self.remove_peer(peer_addr);
            return;
        }

        if !peer.get_am_choking()
            && util::should_choke(
                // todo: choking algorithm is really naive, must improve it to avoid saturating upload
                self.peers_channels.incoming_peer_messages_tx.capacity(),
                peer.get_outstanding_incoming_piece_block_requests(),
                true,
                &self.file_manager_handler,
            )
        {
            peer.send(ToPeerMsg::Send(Message::Choke)).await;
            peer.set_am_choking(true);
        }

        if peer.get_am_choking() {
            if peer.supports_fast_extension() {
                peer.send(ToPeerMsg::Send(Message::Reject(block_request)))
                    .await;
            }
            return;
        }

        // else, we are not choking, we can send the block, read the piece, once read, we will send it
        peer.increase_outstanding_incoming_piece_block_requests();
        let _ = self
            .file_manager_handler
            .send_read_req(ReadPieceBlockRequest {
                requestor_peer_addr: peer_addr,
                piece_idx: block_request.piece_idx as usize,
                block_begin: block_request.block_begin as u64,
                block_length: block_request.data_len as u64,
            })
            .await;
        // once the block is read, we will send it on handle_read_piece_block_response
    }

    async fn handle_suggest_message(&mut self, peer_addr: HostAndPort, _piece_idx: u32) {
        if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr)
            && !peer.supports_fast_extension()
        {
            log::debug!(
                "removing peer {peer_addr}: we received a \"suggest\" fast track message but the peer did not advertise its support"
            );
            peer.send(ToPeerMsg::Disconnect()).await;
            self.remove_peer(peer_addr);
            return;
        }
        // todo: at the moment we ignore these suggestions
        log::trace!("received a suggest from {peer_addr}");
    }

    async fn handle_have_all_message(&mut self, peer_addr: HostAndPort) {
        let torrent_data_status = match &mut self.torrent_data_status {
            Some(torrent_data_status) => torrent_data_status,
            None => return,
        };

        if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr) {
            if !peer.supports_fast_extension() {
                log::debug!(
                    "removing peer {peer_addr}: we received a \"have all\" fast track message but the peer did not advertise its support"
                );
                peer.send(ToPeerMsg::Disconnect()).await;
                self.remove_peer(peer_addr);
                return;
            }

            peer.set_haves(Some(vec![true; torrent_data_status.num_pieces()]));

            // check if we need to send interest
            if !peer.get_am_interested() {
                for piece_idx in 0..torrent_data_status.num_pieces() {
                    if !torrent_data_status.piece_is_completed(piece_idx) {
                        peer.set_am_interested(true);
                        peer.send(ToPeerMsg::Send(Message::Interested)).await;
                        break;
                    }
                }
            }
        }
        // todo: maybe re-compute assignations immediately here instead of waiting tick
    }

    async fn handle_have_none_message(&mut self, peer_addr: HostAndPort) {
        let torrent_data_status = match &mut self.torrent_data_status {
            Some(torrent_data_status) => torrent_data_status,
            None => return,
        };

        if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr) {
            if !peer.supports_fast_extension() {
                log::debug!(
                    "removing peer {peer_addr}: we received a \"have none\" fast track message but the peer did not advertise its support"
                );
                peer.send(ToPeerMsg::Disconnect()).await;
                self.remove_peer(peer_addr);
                return;
            }

            peer.set_haves(Some(vec![false; torrent_data_status.num_pieces()]));
        }
    }

    async fn handle_reject_message(
        &mut self,
        peer_addr: HostAndPort,
        _block_request: BlockRequest,
    ) {
        if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr)
            && !peer.supports_fast_extension()
        {
            log::debug!(
                "removing peer {peer_addr}: we received a \"reject\" fast track message but the peer did not advertise its support"
            );
            peer.send(ToPeerMsg::Disconnect()).await;
            self.remove_peer(peer_addr);
        }
        // for the moment we will ignore this and let the normal fast expiration work after choke
    }

    async fn handle_allow_fast_message(&mut self, peer_addr: HostAndPort, _piece_idx: usize) {
        if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr)
            && !peer.supports_fast_extension()
        {
            log::debug!(
                "removing peer {peer_addr}: we received an \"allow fast\" fast track message but the peer did not advertise its support"
            );
            peer.send(ToPeerMsg::Disconnect()).await;
            self.remove_peer(peer_addr);
        }
        // for the moment we ignore allow fast messages
    }

    async fn handle_receive_extended_message(
        &mut self,
        peer_addr: HostAndPort,
        extension_id: u8,
        extended_message: Value,
        additional_data: Vec<u8>,
    ) {
        let peer = match self.peers_ctx.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };

        // retrieve reqq, if provided
        if let Dict { dict: d, .. } = extended_message.clone() {
            let client_version = if let Some(Value::Str(v)) = d.get(b"v".as_slice()) {
                force_string(v)
            } else {
                "unknown".to_string()
            };
            if let Some(Value::Int(reqq)) = d.get(b"reqq".as_slice()) {
                peer.set_reqq(*reqq as usize);
                log::debug!("{peer_addr} (version: {client_version}) has reqq: {reqq}");
            }
            peer.set_client_version(client_version);
        }

        match extension_id {
            0 => {
                // this is an extension handshake
                self.handle_receive_extended_message_handshake(extended_message, peer_addr)
                    .await;
            }
            _ if extension_id == peer.get_ut_pex_id() => {
                // this is an ut_pex extended message
                let new_peers =
                    pex_handler::parse_extended_message_ut_pex(extended_message, peer_addr);
                self.peers_ctx.advertised_peers.insert(new_peers);
            }
            _ if extension_id == peer.get_ut_metadata_id() => {
                // this is an ut_metadata extended message
                self.handle_receive_extended_message_ut_metadata(
                    extended_message,
                    peer_addr,
                    additional_data,
                )
                .await;
            }
            _ => {
                log::debug!(
                    "got an extension message from {peer_addr} but id was not recognized as an extension we registered: {extension_id}. Message was: {extended_message}"
                );
            }
        }
    }

    async fn handle_receive_extended_message_handshake(
        &mut self,
        extended_message: Value,
        peer_addr: HostAndPort,
    ) {
        let extended_message_dict = match extended_message {
            Dict {
                dict: extended_message_dict,
                ..
            } => extended_message_dict,
            _ => {
                log::debug!(
                    "got an ut_metadata extension handshake but data was not a dict, ignoring this message"
                );
                return;
            }
        };
        let m = match extended_message_dict.get(b"m".as_slice()) {
            Some(Dict { dict: m, .. }) => m,
            _ => {
                log::debug!(
                    "got an ut_metadata extension handshake but \"m\" entry was not found in dict or was not a dict itself, ignoring this message"
                );
                return;
            }
        };

        let other_active_peers = self
            .peers_ctx
            .peers
            .keys()
            .filter(|k| peer_addr != **k)
            .cloned()
            .collect::<Vec<_>>();
        let peer = match self.peers_ctx.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };

        if let Some(Int(ut_pex_id)) = m.get(&b"ut_pex".to_vec()) {
            // this peer supports the PEX extension, registered at number ut_pex_id
            peer.set_ut_pex_id(*ut_pex_id as u8);
            // send first peer list
            peer.send_pex_extension_message(other_active_peers, Vec::new())
                .await;
        }
        if let Some(Int(ut_metadata_id)) = m.get(&b"ut_metadata".to_vec()) {
            // this peer supports the ut_metadata extension, registered at number ut_metadata_id
            peer.set_ut_metadata_id(*ut_metadata_id as u8);
            if let Some(Int(metadata_size)) = extended_message_dict.get(b"metadata_size".as_slice())
            {
                if *metadata_size <= 0 {
                    log::debug!(
                        "got an ut_metadata extension handshake where \"metadata_size\" was <= 0, ignoring this message"
                    );
                } else {
                    self.metadata_handler
                        .update_raw_metadata_size(*metadata_size);
                }
            }
        }
    }

    async fn handle_receive_piece_message(
        &mut self,
        peer_addr: HostAndPort,
        piece_idx: u32,
        begin: u32,
        data: Vec<u8>,
    ) {
        if self.torrent_data_status.is_none() {
            return;
        }

        let data_len = data.len() as u64;
        self.bandwidth_tracker.add_downloaded_bytes(data_len);
        if let Some(peer) = self.peers_ctx.peers.get_mut(&peer_addr) {
            peer.get_bandwidth_tracker_mut()
                .add_downloaded_bytes(data_len);
            let rtt = self.piece_requestor.block_request_completed(
                &peer_addr,
                &BlockRequest {
                    piece_idx,
                    block_begin: begin,
                    data_len: data.len() as u32,
                },
            );
            if let Some(rtt) = rtt {
                peer.update_rtt(rtt);
            }
        }

        // since we completed receiving a piece block, we can try to send more requests immediately to this peer, without waiting for a tick
        self.send_pieces_reqs_to_peer(peer_addr.clone()).await;

        // send data to file manager to persist it
        let _ = self
            .file_manager_handler
            .send_write_req(WritePieceBlockRequest {
                requestor_peer_addr: peer_addr,
                piece_idx: piece_idx as usize,
                data,
                block_begin: begin as u64,
            })
            .await;
    }

    async fn send_pieces_reqs_to_peer(&mut self, peer_addr: HostAndPort) {
        let torrent_data_status = match &self.torrent_data_status {
            Some(torrent_data_status) => torrent_data_status,
            None => return,
        };

        let peer = match self.peers_ctx.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };

        // compute requests from piece requestor
        let reqs_to_send = self.piece_requestor.generate_requests_to_send_for_peer(
            &peer_addr,
            peer,
            torrent_data_status,
        );

        // finally send requests
        for block_request in reqs_to_send {
            peer.send(ToPeerMsg::Send(Message::Request(block_request)))
                .await;
        }
    }
}

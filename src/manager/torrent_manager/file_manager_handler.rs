use std::process;

use tokio::sync::mpsc::{
    self, Receiver, Sender, UnboundedReceiver, UnboundedSender, error::SendError,
};

use crate::{
    manager::{
        peer::Peer,
        peer_handler::{ToNewIncomingPeersHandlerMsg, ToPeerMsg},
        torrent_manager::TorrentManager,
    },
    persistence::{
        file_manager::{
            self, ReadPieceBlockRequest, ReadPieceBlockResponse, ShaCheckReadError,
            ShaCorruptedError, WritePieceBlockRequest, WritePieceBlockResponse,
        },
        torrent_data_status::TorrentDataStatus,
    },
    torrent_protocol::wire_protocol::Message,
    tracker::Event,
    util::FileEntry,
};

const MAX_CORRUPTION_ERRORS: u32 = 20; // max sha1 corruption errors on blocks a peer can have before marking it as bad

// the number of enqueued disk read operations requests.
// This should be > than MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER, otherwise a single peer that is righfully sending the max number of block requets we advertise will lead to it being chocked (when read reqs channel cap is 0)
const READ_REQUESTS_CHANNEL_CAPACITY: usize = 2500;
// the number of enqueued disk writes operations requests
const WRITE_REQUESTS_CHANNEL_CAPACITY: usize = 200;

// maximum allowed number of read operations in flight across all peers; i.e., read requests that have been sent to the file manager and whose responses have not yet been handled by the torrent manager.
// We use this to limit memory usage since read_responses channel is unbounded to avoid deadlock.
// See also MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER that is a similar limit (but different: it includes the time to also send the data), per peer.
const MAX_OUTSTANDING_READ_OPS: usize = 3000;

pub(super) enum FileManagerResponse {
    Read(ReadPieceBlockResponse),
    Write(WritePieceBlockResponse),
}

pub(super) struct FileManagerHandler {
    outstanding_read_ops: usize, // these are the read requests enqueued in read_requests channel + the ones being concurrently handled (up to MAX_CONCURRENT_READ_OPS) + the ones enqueued on read_responses still to be read by the torrent manager

    read_requests_tx: Sender<ReadPieceBlockRequest>,
    read_requests_rx: Option<Receiver<ReadPieceBlockRequest>>, // optional bc we will move it to the file manager handler at start
    // for read disk operations, the flow is like this:
    // torrent manager (read_requests channel) ->
    //   file manager (read_responses channel) ->
    //     torrent manager
    //
    // if read_responses was bounded and is full, and therefore file manager is blocked,
    // also read_requests can become soon after full (because file manager is not dequeuing)
    // causing a deadlock on torrent manager in case it wants to send a new message to read_responses.
    //
    // To avoid this, we use unbounded channels.
    // Outstanding read requests are globally bounded anyway by MAX_OUTSTANDING_READ_OPS
    // so the memory is bound to 16KB x 3000 = ~46MB.
    read_responses_tx: UnboundedSender<ReadPieceBlockResponse>,
    read_responses_rx: UnboundedReceiver<ReadPieceBlockResponse>,
    write_requests_tx: Sender<WritePieceBlockRequest>,
    write_requests_rx: Option<Receiver<WritePieceBlockRequest>>, // optional bc we will move it to the file manager handler at start
    // unbounded for the same reason as read_responses, without global caps since the messages are really small for this channel
    write_responses_tx: UnboundedSender<WritePieceBlockResponse>,
    write_responses_rx: UnboundedReceiver<WritePieceBlockResponse>,
}

impl FileManagerHandler {
    pub(super) fn new() -> Self {
        let (read_requests_tx, read_requests_rx) = mpsc::channel(READ_REQUESTS_CHANNEL_CAPACITY);
        let (read_responses_tx, read_responses_rx) = mpsc::unbounded_channel();
        let (write_requests_tx, write_requests_rx) = mpsc::channel(WRITE_REQUESTS_CHANNEL_CAPACITY);
        let (write_responses_tx, write_responses_rx) = mpsc::unbounded_channel();

        FileManagerHandler {
            outstanding_read_ops: 0,
            read_requests_tx,
            read_requests_rx: Some(read_requests_rx),
            read_responses_tx,
            read_responses_rx,
            write_requests_tx,
            write_requests_rx: Some(write_requests_rx),
            write_responses_tx,
            write_responses_rx,
        }
    }

    pub(super) fn start(
        &mut self,
        base_path: &std::path::Path,
        file_list: Vec<FileEntry>,
        piece_length: u64,
        piece_hashes: Vec<[u8; 20]>,
    ) -> TorrentDataStatus {
        let read_requests_rx = self
            .read_requests_rx
            .take()
            .expect("no read_requests_rx, has start been called twice?");
        let write_requests_rx = self
            .write_requests_rx
            .take()
            .expect("no write_requests_rx, has start been called twice?");
        let read_responses_tx = self.read_responses_tx.clone();
        let write_responses_tx = self.write_responses_tx.clone();
        file_manager::start_file_manager(
            base_path,
            file_list,
            piece_length,
            piece_hashes,
            read_requests_rx,
            read_responses_tx,
            write_requests_rx,
            write_responses_tx,
        )
    }

    pub(super) fn inflight_write_reqs(&self) -> usize {
        WRITE_REQUESTS_CHANNEL_CAPACITY - self.write_requests_tx.capacity()
    }

    pub(super) fn inflight_read_reqs(&self) -> usize {
        READ_REQUESTS_CHANNEL_CAPACITY - self.read_requests_tx.capacity()
    }

    pub(super) async fn send_read_req(
        &mut self,
        read_req: ReadPieceBlockRequest,
    ) -> Result<(), SendError<ReadPieceBlockRequest>> {
        self.outstanding_read_ops += 1;
        self.read_requests_tx.send(read_req).await
    }

    pub(super) async fn send_write_req(
        &mut self,
        write_req: WritePieceBlockRequest,
    ) -> Result<(), SendError<WritePieceBlockRequest>> {
        self.write_requests_tx.send(write_req).await
    }

    pub(super) async fn recv_response(&mut self) -> Option<FileManagerResponse> {
        tokio::select! {
            Some(msg) = self.read_responses_rx.recv() => {
                self.outstanding_read_ops = self.outstanding_read_ops.saturating_sub(1);
                Some(FileManagerResponse::Read(msg))
            }
            Some(msg) = self.write_responses_rx.recv() => {
                Some(FileManagerResponse::Write(msg))
            }
            else => None
        }
    }

    pub(super) fn outstanding_read_ops(&self) -> usize {
        self.outstanding_read_ops
    }

    pub(super) fn read_saturated(&self) -> bool {
        self.read_requests_tx.capacity() == 0
            || self.outstanding_read_ops > MAX_OUTSTANDING_READ_OPS
    }
}

impl TorrentManager {
    pub(super) async fn handle_write_piece_block_response(
        &mut self,
        write_piece_block_response: WritePieceBlockResponse,
    ) {
        let already_completed;
        match self.torrent_data_status.as_mut() {
            Some(f) => {
                already_completed = f.completed();
                f.update(&write_piece_block_response);
            }
            None => return,
        }

        let piece_idx = write_piece_block_response.request.piece_idx;
        let peer_addr = write_piece_block_response.request.requestor_peer_addr;
        match write_piece_block_response.response {
            Ok(torrent_data_status_updates) => {
                if torrent_data_status_updates.piece_is_completed {
                    self.piece_requestor
                        .piece_request_completed(&peer_addr, piece_idx);

                    if !already_completed
                        && self
                            .torrent_data_status
                            .as_ref()
                            .expect("invariant checked above")
                            .completed()
                    {
                        log::warn!("torrent download completed");
                        self.tracker_requestor
                            .async_request_to_tracker(
                                Event::Completed,
                                &self.peers_ctx.advertised_peers,
                                self.torrent_data_status.as_ref().map(|f| f.bytes_left()),
                                (
                                    self.bandwidth_tracker.uploaded_bytes(),
                                    self.bandwidth_tracker.downloaded_bytes(),
                                ),
                            )
                            .await;
                        if self.torrent_manager_config.exit_when_complete {
                            log::warn!("Exiting...");
                            process::exit(0);
                        }
                    }

                    let _ = self
                        .peers_channels
                        .to_new_incoming_peers_handler_tx
                        .send(ToNewIncomingPeersHandlerMsg::PieceCompleted(piece_idx));

                    for peer in self.peers_ctx.peers.values_mut() {
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

                let peer = match self.peers_ctx.peers.get_mut(&peer_addr) {
                    Some(peer) => peer,
                    None => return,
                };

                let sha_corrupted_error = e.downcast_ref::<ShaCorruptedError>().is_some();
                let sha_check_read_error = e.downcast_ref::<ShaCheckReadError>().is_some();

                if sha_corrupted_error || sha_check_read_error {
                    // we could not verify the whole piece, wipe current download status
                    // also from the piece requestor so to start over
                    self.piece_requestor
                        .piece_request_completed(&peer_addr, piece_idx);
                } else {
                    // only a block write failed, tell piece requestor this so to remove
                    // it from downloading piece tracking
                    self.piece_requestor
                        .block_write_failed(&peer_addr, piece_idx);
                }

                // keep track of corruptions, remove if too many
                if sha_corrupted_error {
                    peer.increase_corruption_errors();
                    if peer.get_corruption_errors() > MAX_CORRUPTION_ERRORS {
                        log::warn!(
                            "removing peer {peer_addr} due to too many corrupted pieces received",
                        );
                        peer.send(ToPeerMsg::Disconnect()).await;
                        self.remove_peer(peer_addr);
                    }
                }
            }
        }
    }

    pub(super) async fn handle_read_piece_block_response(
        &mut self,
        read_piece_block_response: ReadPieceBlockResponse,
    ) {
        let peer = match self
            .peers_ctx
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
                peer.send(ToPeerMsg::Send(Message::Piece {
                    piece_idx: read_piece_block_response.request.piece_idx as u32,
                    begin: read_piece_block_response.request.block_begin as u32,
                    data,
                }))
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

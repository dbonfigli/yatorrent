use crate::{
    manager::peer::Peer,
    persistence::{
        piece::{Fragment, Piece},
        torrent_data_status::TorrentDataStatus,
    },
    torrent_protocol::wire_protocol::BlockRequest,
    util::HostAndPort,
};
use rand::seq::SliceRandom;
use std::{
    cmp::{Ordering, max, min},
    collections::HashMap,
    time::{Duration, Instant},
};

pub const MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER_HARD_LIMIT: usize = 500; // same as libtorrent, but we can go up to 2000 if needed

const MAX_OUTSTANDING_PIECES: usize = 2000;
const MIN_OUTSTANDING_BLOCK_REQUESTS: usize = 5;
pub const BLOCK_SIZE_B: u64 = 16384;

// requests are calculated based on bandwidth so that they fill up the pipe up to some seconds capped by the below consts
const RTT_MULTIPLIER: f64 = 1.2;
const MIN_TARGET_BUFFER_TIME_SECONDS: f64 = 0.5;
const MAX_TARGET_BUFFER_TIME_SECONDS: f64 = 2.;

const BLOCK_DELAYED_ARRIVAL_LOG_THRESHOLD: Duration = Duration::from_secs(60);

// some peers choke and few moments after unchoke (a thing specs call "fibrillation").
// even if specs says:
// "The client should not attempt to send requests for blocks, and it should consider all pending (unanswered) requests to be discarded by the remote peer."
// here we wait a bit before considering the request lost and reassign pieces assigned to a choked peer to another peer
const CHOKED_PEER_ASSIGMENTS_GRACE_PERIOD: Duration = Duration::from_secs(15);

pub struct PieceRequestor {
    outstanding_piece_assignments: HashMap<usize, HostAndPort>, // piece idx -> peer_addr
    outstanding_piece_block_requests: HashMap<HostAndPort, HashMap<BlockRequest, Instant>>, // peer_addr -> BlockRequest -> request time
    requested_pieces: HashMap<HostAndPort, HashMap<usize, (Piece, bool)>>, // peer_addr -> piece idx -> (piece status with all the requested fragments, all possible block requests already perfomed)
}

impl PieceRequestor {
    pub fn new() -> Self {
        PieceRequestor {
            outstanding_piece_assignments: HashMap::new(),
            outstanding_piece_block_requests: HashMap::new(),
            requested_pieces: HashMap::new(),
        }
    }

    pub fn outstanding_piece_block_request_count_for_peer(&self, peer_addr: &HostAndPort) -> usize {
        self.outstanding_piece_block_requests
            .get(peer_addr)
            .map_or(0, |reqs| reqs.len())
    }

    pub fn remove_assigments_to_peer(&mut self, peer_addr: &HostAndPort) {
        self.outstanding_piece_block_requests.remove(peer_addr);
        if let Some(requests) = self.requested_pieces.remove(peer_addr) {
            for (piece_idx, _) in requests {
                self.outstanding_piece_assignments.remove(&piece_idx);
            }
        }
    }

    pub fn get_pending_block_requests_for_peer(&self, peer_addr: &HostAndPort) -> usize {
        self.outstanding_piece_block_requests
            .get(peer_addr)
            .map_or(0, |o| o.len())
    }

    pub fn get_assigned_pieces_for_peer(&self, peer_addr: &HostAndPort) -> usize {
        self.requested_pieces.get(peer_addr).map_or(0, |r| r.len())
    }

    pub fn block_request_completed(
        &mut self,
        peer_addr: &HostAndPort,
        block_request: &BlockRequest,
    ) -> Option<Duration> // the rtt of the request
    {
        if let Some(reqs) = self.outstanding_piece_block_requests.get_mut(peer_addr) {
            match reqs.remove(block_request) {
                None => {
                    log::debug!(
                        "we received block {:?} from {peer_addr} but request was expired",
                        block_request
                    );
                    None
                }
                Some(t) => {
                    let latency = Instant::now().duration_since(t);
                    if latency > BLOCK_DELAYED_ARRIVAL_LOG_THRESHOLD {
                        log::debug!("requested block from {peer_addr} arrived after {latency:#?}");
                    }
                    Some(latency)
                }
            }
        } else {
            log::debug!(
                "we received block {:?} from {peer_addr} but request was expired (no outstanding piece requests from this peer at all)",
                block_request
            );
            None
        }
    }

    pub fn piece_request_completed(&mut self, peer_addr: &HostAndPort, piece_idx: usize) {
        self.outstanding_piece_assignments.remove(&piece_idx);
        if let Some(reqs) = self.requested_pieces.get_mut(peer_addr) {
            reqs.remove(&piece_idx);
        }
    }

    pub fn block_write_failed(&mut self, peer_addr: &HostAndPort, piece_idx: usize) {
        // ideally we should just change the piece in self.requested_pieces so that that block is removed from Piece
        // but at the moment there is no way to remove a block from a Piece,
        // so we discard the whole piece assigment, with the drawback of potentially requesting blocks
        // already requested that are inflight and not yet written to disk
        // todo: optimize this, add remove_fragment to Piece
        self.piece_request_completed(peer_addr, piece_idx);
    }

    fn remove_assigments_to_choked(&mut self, peers: &HashMap<HostAndPort, Peer>) {
        let mut peers_to_remove = Vec::new();
        for peer_addr in self.requested_pieces.keys() {
            if let Some(peer) = peers.get(peer_addr)
                && peer.peer_choking_since().is_some_and(|choking_since| {
                    Instant::now().duration_since(choking_since)
                        > CHOKED_PEER_ASSIGMENTS_GRACE_PERIOD
                })
            {
                peers_to_remove.push(peer_addr.clone());
            }
        }
        for peer_addr in peers_to_remove {
            self.remove_assigments_to_peer(&peer_addr);
        }
    }

    pub fn remove_stale_requests(
        &mut self,
        request_timeout: Duration,
        peers: &HashMap<HostAndPort, Peer>,
    ) -> Vec<(HostAndPort, BlockRequest)> // expired block requests that should be canceled
    {
        self.remove_assigments_to_choked(peers);

        let mut requests_to_cancel = Vec::<(HostAndPort, BlockRequest)>::new();
        let now = Instant::now();
        self.outstanding_piece_block_requests.iter_mut().for_each(
            |(peer_addr, outstanding_block_requests_for_peer)| {
                outstanding_block_requests_for_peer.retain(
                    |block_request, req_time| {
                        if now.duration_since(*req_time) < request_timeout {
                            true
                        } else {
                            log::debug!("removed stale request to peer: {}: (piece idx: {}, block begin: {}, length: {})",
                                *peer_addr, block_request.piece_idx, block_request.block_begin, block_request.data_len);
                            requests_to_cancel.push((peer_addr.clone(), block_request.clone()));
                            // if a block stalled, we remove the assigment of the piece to this peer, with all associated block requests
                            if let Some(requested_pieces_for_peer) = self.requested_pieces.get_mut(peer_addr) {
                                requested_pieces_for_peer.remove(&(block_request.piece_idx as usize));
                            }
                            self.outstanding_piece_assignments.remove(&(block_request.piece_idx as usize));
                            false
                        }
                    },
                );
            },
        );

        requests_to_cancel
    }

    pub fn generate_requests_to_send(
        &mut self,
        peers: &HashMap<HostAndPort, Peer>,
        torrent_data_status: &TorrentDataStatus,
    ) -> Vec<(HostAndPort, Vec<BlockRequest>)> {
        let mut requests_to_send: Vec<(HostAndPort, Vec<BlockRequest>)> = Vec::new();

        // 1. send requests for new blocks for pieces currently downloading
        let mut piece_idx_to_remove = Vec::new();
        for (piece_idx, peer_addr) in self
            .outstanding_piece_assignments
            .iter()
            .map(|(i, p)| (*i, p.clone()))
            .collect::<Vec<(usize, HostAndPort)>>()
        {
            if let Some((incomplete_piece, requests_completed)) = self
                .requested_pieces
                .get(&peer_addr)
                .and_then(|requested_pieces_for_peer| requested_pieces_for_peer.get(&piece_idx))
            {
                if *requests_completed {
                    continue;
                }
                let peer = match peers.get(&peer_addr) {
                    None => continue,
                    Some(p) => p,
                };
                if peer.peer_choking_since().is_some() {
                    continue;
                }
                let reqs_for_piece = self.generate_requests_to_send_for_piece(
                    &peer_addr,
                    piece_idx,
                    incomplete_piece.clone(),
                    max_outstanding_reqs(peer),
                );
                requests_to_send.push((peer_addr, reqs_for_piece));
            } else {
                log::warn!(
                    "could not find requested piece {piece_idx} for peer {peer_addr}, this should never happen"
                );
                piece_idx_to_remove.push(piece_idx);
            }
        }
        for idx in piece_idx_to_remove {
            self.outstanding_piece_assignments.remove(&idx);
        }

        // 2. assign incomplete pieces if not assigned yet
        for (piece_idx, piece) in torrent_data_status.incomplete_pieces().iter() {
            if !self.outstanding_piece_assignments.contains_key(piece_idx)
                && let Some((peer_addr, reqs)) = self.assign_piece_reqs(*piece_idx, peers, piece)
                && !reqs.is_empty()
            {
                requests_to_send.push((peer_addr, reqs));
            }
        }

        // 3. assign other pieces, in order
        let mut some_peer_can_allocate = self.any_peer_can_allocate_requests(peers);
        for piece_idx in torrent_data_status.missing_pieces() {
            if !some_peer_can_allocate {
                break;
            }
            if self.outstanding_piece_assignments.len() >= MAX_OUTSTANDING_PIECES {
                break; // too many outstanding piece requests, stop assigment
            }
            if self.outstanding_piece_assignments.contains_key(piece_idx) {
                continue; // piece is already assigned, skip this
            }

            if let Some((peer_addr, reqs)) = self.assign_piece_reqs(
                *piece_idx,
                peers,
                &Piece::new(torrent_data_status.piece_length(*piece_idx)),
            ) && !reqs.is_empty()
            {
                requests_to_send.push((peer_addr, reqs));
                some_peer_can_allocate = self.any_peer_can_allocate_requests(peers);
            }
        }

        requests_to_send
    }

    fn any_peer_can_allocate_requests(&self, peers: &HashMap<HostAndPort, Peer>) -> bool {
        peers.iter().any(|(peer_addr, peer)| {
            peer.peer_choking_since().is_none()
                && self.peer_can_allocate_requests(peer_addr, max_outstanding_reqs(peer))
        })
    }

    fn assign_piece_reqs(
        &mut self,
        piece_idx: usize,
        peers: &HashMap<HostAndPort, Peer>,
        incomplete_piece: &Piece,
    ) -> Option<(HostAndPort, Vec<BlockRequest>)> {
        let mut peers_ready_for_new_requests = peers
            .iter()
            .filter(|(peer_addr, peer)| {
                peer.peer_choking_since().is_none()
                    && peer.have_piece(piece_idx)
                    && self
                        .outstanding_piece_block_requests
                        .get(*peer_addr)
                        .map(|o| o.len())
                        .unwrap_or(0)
                        < max_outstanding_reqs(peer)
            })
            .map(|(peer_addr, peer)| {
                let outstanding_piece_block_requests_count = self
                    .outstanding_piece_block_requests
                    .get(peer_addr)
                    .map(|o| o.len())
                    .unwrap_or(0);
                let concurrent_requested_pieces_count = self
                    .requested_pieces
                    .get(peer_addr)
                    .map(|o| o.len())
                    .unwrap_or(0);
                (
                    peer_addr,
                    peer,
                    concurrent_requested_pieces_count,
                    outstanding_piece_block_requests_count,
                )
            })
            .collect::<Vec<(&HostAndPort, &Peer, usize, usize)>>();

        peers_ready_for_new_requests.shuffle(&mut rand::rng());

        peers_ready_for_new_requests.sort_by(|a, b| {
            if a.2 < b.2 {
                // prefer lower concurrent_requested_pieces_count
                Ordering::Less
            } else if a.2 > b.2 {
                // prefer lower concurrent_requested_pieces_count
                Ordering::Greater
            } else if a.3 < b.3 {
                // if above equal, prefer lower outstanding_piece_block_requests_count
                Ordering::Less
            } else if a.3 > b.3 {
                // if above equal, prefer lower outstanding_piece_block_requests_count
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        });

        if !peers_ready_for_new_requests.is_empty() {
            let peer_addr = peers_ready_for_new_requests[0].0;
            let request_count = max_outstanding_reqs(peers_ready_for_new_requests[0].1);
            let reqs = self.generate_requests_to_send_for_piece(
                peer_addr,
                piece_idx,
                incomplete_piece.clone(),
                request_count,
            );
            return Option::Some((peer_addr.clone(), reqs));
        }

        // no candidate for new request found
        Option::None
    }

    fn peer_can_allocate_requests(
        &self,
        peer_addr: &HostAndPort,
        max_request_count_for_peer: usize,
    ) -> bool {
        self.outstanding_piece_block_requests
            .get(peer_addr)
            .map(|o| o.len())
            .unwrap_or(0)
            < max_request_count_for_peer
    }

    fn generate_requests_to_send_for_piece(
        &mut self,
        peer_addr: &HostAndPort,
        piece_idx: usize,
        mut incomplete_piece: Piece,
        max_request_count_for_peer: usize,
    ) -> Vec<BlockRequest> {
        let mut requests_to_send: Vec<BlockRequest> = Vec::new();
        // until we reach the max inflight requests for this peer...
        while self.peer_can_allocate_requests(peer_addr, max_request_count_for_peer) {
            match incomplete_piece.get_next_fragment(BLOCK_SIZE_B) {
                None => break, // no more blocks to request for this piece
                Some(Fragment { begin, end }) => {
                    let request = BlockRequest {
                        piece_idx: piece_idx as u32,
                        block_begin: begin as u32,
                        data_len: ((end - begin + 1) as u32),
                    };
                    requests_to_send.push(request.clone());
                    self.outstanding_piece_block_requests
                        .entry(peer_addr.clone())
                        .or_default()
                        .insert(request, Instant::now());
                    incomplete_piece.add_fragment(begin, end);
                }
            }
        }

        if !requests_to_send.is_empty() {
            self.requested_pieces
                .entry(peer_addr.clone())
                .or_default()
                .insert(
                    piece_idx,
                    (incomplete_piece.clone(), incomplete_piece.complete()),
                );
            self.outstanding_piece_assignments
                .insert(piece_idx, peer_addr.clone());
        }

        requests_to_send
    }

    pub fn generate_requests_to_send_for_peer(
        &mut self,
        peer_addr: &HostAndPort,
        peer: &Peer,
        torrent_data_status: &TorrentDataStatus,
    ) -> Vec<BlockRequest> {
        if peer.peer_choking_since().is_some() {
            return Vec::new();
        }
        let mut requests_to_send: Vec<BlockRequest> = Vec::new();
        let request_count = max_outstanding_reqs(peer);

        // 1. send requests for new blocks for pieces currently downloading
        match self.requested_pieces.get(peer_addr) {
            None => {} // the peer has no current piece assigned
            Some(requested_pieces_for_peer) => {
                for (piece_idx, incomplete_piece) in requested_pieces_for_peer
                    .iter()
                    .filter(|(_, (_, requests_completed))| !requests_completed)
                    .map(|(i, (p, _))| (*i, p.clone()))
                    .collect::<Vec<(usize, Piece)>>()
                {
                    if !torrent_data_status.piece_is_completed(piece_idx) {
                        let reqs = &mut self.generate_requests_to_send_for_piece(
                            peer_addr,
                            piece_idx,
                            incomplete_piece,
                            request_count,
                        );
                        requests_to_send.append(reqs);
                    }
                }
            }
        }

        // 2. assign incomplete pieces if not assigned yet
        for (piece_idx, piece) in torrent_data_status.incomplete_pieces().iter() {
            if !self.peer_can_allocate_requests(peer_addr, request_count) {
                break;
            }
            if peer.have_piece(*piece_idx)
                && !self.outstanding_piece_assignments.contains_key(piece_idx)
            {
                let reqs = &mut self.generate_requests_to_send_for_piece(
                    peer_addr,
                    *piece_idx,
                    piece.clone(),
                    request_count,
                );
                requests_to_send.append(reqs);
            }
        }

        // 3. assign other pieces, in order
        for piece_idx in torrent_data_status.missing_pieces() {
            if self.outstanding_piece_assignments.len() >= MAX_OUTSTANDING_PIECES {
                break;
            }
            if !self.peer_can_allocate_requests(peer_addr, request_count) {
                break;
            }
            if !peer.have_piece(*piece_idx) {
                continue;
            }
            if self.outstanding_piece_assignments.contains_key(piece_idx) {
                continue; // piece is already assigned, skip this
            }
            let reqs = &mut self.generate_requests_to_send_for_piece(
                peer_addr,
                *piece_idx,
                Piece::new(torrent_data_status.piece_length(*piece_idx)),
                request_count,
            );
            requests_to_send.append(reqs);
        }

        requests_to_send
    }
}

fn max_outstanding_reqs(peer: &Peer) -> usize {
    let bandwidth_down = peer.get_bandwidth_tracker().avg_bandwidth_down();

    let rtt = peer
        .get_rtt()
        .unwrap_or(Duration::from_secs(1))
        .as_secs_f64();

    // we want to pipeline requests to a peer so that the pipe is full for up to the target buffer time
    let target_buffer_time = (rtt * RTT_MULTIPLIER).clamp(
        MIN_TARGET_BUFFER_TIME_SECONDS,
        MAX_TARGET_BUFFER_TIME_SECONDS,
    );

    let reqs_to_fill_cur_bandwidth_for_buffer_time =
        (bandwidth_down / BLOCK_SIZE_B as f64 * target_buffer_time) as usize;

    // if current bandwith is 0, we want at least some requests to be performed
    let min_reqs = max(
        MIN_OUTSTANDING_BLOCK_REQUESTS,
        reqs_to_fill_cur_bandwidth_for_buffer_time,
    );

    // we never want to go above peer advertised reqq, and never too much also
    let max_reqs = min(
        peer.get_reqq(),
        MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER_HARD_LIMIT,
    );

    min(min_reqs, max_reqs)
}

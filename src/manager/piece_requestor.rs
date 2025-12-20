use crate::{
    manager::{peer::PeerAddr, torrent_manager::Peer},
    persistence::{file_manager::FileManager, piece::Piece},
    torrent_protocol::wire_protocol::BlockRequest,
};
use rand::seq::SliceRandom;
use std::{
    cmp::{Ordering, max, min},
    collections::HashMap,
    time::{Duration, SystemTime},
};

pub const MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER_HARD_LIMIT: usize = 3000;

const MAX_OUTSTANDING_PIECES: usize = 2000;
const MIN_OUTSTANDING_BLOCK_REQUESTS: usize = 10;
const BLOCK_SIZE_B: u64 = 16384;

// some peers choke and few moments after unchoke (a thing specs call "fibrillation").
// even if specs says:
// "The client should not attempt to send requests for blocks, and it should consider all pending (unanswered) requests to be discarded by the remote peer."
// here we wait a bit before considering the request lost and reassign pieces assigned to a choked peer to another peer
const CHOKED_PEER_ASSIGMENTS_GRACE_PERIOD: Duration = Duration::from_secs(15);

pub struct PieceRequestor {
    outstanding_piece_assignments: HashMap<usize, PeerAddr>, // piece idx -> peer_addr
    outstanding_piece_block_requests: HashMap<PeerAddr, HashMap<BlockRequest, SystemTime>>, // peer_addr -> BlockRequest -> request time
    requested_pieces: HashMap<PeerAddr, HashMap<usize, Piece>>, // peer_addr -> piece idx -> piece status with all the requested fragments
}

impl PieceRequestor {
    pub fn new() -> Self {
        PieceRequestor {
            outstanding_piece_assignments: HashMap::new(),
            outstanding_piece_block_requests: HashMap::new(),
            requested_pieces: HashMap::new(),
        }
    }

    pub fn outstanding_piece_block_request_count_for_peer(&self, peer_addr: &PeerAddr) -> usize {
        return self
            .outstanding_piece_block_requests
            .get(peer_addr)
            .map_or(0, |reqs| reqs.len());
    }

    pub fn remove_assigments_to_peer(&mut self, peer_addr: &PeerAddr) {
        self.outstanding_piece_block_requests.remove(peer_addr);
        if let Some(requests) = self.requested_pieces.remove(peer_addr) {
            for (piece_idx, _) in requests {
                self.outstanding_piece_assignments.remove(&piece_idx);
            }
        }
    }

    pub fn get_pending_block_requests_for_peer(&self, peer_addr: &PeerAddr) -> usize {
        self.outstanding_piece_block_requests
            .get(peer_addr)
            .map_or(0, |o| o.len())
    }

    pub fn get_assigned_pieces_for_peer(&self, peer_addr: &PeerAddr) -> usize {
        self.requested_pieces.get(peer_addr).map_or(0, |r| r.len())
    }

    pub fn block_request_completed(&mut self, peer_addr: &PeerAddr, block_request: &BlockRequest) {
        if let Some(reqs) = self.outstanding_piece_block_requests.get_mut(peer_addr) {
            match reqs.remove(block_request) {
                None => log::debug!(
                    "we received block {:?} from {peer_addr} but request was expired",
                    block_request
                ),
                Some(t) => {
                    let now = SystemTime::now();
                    if now.duration_since(t).unwrap_or_default() > Duration::from_secs(60) {
                        log::debug!(
                            "requested block from {peer_addr} arrived after {:#?}",
                            now.duration_since(t).unwrap_or_default()
                        );
                    }
                }
            }
        } else {
            log::debug!(
                "we received block {:?} from {peer_addr} but request was expired (no outstanding piece requests from this peer at all)",
                block_request
            );
        }
    }

    pub fn piece_request_completed(&mut self, peer_addr: &PeerAddr, piece_idx: usize) {
        self.outstanding_piece_assignments.remove(&piece_idx);
        if let Some(reqs) = self.requested_pieces.get_mut(peer_addr) {
            reqs.remove(&piece_idx);
        }
    }

    fn remove_assigments_to_choked(&mut self, peers: &HashMap<String, Peer>) {
        let mut peers_to_remove = Vec::new();
        for (peer_addr, _) in self.requested_pieces.iter() {
            if let Some(peer) = peers.get(peer_addr) {
                if peer.is_peer_choking()
                    && SystemTime::now()
                        .duration_since(peer.peer_choking_since())
                        .unwrap_or_default()
                        > CHOKED_PEER_ASSIGMENTS_GRACE_PERIOD
                {
                    peers_to_remove.push(peer_addr.clone());
                }
            }
        }
        for peer_addr in peers_to_remove {
            self.remove_assigments_to_peer(&peer_addr);
        }
    }

    pub fn remove_stale_requests(
        &mut self,
        request_timeout: Duration,
        peers: &HashMap<String, Peer>,
    ) -> Vec<(PeerAddr, BlockRequest)> // expired block requests that should be canceled
    {
        self.remove_assigments_to_choked(peers);

        let mut requests_to_cancel = Vec::<(PeerAddr, BlockRequest)>::new();
        let now = SystemTime::now();
        self.outstanding_piece_block_requests.iter_mut().for_each(
            |(peer_addr, outstanding_block_requests_for_peer)| {
                outstanding_block_requests_for_peer.retain(
                    |block_request, req_time| {
                        if now.duration_since(*req_time).unwrap_or_default() < request_timeout {
                            return true;
                        } else {
                            log::debug!("removed stale request to peer: {}: (piece idx: {}, block begin: {}, length: {})",
                                *peer_addr, block_request.piece_idx, block_request.block_begin, block_request.data_len);
                            requests_to_cancel.push((peer_addr.clone(), block_request.clone()));
                            // if a block stalled, we remove the assigment of the piece to this peer, with all associated block requests
                            if let Some(requested_pieces_for_peer) = self.requested_pieces.get_mut(peer_addr) {
                                requested_pieces_for_peer.remove(&(block_request.piece_idx as usize));
                            }
                            self.outstanding_piece_assignments.remove(&(block_request.piece_idx as usize));
                            return false;
                        };
                    },
                );
            },
        );

        requests_to_cancel
    }

    pub fn generate_requests_to_send(
        &mut self,
        peers: &HashMap<String, Peer>,
        file_manager: &FileManager,
    ) -> Vec<(PeerAddr, Vec<BlockRequest>)> {
        let mut requests_to_send: Vec<(PeerAddr, Vec<BlockRequest>)> = Vec::new();

        // 1. send requests for new blocks for pieces currently downloading
        let mut piece_idx_to_remove = Vec::new();
        for (piece_idx, peer_addr) in self
            .outstanding_piece_assignments
            .iter()
            .map(|(i, p)| (*i, p.clone()))
            .collect::<Vec<(usize, PeerAddr)>>()
        {
            if let Some(incomplete_piece) = self
                .requested_pieces
                .get(&peer_addr)
                .and_then(|requested_pieces_for_peer| requested_pieces_for_peer.get(&piece_idx))
            {
                let peer = match peers.get(&peer_addr) {
                    None => continue,
                    Some(p) => p,
                };
                if peer.is_peer_choking() {
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
        for (piece_idx, piece) in file_manager.incomplete_pieces().iter() {
            if !self.outstanding_piece_assignments.contains_key(piece_idx) {
                if let Some((peer_addr, reqs)) = self.assign_piece_reqs(*piece_idx, peers, piece) {
                    requests_to_send.push((peer_addr, reqs));
                }
            }
        }

        // 3. assign other pieces, in order
        for piece_idx in file_manager.missing_pieces() {
            if self.outstanding_piece_assignments.len() > MAX_OUTSTANDING_PIECES {
                break; // too many outstanding piece requests, stop assigment
            }
            if self.outstanding_piece_assignments.contains_key(piece_idx) {
                continue; // piece is already assigned, skip this
            }

            match self.assign_piece_reqs(
                *piece_idx,
                peers,
                &Piece::new(file_manager.piece_length(*piece_idx)),
            ) {
                Some((peer_addr, reqs)) => requests_to_send.push((peer_addr, reqs)),
                None => break, // we could not find a possible peer to assign this piece, it means there is no capacity left, stop assigment
            }
        }

        requests_to_send
    }

    fn assign_piece_reqs(
        &mut self,
        piece_idx: usize,
        peers: &HashMap<String, Peer>,
        incomplete_piece: &Piece,
    ) -> Option<(PeerAddr, Vec<BlockRequest>)> {
        let mut peers_ready_for_new_requests = peers
            .iter()
            .filter(|(peer_addr, peer)| {
                !peer.is_peer_choking()
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
            .collect::<Vec<(&String, &Peer, usize, usize)>>();

        peers_ready_for_new_requests.shuffle(&mut rand::rng());

        peers_ready_for_new_requests.sort_by(|a, b| {
            return if a.2 < b.2 {
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
            };
        });

        if peers_ready_for_new_requests.len() > 0 {
            let peer_addr = peers_ready_for_new_requests[0].0;
            let request_count = max_outstanding_reqs(peers_ready_for_new_requests[0].1);
            let reqs = self.generate_requests_to_send_for_piece(
                &peer_addr,
                piece_idx,
                incomplete_piece.clone(),
                request_count,
            );
            return Option::Some((peer_addr.clone(), reqs));
        }

        // no candidate for new request found
        return Option::None;
    }

    fn peer_can_allocate_requests(
        &self,
        peer_addr: &String,
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
        peer_addr: &String,
        piece_idx: usize,
        mut incomplete_piece: Piece,
        max_request_count_for_peer: usize,
    ) -> Vec<BlockRequest> {
        let mut requests_to_send: Vec<BlockRequest> = Vec::new();
        // until we reach the max inflight requests for this peer...
        while self.peer_can_allocate_requests(peer_addr, max_request_count_for_peer) {
            match incomplete_piece.get_next_fragment(BLOCK_SIZE_B) {
                None => break, // no more blocks to request for this piece
                Some((begin, end)) => {
                    let request = BlockRequest {
                        piece_idx: piece_idx as u32,
                        block_begin: begin as u32,
                        data_len: ((end - begin + 1) as u32),
                    };
                    requests_to_send.push(request.clone());
                    self.outstanding_piece_block_requests
                        .entry(peer_addr.clone())
                        .or_insert(HashMap::new())
                        .insert(request, SystemTime::now());
                    incomplete_piece.add_fragment(begin, end);
                }
            }
        }

        if !requests_to_send.is_empty() {
            self.requested_pieces
                .entry(peer_addr.clone())
                .or_insert(HashMap::new())
                .insert(piece_idx, incomplete_piece.clone());
            self.outstanding_piece_assignments
                .insert(piece_idx, peer_addr.clone());
        }

        requests_to_send
    }

    pub fn generate_requests_to_send_for_peer(
        &mut self,
        peer_addr: &String,
        peer: &Peer,
        file_manager: &FileManager,
    ) -> Vec<BlockRequest> {
        if peer.is_peer_choking() {
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
                    .map(|(i, p)| (*i, p.clone()))
                    .collect::<Vec<(usize, Piece)>>()
                {
                    if !file_manager.piece_completion_status(piece_idx) {
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
        for (piece_idx, piece) in file_manager.incomplete_pieces().iter() {
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
        for piece_idx in file_manager.missing_pieces() {
            if self.outstanding_piece_assignments.len() > MAX_OUTSTANDING_PIECES {
                break;
            }
            if !self.peer_can_allocate_requests(peer_addr, request_count) {
                break;
            }
            if !peer.have_piece(*piece_idx) {
                continue;
            }
            if self.outstanding_piece_assignments.contains_key(&piece_idx) {
                continue; // piece is already assigned, skip this
            }
            let reqs = &mut self.generate_requests_to_send_for_piece(
                peer_addr,
                *piece_idx,
                Piece::new(file_manager.piece_length(*piece_idx)),
                request_count,
            );
            requests_to_send.append(reqs);
        }

        requests_to_send
    }
}

fn max_outstanding_reqs(peer: &Peer) -> usize {
    // we want to pipeline requests to a peer so that we ask twice as much the current download rate
    let bandwidth_down = peer.bandwidth_tracker().bandwidth_down();
    let reqs_to_fill_cur_bandwidth_twice = (bandwidth_down / BLOCK_SIZE_B as f64 * 2.) as usize;

    // if current bandwith is 0, we want at least some requests to be performed
    let min_reqs = max(
        MIN_OUTSTANDING_BLOCK_REQUESTS,
        reqs_to_fill_cur_bandwidth_twice,
    );

    // we never want to go above peer advertised reqq, and never too much also
    let max_reqs = min(
        peer.get_reqq(),
        MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER_HARD_LIMIT,
    );
    return min(min_reqs, max_reqs);
}

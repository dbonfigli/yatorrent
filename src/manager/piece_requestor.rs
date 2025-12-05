use crate::{
    manager::{peer::PeerAddr, torrent_manager::Peer},
    persistence::{file_manager::FileManager, piece::Piece},
};
use rand::seq::SliceRandom;
use std::{
    cmp::Ordering,
    collections::HashMap,
    time::{Duration, SystemTime},
};

// can be retrieved per peer if it supports extensions, dict key "reqq",
// seen: deluge: 2000, qbittorrent: 500, transmission: 500, utorrent: 255, freebox bittorrent 2: 768, maybe variable.
// This parameter is extremelly important: a too low value will waste bandwidth in case a peer is really fast,
// a too high value will make the peer choke the connection and also saturate the channel capacity (see TO_PEER_CHANNEL_CAPACITY)
// 250 is the default in libtorrent as per https://bittorrent.org/beps/bep_0010.html
pub const DEFAULT_MAX_OUTSTANDING_REQUESTS_PER_PEER: usize = 250;

const MAX_OUTSTANDING_PIECES: usize = 2000;
const BLOCK_SIZE_B: u64 = 16384;

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
pub struct BlockRequest {
    pub piece_idx: u32,
    pub block_begin: u32,
    pub data_len: u32,
}

pub struct PieceRequestor {
    outstanding_piece_assignments: HashMap<usize, PeerAddr>, // piece idx -> peer_addr
    outstanding_block_requests: HashMap<PeerAddr, HashMap<BlockRequest, SystemTime>>, // peer_addr -> BlockRequest -> request time
    requested_pieces: HashMap<PeerAddr, HashMap<usize, Piece>>, // peer_addr -> piece idx -> piece status with all the requested fragments
}

impl PieceRequestor {
    pub fn new() -> Self {
        PieceRequestor {
            outstanding_piece_assignments: HashMap::new(),
            outstanding_block_requests: HashMap::new(),
            requested_pieces: HashMap::new(),
        }
    }

    pub fn outstanding_block_request_count_for_peer(&self, peer_addr: &PeerAddr) -> usize {
        return self
            .outstanding_block_requests
            .get(peer_addr)
            .map_or(0, |reqs| reqs.len());
    }

    pub fn remove_assigments_to_peer(&mut self, peer_addr: &PeerAddr) {
        self.outstanding_block_requests.remove(peer_addr);
        if let Some(requests) = self.requested_pieces.remove(peer_addr) {
            for (piece_idx, _) in requests {
                self.outstanding_piece_assignments.remove(&piece_idx);
            }
        }
    }

    pub fn block_request_completed(&mut self, peer_addr: &PeerAddr, block_request: &BlockRequest) {
        if let Some(reqs) = self.outstanding_block_requests.get_mut(peer_addr) {
            reqs.remove(block_request);
        }
    }

    pub fn piece_request_completed(&mut self, peer_addr: &PeerAddr, piece_idx: usize) {
        self.outstanding_piece_assignments.remove(&piece_idx);
        if let Some(reqs) = self.requested_pieces.get_mut(peer_addr) {
            reqs.remove(&piece_idx);
        }
    }

    pub fn remove_stale_requests(&mut self, request_timeout: Duration) {
        let now = SystemTime::now();
        self.outstanding_block_requests.iter_mut().for_each(
            |(peer_addr, outstanding_block_requests_for_peer)| {
                outstanding_block_requests_for_peer.retain(
                    |block_request, req_time| {
                        if now.duration_since(*req_time).unwrap_or_default() < request_timeout {
                            return true;
                        } else {
                            log::debug!("removed stale request to peer: {}: (piece idx: {}, block begin: {}, length: {})",
                                *peer_addr, block_request.piece_idx, block_request.block_begin, block_request.data_len);
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
    }

    pub fn generate_requests_to_send(
        &mut self,
        peers: &HashMap<String, Peer>,
        file_manager: &FileManager,
    ) -> Vec<(PeerAddr, Vec<BlockRequest>)> {
        let mut requests_to_send: Vec<(PeerAddr, Vec<BlockRequest>)> = Vec::new();

        // 1. send requests for new blocks for pieces currently downloading
        let mut piece_idx_to_remove = Vec::new();
        let outstanding_piece_assignments = self.outstanding_piece_assignments.clone();
        for (piece_idx, peer_addr) in outstanding_piece_assignments.iter() {
            if let Some(incomplete_piece) = self
                .requested_pieces
                .get(peer_addr)
                .and_then(|requested_pieces_for_peer| requested_pieces_for_peer.get(&piece_idx))
            {
                let reqs_for_piece = self.generate_requests_to_send_for_piece(
                    peer_addr,
                    *piece_idx,
                    incomplete_piece.clone(),
                    peers
                        .get(peer_addr)
                        .map(|p| p.get_reqq())
                        .unwrap_or(DEFAULT_MAX_OUTSTANDING_REQUESTS_PER_PEER),
                );
                requests_to_send.push((peer_addr.clone(), reqs_for_piece));
            } else {
                log::warn!(
                    "could not find requested piece {piece_idx} for peer {peer_addr}, this should never happen"
                );
                piece_idx_to_remove.push(*piece_idx);
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
        for piece_idx in 0..file_manager.num_pieces() {
            if self.outstanding_piece_assignments.len() > MAX_OUTSTANDING_PIECES {
                break; // too many outstanding piece requests, stop assigment
            }
            if file_manager.piece_completion_status(piece_idx)
                || self.outstanding_piece_assignments.contains_key(&piece_idx)
            {
                continue; // piece is already assigned or completed, skip this
            }

            match self.assign_piece_reqs(
                piece_idx,
                peers,
                &Piece::new(file_manager.piece_length(piece_idx)),
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
                        .outstanding_block_requests
                        .get(*peer_addr)
                        .map(|o| o.len())
                        .unwrap_or(0)
                        < peer.get_reqq()
            })
            .map(|(peer_addr, peer)| {
                let outstanding_block_requests_count = self
                    .outstanding_block_requests
                    .get(peer_addr)
                    .map(|o| o.len())
                    .unwrap_or(0);
                let concurrent_requested_pieces_count = self
                    .outstanding_block_requests
                    .get(peer_addr)
                    .map(|o| o.len())
                    .unwrap_or(0);
                (
                    peer_addr,
                    peer,
                    concurrent_requested_pieces_count,
                    outstanding_block_requests_count,
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
                // if above equal, prefer lower outstanding_block_requests_count
                Ordering::Less
            } else if a.3 > b.3 {
                // if above equal, prefer lower outstanding_block_requests_count
                Ordering::Greater
            } else {
                Ordering::Equal
            };
        });

        if peers_ready_for_new_requests.len() > 0 {
            let peer_addr = peers_ready_for_new_requests[0].0;
            let reqq = peers_ready_for_new_requests[0].1.get_reqq();
            let reqs = self.generate_requests_to_send_for_piece(
                &peer_addr,
                piece_idx,
                incomplete_piece.clone(),
                reqq,
            );
            return Option::Some((peer_addr.clone(), reqs));
        }

        // no candidate for new request found
        return Option::None;
    }

    fn generate_requests_to_send_for_piece(
        &mut self,
        peer_addr: &String,
        piece_idx: usize,
        mut incomplete_piece: Piece,
        reqq: usize,
    ) -> Vec<BlockRequest> {
        let mut requests_to_send: Vec<BlockRequest> = Vec::new();
        // until we reach the max inflight requests for this peer...
        while self
            .outstanding_block_requests
            .get(peer_addr)
            .map(|o| o.len())
            .unwrap_or(0)
            < reqq
        {
            match incomplete_piece.get_next_fragment(BLOCK_SIZE_B) {
                None => break, // no more blocks to request for this piece
                Some((begin, end)) => {
                    let request = BlockRequest {
                        piece_idx: piece_idx as u32,
                        block_begin: begin as u32,
                        data_len: ((end - begin + 1) as u32),
                    };
                    requests_to_send.push(request.clone());
                    self.outstanding_block_requests
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
        } else {
            // log::error!("empty assigment {peer_addr} {piece_idx}");
        }

        requests_to_send
    }
}

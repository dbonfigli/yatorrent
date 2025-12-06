use std::{
    cmp::min,
    collections::HashMap,
    time::{Duration, SystemTime},
};

use rand::seq::SliceRandom;

use crate::manager::{peer::PeerAddr, torrent_manager::Peer};

const METADATA_PIECE_SIZE_B: usize = 16384;
const PEER_METADATA_REQUEST_REJECTION_COOL_OFF_PERIOD: Duration = Duration::from_secs(30);
const METADATA_PIECE_REQUEST_TIMEOUT: Duration = Duration::from_secs(15); // timeout for waiting a requested metadata piece
const MAX_OUTSTANDING_METADATA_PIECE_REQUESTS_PER_PEER: i64 = 100;

pub struct MetadataHandler {
    metadata_piece_download_status: Vec<(bool, PeerAddr, SystemTime)>, // downloaded, peer addr we requested piece to, request time
    raw_metadata: Option<Vec<u8>>,
    raw_metadata_size: Option<i64>,
}

fn metadata_pieces_from_size(size: i64, default_value: bool) -> Vec<(bool, PeerAddr, SystemTime)> {
    vec![
        (
            default_value,
            "0.0.0.0:0".to_string(),
            SystemTime::UNIX_EPOCH
        );
        (size as f64 / METADATA_PIECE_SIZE_B as f64).ceil() as usize
    ]
}

impl MetadataHandler {
    pub fn new(raw_metadata_size: Option<i64>, raw_metadata: Option<Vec<u8>>) -> Self {
        let metadata_piece_download_status = match raw_metadata_size {
            None => Vec::new(),
            Some(s) => metadata_pieces_from_size(s, raw_metadata.is_some()),
        };

        let raw_metadata = match raw_metadata {
            Some(r) => Some(r),
            None => raw_metadata_size
                .map(|metadata_size| Some(vec![0; metadata_size as usize]))
                .unwrap_or(None),
        };

        MetadataHandler {
            metadata_piece_download_status,
            raw_metadata,
            raw_metadata_size,
        }
    }

    pub fn full_metadata_known(&self) -> bool {
        self.metadata_piece_download_status
            .iter()
            .all(|(completed, _, _)| *completed)
    }

    pub fn raw_metadata_size(&self) -> Option<i64> {
        self.raw_metadata_size.clone()
    }

    pub fn total_metadata_pieces_downloaded(&self) -> usize {
        self.metadata_piece_download_status
            .iter()
            .fold(0, |acc, v| if v.0 { acc + 1 } else { acc })
    }

    pub fn total_metadata_pieces(&self) -> usize {
        self.metadata_piece_download_status.len()
    }

    pub fn insert_piece(&mut self, piece_idx: usize, piece_data: Vec<u8>) {
        let raw_metadata_size = match self.raw_metadata_size {
            Some(raw_metadata_size) => raw_metadata_size as usize,
            None => return,
        };

        if piece_idx >= self.metadata_piece_download_status.len()
            || self.metadata_piece_download_status[piece_idx].0
        {
            return;
        }

        let raw_metadata_start = piece_idx * METADATA_PIECE_SIZE_B;
        let raw_metadata_end = min(
            raw_metadata_size,
            raw_metadata_start + METADATA_PIECE_SIZE_B,
        );
        self.raw_metadata
            .as_deref_mut()
            .expect("if raw_metadata_size is defined also raw_metadata is")
            [raw_metadata_start..raw_metadata_end]
            .copy_from_slice(&piece_data[..raw_metadata_end - raw_metadata_start]);

        self.metadata_piece_download_status[piece_idx].0 = true;
    }

    pub fn get_raw_metadata(&self) -> &Option<Vec<u8>> {
        &self.raw_metadata
    }

    pub fn get_piece(&self, piece_idx: usize) -> Option<(Vec<u8>, i64)> {
        if !self.full_metadata_known() {
            return None;
        }

        let raw_metadata_size = self
            .raw_metadata_size
            .expect("we know we have the metadata size since the full metadata is known");

        let piece_data_start = piece_idx * METADATA_PIECE_SIZE_B;
        let piece_data_end = min(
            raw_metadata_size as usize,
            piece_data_start + METADATA_PIECE_SIZE_B,
        );
        if piece_data_start < piece_data_end {
            let piece = self
                .raw_metadata
                .as_ref()
                .expect("we know we have the metadata size since the full metadata is known")
                [piece_data_start..piece_data_end]
                .to_vec();
            return Some((piece, raw_metadata_size));
        }
        return None;
    }

    pub fn generate_metadata_piece_reqs(
        &mut self,
        peers: &HashMap<String, Peer>,
    ) -> Vec<(PeerAddr, usize)> {
        // get inflight requests
        let mut inflight_metadata_piece_requests_per_peer: HashMap<PeerAddr, i64> = HashMap::new();
        for (downloaded, peer_addr, _) in self.metadata_piece_download_status.iter() {
            if *downloaded {
                continue;
            }

            inflight_metadata_piece_requests_per_peer
                .entry(peer_addr.clone())
                .and_modify(|outstanding_requests| *outstanding_requests += 1)
                .or_insert(1);
        }

        // get possible peers we can ask for metadata pieces, sorted by outstanding reqs
        let now = SystemTime::now();
        let mut possible_peers = peers
            .iter()
            .filter(|(_, peer)| {
                peer.support_metadata_extension()
                    && now
                        .duration_since(peer.get_last_metadata_request_rejection())
                        .unwrap_or_default()
                        > PEER_METADATA_REQUEST_REJECTION_COOL_OFF_PERIOD
            })
            .map(|(peer_addr, _)| {
                let outstanding_req = inflight_metadata_piece_requests_per_peer
                    .get(peer_addr)
                    .map(|reqs| *reqs)
                    .unwrap_or_default();
                (outstanding_req, peer_addr.clone())
            })
            .collect::<Vec<(i64, String)>>();
        possible_peers.shuffle(&mut rand::rng());
        possible_peers.sort_by_key(|k| k.0);

        if possible_peers.len() == 0 {
            return Vec::new();
        }

        let mut metadata_pieces_to_request: Vec<usize> = Vec::new();
        for n in 0..self.metadata_piece_download_status.len() {
            if !self.metadata_piece_download_status[n].0
                && now
                    .duration_since(self.metadata_piece_download_status[n].2)
                    .unwrap_or_default()
                    > METADATA_PIECE_REQUEST_TIMEOUT
            {
                metadata_pieces_to_request.push(n);
            }
        }

        let mut new_metadata_piece_requests = Vec::new();
        for (outstanding_reqs, peer_addr) in possible_peers.iter_mut() {
            while metadata_pieces_to_request.len() > 0 {
                if *outstanding_reqs > MAX_OUTSTANDING_METADATA_PIECE_REQUESTS_PER_PEER {
                    break;
                } else {
                    let piece_to_request = metadata_pieces_to_request[0];
                    self.metadata_piece_download_status[piece_to_request] =
                        (false, peer_addr.clone(), now);
                    new_metadata_piece_requests.push((peer_addr.clone(), piece_to_request));
                }
                metadata_pieces_to_request.remove(0);
            }
        }

        new_metadata_piece_requests
    }
}

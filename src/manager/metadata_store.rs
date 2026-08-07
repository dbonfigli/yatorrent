use std::{
    cmp::min,
    collections::HashMap,
    time::{Duration, SystemTime},
};

use anyhow::{Result, bail};
use rand::seq::SliceRandom;
use size::{Size, Style};

use crate::{manager::peer::Peer, util::HostAndPort};

const METADATA_PIECE_SIZE_B: usize = 16384;
const PEER_METADATA_REQUEST_REJECTION_COOL_OFF_PERIOD: Duration = Duration::from_secs(30);
const METADATA_PIECE_REQUEST_TIMEOUT: Duration = Duration::from_secs(15); // timeout for waiting a requested metadata piece
const MAX_OUTSTANDING_METADATA_PIECE_REQUESTS_PER_PEER: i64 = 100;
const METADATA_BIG_WARN_THRESHOLD: i64 = 20 * 1024 * 1024;
const METADATA_BIG_REJECT_THRESHOLD: i64 = 50 * 1024 * 1024;

#[derive(Clone)]
struct MetadataPieceDownloadStatus {
    downloaded: bool,
    request_destination_peer: HostAndPort,
    request_time: SystemTime,
}

pub struct MetadataPieceRequest {
    pub destination_peer: HostAndPort,
    pub piece_index: usize,
}

pub struct MetadataStore {
    metadata_piece_download_status: Vec<MetadataPieceDownloadStatus>, // The index in the vector is the piece index
    raw_metadata: Option<Vec<u8>>,
    raw_metadata_size: Option<i64>,
}

pub struct MetadataPieceReqResponse {
    pub piece: Vec<u8>,
    pub raw_metadata_size: i64,
}

fn metadata_pieces_from_size(size: i64, default_value: bool) -> Vec<MetadataPieceDownloadStatus> {
    vec![
        MetadataPieceDownloadStatus {
            downloaded: default_value,
            request_destination_peer: "0.0.0.0:0".to_string(),
            request_time: SystemTime::UNIX_EPOCH
        };
        (size as f64 / METADATA_PIECE_SIZE_B as f64).ceil() as usize
    ]
}

impl MetadataStore {
    pub fn new(raw_metadata_size: Option<i64>, raw_metadata: Option<Vec<u8>>) -> Result<Self> {
        // discard negative values
        let raw_metadata_size = raw_metadata_size.filter(|s| *s > 0);

        let metadata_piece_download_status = match raw_metadata_size {
            // We normally get the size with the peer handshake, but we could later discard it in case of metadata corruptions.
            // By setting the metadata_piece_download_status with size 1, will will perform requests for the piece 0 even if we
            // don't know the size, and with that we will get the it.
            None => vec![
                MetadataPieceDownloadStatus {
                    downloaded: false,
                    request_destination_peer: "0.0.0.0:0".to_string(),
                    request_time: SystemTime::UNIX_EPOCH
                };
                1
            ],
            Some(s) => {
                if s > METADATA_BIG_WARN_THRESHOLD {
                    log::warn!(
                        "the metadata size is abnormally big: {} (metadata is fully kept in memory)",
                        Size::from_bytes(s).format().with_style(Style::Abbreviated)
                    );
                }
                if s > METADATA_BIG_REJECT_THRESHOLD {
                    if raw_metadata.is_none() {
                        // return an error only in case we don't know the full metadata yet:
                        // * if we know it, it means it is coming from the torrent file passed by the user, so we trust the user that he really want this torrent, albeit strange
                        // * if we don't know it yet, it means we are about to download it from peers via magnet and maybe some bad peer has maliciously injected such large size
                        bail!(
                            "the metadata size is suspiciously big: {}. Metadata is fully kept in memory. Rejecting it. If needed, increase METADATA_BIG_REJECT_THRESHOLD (currently: {})",
                            Size::from_bytes(s).format().with_style(Style::Abbreviated),
                            Size::from_bytes(METADATA_BIG_REJECT_THRESHOLD)
                                .format()
                                .with_style(Style::Abbreviated),
                        );
                    }
                }

                metadata_pieces_from_size(s, raw_metadata.is_some())
            }
        };

        let raw_metadata = match raw_metadata {
            Some(r) => Some(r),
            None => raw_metadata_size.map(|metadata_size| vec![0; metadata_size as usize]),
        };

        Ok(MetadataStore {
            metadata_piece_download_status,
            raw_metadata,
            raw_metadata_size,
        })
    }

    pub fn full_metadata_known(&self) -> bool {
        !self.raw_metadata_size.is_none()
            && !self.metadata_piece_download_status.is_empty()
            && self
                .metadata_piece_download_status
                .iter()
                .all(|entry| entry.downloaded)
    }

    pub fn raw_metadata_size(&self) -> Option<i64> {
        self.raw_metadata_size
    }

    pub fn total_metadata_pieces_downloaded(&self) -> usize {
        self.metadata_piece_download_status
            .iter()
            .fold(0, |acc, v| if v.downloaded { acc + 1 } else { acc })
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
            || self.metadata_piece_download_status[piece_idx].downloaded
        {
            return;
        }

        let raw_metadata_start = piece_idx * METADATA_PIECE_SIZE_B;
        let raw_metadata_end = min(
            raw_metadata_size,
            raw_metadata_start + METADATA_PIECE_SIZE_B,
        );

        if piece_data.len() < (raw_metadata_end - raw_metadata_start) {
            // peer sent us less data than expected for this piece, avoid panic on copy_from_slice
            // in this case will ask for again for this piece immediatelly
            self.metadata_piece_download_status[piece_idx].request_time = SystemTime::UNIX_EPOCH;
            return;
        }

        self.raw_metadata
            .as_deref_mut()
            .expect("if raw_metadata_size is defined also raw_metadata is")
            [raw_metadata_start..raw_metadata_end]
            .copy_from_slice(&piece_data[..raw_metadata_end - raw_metadata_start]);

        self.metadata_piece_download_status[piece_idx].downloaded = true;
    }

    pub fn get_raw_metadata(&self) -> &Option<Vec<u8>> {
        &self.raw_metadata
    }

    pub fn generate_metadata_piece_req_response(
        &self,
        piece_idx: usize,
    ) -> Option<MetadataPieceReqResponse> {
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
            return Some(MetadataPieceReqResponse {
                piece,
                raw_metadata_size,
            });
        }
        return None;
    }

    pub fn generate_metadata_piece_reqs(
        &mut self,
        peers: &HashMap<HostAndPort, Peer>,
    ) -> Vec<MetadataPieceRequest> {
        // get inflight requests
        let mut inflight_metadata_piece_requests_per_peer: HashMap<HostAndPort, i64> =
            HashMap::new();
        let now = SystemTime::now();
        for MetadataPieceDownloadStatus {
            downloaded,
            request_destination_peer,
            request_time,
        } in self.metadata_piece_download_status.iter()
        {
            if *downloaded
                || now.duration_since(*request_time).unwrap_or_default()
                    > METADATA_PIECE_REQUEST_TIMEOUT
            {
                continue;
            }

            inflight_metadata_piece_requests_per_peer
                .entry(request_destination_peer.clone())
                .and_modify(|outstanding_requests| *outstanding_requests += 1)
                .or_insert(1);
        }

        // get possible peers we can ask for metadata pieces, sorted by outstanding reqs
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
            .collect::<Vec<(i64, HostAndPort)>>();
        possible_peers.shuffle(&mut rand::rng());
        possible_peers.sort_by_key(|k| k.0);

        if possible_peers.is_empty() {
            return Vec::new();
        }

        let mut metadata_pieces_to_request: Vec<usize> = Vec::new();
        for n in 0..self.metadata_piece_download_status.len() {
            if !self.metadata_piece_download_status[n].downloaded
                && now
                    .duration_since(self.metadata_piece_download_status[n].request_time)
                    .unwrap_or_default()
                    > METADATA_PIECE_REQUEST_TIMEOUT
            {
                metadata_pieces_to_request.push(n);
            }
        }

        if metadata_pieces_to_request.is_empty() {
            return Vec::new();
        }

        let mut new_metadata_piece_requests = Vec::new();
        let mut metadata_pieces_to_request_idx = 0;
        for (outstanding_reqs, peer_addr) in possible_peers.iter_mut() {
            if metadata_pieces_to_request_idx >= metadata_pieces_to_request.len() {
                break;
            }
            while metadata_pieces_to_request_idx < metadata_pieces_to_request.len() {
                if *outstanding_reqs >= MAX_OUTSTANDING_METADATA_PIECE_REQUESTS_PER_PEER {
                    break;
                }
                let piece_to_request = metadata_pieces_to_request[metadata_pieces_to_request_idx];
                self.metadata_piece_download_status[piece_to_request] =
                    MetadataPieceDownloadStatus {
                        downloaded: false,
                        request_destination_peer: peer_addr.clone(),
                        request_time: now,
                    };
                new_metadata_piece_requests.push(MetadataPieceRequest {
                    destination_peer: peer_addr.clone(),
                    piece_index: piece_to_request,
                });
                *outstanding_reqs += 1;
                metadata_pieces_to_request_idx += 1;
            }
        }

        new_metadata_piece_requests
    }
}

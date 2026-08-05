use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{iter, path::Path};

use rand::RngExt;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::manager::bandwidth_tracker::BandwidthTracker;

use crate::manager::peer::Peer;
use crate::manager::peer_handler;
use crate::manager::peer_handler::{PeerAddr, PeersToManagerMsg, ToNewIncomingPeersHandlerMsg};
use crate::manager::piece_requestor::PieceRequestor;
use crate::manager::rate_limiter::RateLimiter;
use crate::manager::torrent_manager::dht_state::DhtState;
use crate::manager::torrent_manager::file_manager_state::FileManagerState;
use crate::manager::torrent_manager::metadata::MetadataState;
use crate::manager::torrent_manager::pex::PexHandler;
use crate::manager::torrent_manager::tracker_state::TrackerState;
use crate::persistence::torrent_data_status::TorrentDataStatus;
use crate::tracker;

mod control_loop;
mod dht_state;
mod file_manager_state;
mod log_stats;
mod metadata;
mod peer_message_handler;
pub(super) mod pex;
mod ticker_handler;
mod tracker_state;
mod util;

// this is mostly the number of inflight (i.e. not fulfilled) requests from peers
// and downloaded blocks from peers, the latter in particular are holding the block buffers
// if we are slow on writes, these will pile up and consume memory
// for example, assuming 16kb blocks, 50000 blocks is 781MB
const PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY: usize = 50000;

// decreasing this will waste more bandwidth (needlessly requesting the same block again even if a peer sends it eventually) but will make retries for pieces requested to slow peers faster
// eventually we should tune this respect to download spped from a peer and how many outstanding requests we made
const BASE_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

const TO_NEW_INCOMING_PEERS_HANDLER_CHANNEL_CAPACITY: usize = 100;

struct TorrentManagerConfig {
    info_hash: [u8; 20],
    own_peer_id: String,
    listening_torrent_wire_protocol_port: u16,
    listening_dht_port: u16,
    show_peers_stats: bool,
    // if we reach this, we stop starting new connections to other peers we know, what we have should be enough
    // todo: we should add some churning of slow downloading peers upon reaching this limit
    // todo: we should also cap more this if there are already peers with a good dwnloading speed
    // otherwise we are just spreading too much the download bandwidth on too many peers, each with a small bandwidth speed
    // risking to be choked by them because of this and generally being inefficient
    max_connected_peers: usize,
    exit_when_complete: bool,
}

struct PeersState {
    peers: HashMap<PeerAddr, Peer>,
    advertised_peers: Arc<Mutex<HashMap<PeerAddr, (tracker::Peer, SystemTime)>>>, // peer addr -> (peer, last connection attempt)
    bad_peers: HashSet<PeerAddr>, // todo: remove old bad peers after a while?
    to_new_incoming_peers_handler_tx: Sender<ToNewIncomingPeersHandlerMsg>,
    to_new_incoming_peers_handler_rx: Option<Receiver<ToNewIncomingPeersHandlerMsg>>, // optional bc we will move it to the incoming peer handler at start, todo: should we move creation of this channel there?
    peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
    peers_to_torrent_manager_rx: Receiver<PeersToManagerMsg>,
}

struct RateLimiterState {
    download_rate_limiter: Option<Arc<tokio::sync::Mutex<RateLimiter>>>,
    upload_rate_limiter: Option<Arc<tokio::sync::Mutex<RateLimiter>>>,
}

pub struct TorrentManager {
    torrent_manager_config: TorrentManagerConfig,
    torrent_data_status: Option<TorrentDataStatus>,
    peers_state: PeersState,
    dht_state: DhtState,
    rate_limiter_state: RateLimiterState,
    file_manager_state: FileManagerState,
    metadata_state: MetadataState,
    tracker_state: TrackerState,
    bandwidth_tracker: BandwidthTracker,
    piece_requestor: PieceRequestor,
    pex_handler: PexHandler,
    request_timeout: Duration,
}

impl TorrentManager {
    pub fn new(
        info_hash: [u8; 20],
        base_path: &Path,
        listening_torrent_wire_protocol_port: u16,
        announce_list: Vec<Vec<String>>,
        files_data: Option<(
            Vec<(String, u64)>, // files_list
            u64,                // piece_length
            Vec<[u8; 20]>,      // piece_hashes
        )>,
        raw_metadata: Option<Vec<u8>>,
        listening_dht_port: u16,
        dht_nodes: Vec<String>,
        initial_peers: Vec<String>,
        show_peers_details: bool,
        max_connected_peers: usize,
        max_download_bandwidth: Option<i64>,
        max_upload_bandwidth: Option<i64>,
        exit_when_complete: bool,
    ) -> Self {
        let own_peer_id = generate_peer_id();
        let mut initial_advertised_peers = HashMap::new();
        for peer_addr in initial_peers {
            let ip_and_port = peer_addr
                .rsplit_once(':')
                .expect("all initial peers should be of the host:port format");
            let p = tracker::Peer {
                peer_id: None,
                ip: ip_and_port.0.to_string(),
                port: ip_and_port
                    .1
                    .to_string()
                    .parse::<u16>()
                    .expect("all initial peers should be of the host:port format"),
            };
            initial_advertised_peers.insert(peer_addr, (p, SystemTime::UNIX_EPOCH));
        }
        let advertised_peers = Arc::new(Mutex::new(initial_advertised_peers));
        let (to_new_incoming_peers_handler_tx, to_new_incoming_peers_handler_rx) =
            mpsc::channel(TO_NEW_INCOMING_PEERS_HANDLER_CHANNEL_CAPACITY);
        let (peers_to_torrent_manager_tx, peers_to_torrent_manager_rx) =
            mpsc::channel::<PeersToManagerMsg>(PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY);

        let mut torrent_manager = TorrentManager {
            torrent_manager_config: TorrentManagerConfig {
                info_hash,
                own_peer_id: own_peer_id.clone(),
                listening_torrent_wire_protocol_port,
                listening_dht_port,
                show_peers_stats: show_peers_details,
                max_connected_peers,
                exit_when_complete,
            },
            torrent_data_status: None,
            peers_state: PeersState {
                peers: HashMap::new(),
                advertised_peers,
                bad_peers: HashSet::new(),
                to_new_incoming_peers_handler_tx,
                to_new_incoming_peers_handler_rx: Some(to_new_incoming_peers_handler_rx),
                peers_to_torrent_manager_tx,
                peers_to_torrent_manager_rx,
            },
            dht_state: DhtState::new(dht_nodes, info_hash),
            metadata_state: MetadataState::new(
                raw_metadata.as_ref().map(|m| m.len() as i64).or(None),
                raw_metadata,
                info_hash,
                PathBuf::from(base_path),
            ),
            tracker_state: TrackerState::new(
                own_peer_id,
                announce_list,
                listening_torrent_wire_protocol_port,
                info_hash,
            ),
            bandwidth_tracker: BandwidthTracker::new(),
            piece_requestor: PieceRequestor::new(),
            pex_handler: PexHandler::new(),
            request_timeout: BASE_REQUEST_TIMEOUT,
            rate_limiter_state: RateLimiterState {
                download_rate_limiter: max_download_bandwidth
                    .map(|b| Arc::new(tokio::sync::Mutex::new(RateLimiter::new(b as u128)))),
                upload_rate_limiter: max_upload_bandwidth
                    .map(|b| Arc::new(tokio::sync::Mutex::new(RateLimiter::new(b as u128)))),
            },
            file_manager_state: FileManagerState::new(),
        };

        if let Some((file_list, piece_length, piece_hashes)) = files_data {
            torrent_manager.torrent_data_status = Some(torrent_manager.file_manager_state.start(
                base_path,
                file_list,
                piece_length,
                piece_hashes,
            ));
        }

        torrent_manager
    }

    pub async fn start(&mut self) {
        // start dht manager
        let dht_to_torrent_manager_rx = self.dht_state.start_dht_manager(
            self.torrent_manager_config
                .listening_torrent_wire_protocol_port,
            self.torrent_manager_config.listening_dht_port,
        );

        // start incoming peer connections handler
        peer_handler::run_new_incoming_peers_handler(
            self.torrent_manager_config.info_hash.clone(),
            self.torrent_manager_config.own_peer_id.clone(),
            self.torrent_manager_config
                .listening_torrent_wire_protocol_port
                .clone(),
            self.torrent_data_status
                .as_ref()
                .map(|f| f.current_piece_completion_status()),
            self.peers_state
                .to_new_incoming_peers_handler_rx
                .take()
                .expect("no to_new_incoming_peers_handler_rx, has start been called twice?"),
            self.peers_state.peers_to_torrent_manager_tx.clone(),
            self.metadata_state.raw_metadata_size(),
        )
        .await;

        // start control loop to handle channel messages - will block forever
        self.control_loop(dht_to_torrent_manager_rx).await;
    }
}

fn generate_peer_id() -> String {
    const CHARSET: &[u8] = b"0123456789";
    let mut rng = rand::rng();
    let one_char = || CHARSET[rng.random_range(0..CHARSET.len())] as char;
    let random_string: String = iter::repeat_with(one_char).take(12).collect();
    format!("-YT0001-{random_string}")
}

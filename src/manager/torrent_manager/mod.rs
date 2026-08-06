use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{iter, path::Path};

use rand::RngExt;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::manager::bandwidth_tracker::BandwidthTracker;

use crate::manager::dht_handler::DhtHandler;
use crate::manager::peer::Peer;
use crate::manager::peer_handler;
use crate::manager::peer_handler::{PeersToManagerMsg, ToNewIncomingPeersHandlerMsg};
use crate::manager::pex_handler::PexHandler;
use crate::manager::piece_requestor::PieceRequestor;
use crate::manager::rate_limiter::RateLimiter;
use crate::manager::torrent_manager::file_manager_handler::FileManagerHandler;
use crate::manager::torrent_manager::metadata_handler::MetadataHandler;
use crate::manager::tracker_requestor::TrackerRequestor;
use crate::persistence::torrent_data_status::TorrentDataStatus;
use crate::tracker;
use crate::util::{FileEntry, HostAndPort};

mod control_loop;
mod file_manager_handler;
mod log_stats;
mod metadata_handler;
mod peer_message_handler;
mod ticker_handler;
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

pub struct FilesData {
    pub file_list: Vec<FileEntry>,
    pub piece_length: u64,
    pub piece_hashes: Vec<[u8; 20]>,
}

pub struct TorrentManagerLimitOptions {
    pub max_connected_peers: usize,
    pub max_download_bandwidth: Option<i64>,
    pub max_upload_bandwidth: Option<i64>,
}

pub struct TorrentManagerStorageOptions {
    pub base_path: String,
    pub files_data: Option<FilesData>,
    pub raw_metadata: Option<Vec<u8>>,
}

pub struct TorrentManagerNetworkOptions {
    pub listening_torrent_wire_protocol_port: u16,
    pub listening_dht_port: u16,
    pub dht_nodes: Vec<HostAndPort>,
    pub initial_peers: Vec<HostAndPort>,
}

pub struct TorrentManagerOptions {
    pub info_hash: [u8; 20],
    pub network_opts: TorrentManagerNetworkOptions,
    pub storage_opts: TorrentManagerStorageOptions,
    pub limit_opts: TorrentManagerLimitOptions,
    pub tracker_announce_list: Vec<Vec<String>>,
    pub show_peers_stats: bool,
    pub exit_when_complete: bool,
}

struct TorrentManagerConfig {
    base_path: PathBuf,
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

struct PeersContext {
    peers: HashMap<HostAndPort, Peer>,
    advertised_peers: Arc<Mutex<HashMap<HostAndPort, (tracker::Peer, SystemTime)>>>, // peer addr -> (peer, last connection attempt)
    bad_peers: HashSet<HostAndPort>, // todo: remove old bad peers after a while?
    to_new_incoming_peers_handler_tx: Sender<ToNewIncomingPeersHandlerMsg>,
    to_new_incoming_peers_handler_rx: Option<Receiver<ToNewIncomingPeersHandlerMsg>>, // optional bc we will move it to the incoming peer handler at start, todo: should we move creation of this channel there?
    peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
    peers_to_torrent_manager_rx: Receiver<PeersToManagerMsg>,
}

struct GlobalRateLimiter {
    download_rate_limiter: Option<Arc<tokio::sync::Mutex<RateLimiter>>>,
    upload_rate_limiter: Option<Arc<tokio::sync::Mutex<RateLimiter>>>,
}

pub struct TorrentManager {
    torrent_manager_config: TorrentManagerConfig,
    file_manager_handler: FileManagerHandler,
    torrent_data_status: Option<TorrentDataStatus>,
    peers_ctx: PeersContext,
    dht_handler: DhtHandler,
    global_rate_limiter: GlobalRateLimiter,
    metadata_handler: MetadataHandler,
    tracker_requestor: TrackerRequestor,
    bandwidth_tracker: BandwidthTracker,
    piece_requestor: PieceRequestor,
    pex_handler: PexHandler,
    request_timeout: Duration,
}

impl TorrentManager {
    pub fn new(opts: TorrentManagerOptions) -> Self {
        let own_peer_id = generate_peer_id();
        let base_path = Path::new(&opts.storage_opts.base_path);

        let mut initial_advertised_peers = HashMap::new();
        for peer_addr in opts.network_opts.initial_peers {
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

        let torrent_manager_config = TorrentManagerConfig {
            base_path: PathBuf::from(base_path),
            info_hash: opts.info_hash,
            own_peer_id: own_peer_id.clone(),
            listening_torrent_wire_protocol_port: opts
                .network_opts
                .listening_torrent_wire_protocol_port,
            listening_dht_port: opts.network_opts.listening_dht_port,
            show_peers_stats: opts.show_peers_stats,
            max_connected_peers: opts.limit_opts.max_connected_peers,
            exit_when_complete: opts.exit_when_complete,
        };

        let mut file_manager_handler = FileManagerHandler::new();
        let torrent_data_status = opts.storage_opts.files_data.map(
            |FilesData {
                 file_list,
                 piece_length,
                 piece_hashes,
             }| {
                file_manager_handler.start(base_path, file_list, piece_length, piece_hashes)
            },
        );

        let (to_new_incoming_peers_handler_tx, to_new_incoming_peers_handler_rx) =
            mpsc::channel(TO_NEW_INCOMING_PEERS_HANDLER_CHANNEL_CAPACITY);
        let (peers_to_torrent_manager_tx, peers_to_torrent_manager_rx) =
            mpsc::channel::<PeersToManagerMsg>(PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY);
        let peers_ctx = PeersContext {
            peers: HashMap::new(),
            advertised_peers,
            bad_peers: HashSet::new(),
            to_new_incoming_peers_handler_tx,
            to_new_incoming_peers_handler_rx: Some(to_new_incoming_peers_handler_rx),
            peers_to_torrent_manager_tx,
            peers_to_torrent_manager_rx,
        };

        let global_rate_limiter = GlobalRateLimiter {
            download_rate_limiter: opts
                .limit_opts
                .max_download_bandwidth
                .map(|b| Arc::new(tokio::sync::Mutex::new(RateLimiter::new(b as u128)))),
            upload_rate_limiter: opts
                .limit_opts
                .max_upload_bandwidth
                .map(|b| Arc::new(tokio::sync::Mutex::new(RateLimiter::new(b as u128)))),
        };

        let metadata_handler = MetadataHandler::new(
            opts.storage_opts
                .raw_metadata
                .as_ref()
                .map(|m| m.len() as i64)
                .or(None),
            opts.storage_opts.raw_metadata,
            opts.info_hash,
        );

        let tracker_requestor = TrackerRequestor::new(
            own_peer_id,
            opts.tracker_announce_list,
            opts.network_opts.listening_torrent_wire_protocol_port,
            opts.info_hash,
        );

        TorrentManager {
            torrent_manager_config,
            file_manager_handler,
            torrent_data_status,
            peers_ctx,
            dht_handler: DhtHandler::new(opts.network_opts.dht_nodes, opts.info_hash),
            global_rate_limiter,
            metadata_handler,
            tracker_requestor,
            bandwidth_tracker: BandwidthTracker::new(),
            piece_requestor: PieceRequestor::new(),
            pex_handler: PexHandler::new(),
            request_timeout: BASE_REQUEST_TIMEOUT,
        }
    }

    pub async fn start(&mut self) {
        // start dht manager
        let dht_to_torrent_manager_rx = self.dht_handler.start_dht_manager(
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
            self.peers_ctx
                .to_new_incoming_peers_handler_rx
                .take()
                .expect("no to_new_incoming_peers_handler_rx, has start been called twice?"),
            self.peers_ctx.peers_to_torrent_manager_tx.clone(),
            self.metadata_handler.raw_metadata_size(),
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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::{iter, path::Path};

use rand::RngExt;
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender};

use crate::manager::bandwidth_tracker::BandwidthTracker;

use crate::manager::dht_handler::DhtHandler;
use crate::manager::peer_handler::PeerMessage;
use crate::manager::peer_handler::{self, PeerHandlerToManagerMsg, ToNewIncomingPeersHandlerMsg};
use crate::manager::pex_handler::PexHandler;
use crate::manager::piece_requestor::PieceRequestor;
use crate::manager::rate_limiter::RateLimiter;
use crate::manager::torrent_manager::file_manager_handler::FileManagerHandler;
use crate::manager::torrent_manager::metadata_handler::MetadataHandler;
use crate::manager::torrent_manager::peer_context::PeersContext;
use crate::manager::tracker_requestor::TrackerRequestor;
use crate::persistence::torrent_data_status::TorrentDataStatus;
use crate::tracker;
use crate::util::{FileEntry, HostAndPort};

mod control_loop;
mod file_manager_handler;
mod log_stats;
mod metadata_handler;
pub mod peer_context;
mod peer_message_handler;
mod ticker_handler;
mod util;

// this is mostly the number of inflight (i.e. not fulfilled) requests from peers
// and downloaded blocks from peers, the latter in particular are holding the block buffers
// if we are slow on writes, these will pile up and consume memory
// for example, assuming 16kb blocks, 50000 blocks is 781MB
const INCOMING_PEER_MESSAGES_CHANNEL_CAPACITY: usize = 50000;

// decreasing this will waste more bandwidth (needlessly requesting the same block again even if a peer sends it eventually) but will make retries for pieces requested to slow peers faster
// eventually we should tune this respect to download spped from a peer and how many outstanding requests we made
const BASE_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

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
    pub dht_enabled: bool,
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

struct PeersChannels {
    to_new_incoming_peers_handler_tx: UnboundedSender<ToNewIncomingPeersHandlerMsg>,
    to_new_incoming_peers_handler_rx: Option<UnboundedReceiver<ToNewIncomingPeersHandlerMsg>>, // optional bc we will move it to the incoming peer handler at start, todo: should we move creation of this channel there?
    peer_handler_to_torrent_manager_tx: UnboundedSender<PeerHandlerToManagerMsg>,
    peer_handler_to_torrent_manager_rx: UnboundedReceiver<PeerHandlerToManagerMsg>,
    incoming_peer_messages_tx: Sender<PeerMessage>,
    incoming_peer_messages_rx: Receiver<PeerMessage>,
}

struct GlobalRateLimiter {
    download_rate_limiter: Option<Arc<tokio::sync::Mutex<RateLimiter>>>,
    upload_rate_limiter: Option<Arc<tokio::sync::Mutex<RateLimiter>>>,
}

#[derive(Debug, Error)]
#[error("unrecoverable error: {error}")]
pub struct UnrecoverableError {
    // maybe add something structured in the future
    error: String,
}

impl UnrecoverableError {
    pub fn new(error: String) -> UnrecoverableError {
        UnrecoverableError { error }
    }
}

pub enum ShutdownRequest {
    Success,
    Error(UnrecoverableError),
}

pub struct TorrentManager {
    torrent_manager_config: TorrentManagerConfig,
    file_manager_handler: FileManagerHandler,
    torrent_data_status: Option<TorrentDataStatus>,
    peers_ctx: PeersContext,
    peers_channels: PeersChannels,
    dht_handler: DhtHandler,
    global_rate_limiter: GlobalRateLimiter,
    metadata_handler: MetadataHandler,
    tracker_requestor: TrackerRequestor,
    bandwidth_tracker: BandwidthTracker,
    piece_requestor: PieceRequestor,
    pex_handler: PexHandler,
    request_timeout: Duration,
    shutdown_request_tx: UnboundedSender<ShutdownRequest>,
    shutdown_request_rx: UnboundedReceiver<ShutdownRequest>,
}

impl TorrentManager {
    pub fn new(opts: TorrentManagerOptions) -> Result<Self, UnrecoverableError> {
        let own_peer_id = generate_peer_id();
        let base_path = Path::new(&opts.storage_opts.base_path);

        let mut initial_advertised_peers = Vec::new();
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
            initial_advertised_peers.push(p);
        }

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

        let torrent_data_status = if let Some(files_data) = opts.storage_opts.files_data {
            match file_manager_handler.start(
                base_path,
                files_data.file_list,
                files_data.piece_length,
                files_data.piece_hashes,
            ) {
                Ok(torrent_data_status) => Some(torrent_data_status),
                Err(e) => {
                    return Result::Err(UnrecoverableError::new(format!(
                        "initialization of torrent data failed: {e}"
                    )));
                }
            }
        } else {
            None
        };

        let (to_new_incoming_peers_handler_tx, to_new_incoming_peers_handler_rx) =
            mpsc::unbounded_channel();
        let (peer_handler_to_torrent_manager_tx, peer_handler_to_torrent_manager_rx) =
            mpsc::unbounded_channel();
        let (peers_to_torrent_manager_tx, peers_to_torrent_manager_rx) =
            mpsc::channel::<PeerMessage>(INCOMING_PEER_MESSAGES_CHANNEL_CAPACITY);

        let peers_channels = PeersChannels {
            to_new_incoming_peers_handler_tx,
            to_new_incoming_peers_handler_rx: Some(to_new_incoming_peers_handler_rx),
            peer_handler_to_torrent_manager_tx,
            peer_handler_to_torrent_manager_rx,
            incoming_peer_messages_tx: peers_to_torrent_manager_tx,
            incoming_peer_messages_rx: peers_to_torrent_manager_rx,
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

        let metadata_handler = match MetadataHandler::new(
            opts.storage_opts
                .raw_metadata
                .as_ref()
                .map(|m| m.len() as i64)
                .or(None),
            opts.storage_opts.raw_metadata,
            opts.info_hash,
        ) {
            Ok(metadata_handler) => metadata_handler,
            Err(e) => {
                log::error!("initialization failed: {e}");
                return Err(UnrecoverableError::new(e.to_string()));
            }
        };

        let tracker_requestor = TrackerRequestor::new(
            own_peer_id,
            opts.tracker_announce_list,
            opts.network_opts.listening_torrent_wire_protocol_port,
            opts.info_hash,
        );

        let (shutdown_request_tx, shutdown_request_rx) = mpsc::unbounded_channel();

        Ok(TorrentManager {
            torrent_manager_config,
            file_manager_handler,
            torrent_data_status,
            peers_ctx: PeersContext::new(initial_advertised_peers),
            peers_channels,
            dht_handler: DhtHandler::new(
                opts.dht_enabled,
                opts.network_opts.dht_nodes,
                opts.info_hash,
            ),
            global_rate_limiter,
            metadata_handler,
            tracker_requestor,
            bandwidth_tracker: BandwidthTracker::new(),
            piece_requestor: PieceRequestor::new(),
            pex_handler: PexHandler::new(),
            request_timeout: BASE_REQUEST_TIMEOUT,
            shutdown_request_tx,
            shutdown_request_rx,
        })
    }

    pub async fn start(&mut self) -> Result<(), UnrecoverableError> {
        if self.torrent_manager_config.exit_when_complete
            && let Some(torrent_data_status) = self.torrent_data_status.as_ref()
            && torrent_data_status.completed()
        {
            log::info!("torrent fully downloaded; exiting");
            return Ok(());
        }

        // start dht manager
        self.dht_handler.start_dht_manager(
            self.torrent_manager_config
                .listening_torrent_wire_protocol_port,
            self.torrent_manager_config.listening_dht_port,
        );

        // start incoming peer connections handler
        peer_handler::run_new_incoming_peers_handler(
            self.torrent_manager_config.info_hash,
            self.torrent_manager_config.own_peer_id.clone(),
            self.torrent_manager_config
                .listening_torrent_wire_protocol_port,
            self.torrent_data_status
                .as_ref()
                .map(|f| f.current_piece_completion_status()),
            self.peers_channels
                .to_new_incoming_peers_handler_rx
                .take()
                .expect("no to_new_incoming_peers_handler_rx, has start been called twice?"),
            self.peers_channels
                .peer_handler_to_torrent_manager_tx
                .clone(),
            self.metadata_handler.raw_metadata_size(),
        )
        .await;

        // start control loop to handle channel messages
        return self.control_loop().await;
    }
}

fn generate_peer_id() -> String {
    const CHARSET: &[u8] = b"0123456789";
    let mut rng = rand::rng();
    let one_char = || CHARSET[rng.random_range(0..CHARSET.len())] as char;
    let random_string: String = iter::repeat_with(one_char).take(12).collect();
    format!("-YT0001-{random_string}")
}

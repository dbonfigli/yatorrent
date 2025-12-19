use anyhow::{Error, Result, bail};
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{iter, path::Path};

use rand::Rng;
use rand::seq::IndexedRandom;
use size::{Size, Style};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::dht::dht_manager::{DhtManager, DhtToTorrentManagerMsg, ToDhtManagerMsg};
use crate::manager::metadata_handler::MetadataHandler;
use crate::manager::peer::{
    self, MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER, PeerAddr, PeersToManagerMsg,
    ToPeerCancelMsg, ToPeerMsg,
};
use crate::manager::piece_requestor::{
    MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER_HARD_LIMIT, PieceRequestor,
};
use crate::metadata::infodict::{self};
use crate::metadata::metainfo::get_files;
use crate::persistence::file_manager::ShaCorruptedError;
use crate::torrent_protocol::wire_protocol::{BlockRequest, Message};
use crate::tracker;
use crate::util::{force_string, start_tick};
use crate::{
    persistence::file_manager::FileManager,
    tracker::{Event, NoTrackerError, Response, TrackerClient},
};

use crate::bencoding::Value::{self, Dict, Int, Str};

use super::peer::PeerError;

const CONNECTED_PEERS_TO_STOP_INCOMING_PEER_CONNECTIONS: usize = 500;
const CONNECTED_PEERS_TO_START_NEW_PEER_CONNECTIONS: usize = 350;
const MAX_CONNECTED_PEERS_TO_ASK_DHT_FOR_MORE: usize = 10;
const DHT_NEW_PEER_COOL_OFF_PERIOD: Duration = Duration::from_secs(15);
const DHT_BOOTSTRAP_TIME: Duration = Duration::from_secs(5);
const KEEP_ALIVE_FREQ: Duration = Duration::from_secs(90);
const TO_PEER_CHANNEL_CAPACITY: usize =
    MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER_HARD_LIMIT + 700;
const TO_PEER_CANCEL_CHANNEL_CAPACITY: usize =
    MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER_HARD_LIMIT + 200;

// can be retrieved per peer if it supports extensions, dict key "reqq",
// seen: deluge: 2000, qbittorrent: 500, transmission: 500, utorrent: 255, freebox bittorrent 2: 768, maybe variable.
// This parameter is extremelly important: a too low value will waste bandwidth in case a peer is really fast,
// a too high value will make the peer choke the connection and also saturate the channel capacity (see TO_PEER_CHANNEL_CAPACITY)
// 250 is the default in libtorrent as per https://bittorrent.org/beps/bep_0010.html
const DEFAULT_MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER: usize = 2000;

// this is mostly the number of inflight (i.e. not fulfilled) requests from peers
// and downloaded blocks from peers, the latter in particular are holding the block buffers
// if we are slow on writes, these will pile up and consume memory
// for example, assuming 16kb blocks, 50000 blocks is 781MB
const PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY: usize = 50000;

// decreasing this will waste more bandwidth (needlessly requesting the same block again even if a peer sends it eventually) but will make retries for pieces requested to slow peers faster
// eventually we should tune this respect to download spped from a peer and how many outstanding requests we made
const BASE_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const ENDGAME_REQUEST_TIMEOUT: Duration = Duration::from_secs(15); // request timeout during the endgame phase: this will re-request a lot of pieces, wasting bandwidth, but will make endgame faster churning slow peers
const ENDGAME_START_AT_COMPLETION_PERCENTAGE: f64 = 98.; // start endgame when we have this percentage of the torrent
const MIN_CHOKE_TIME: Duration = Duration::from_secs(10);
const NEW_CONNECTION_COOL_OFF_PERIOD: Duration = Duration::from_secs(180); // time to wait before attempting a new connection to a non bad (i.e. with no permanent errors) peer
const ADDED_DROPPED_PEER_EVENTS_RETENTION: Duration = Duration::from_secs(90);
const PEX_MESSAGE_COOL_OFF_PERIOD: Duration = Duration::from_secs(60);
const MAX_CORRUPTION_ERRORS: u32 = 20; // max sha1 corruption errors on blocks a peer can have before marking it as bad

pub struct Peer {
    peer_addr: String,
    am_choking: bool,
    am_choking_since: SystemTime,
    am_interested: bool,
    peer_choking: bool,
    peer_choking_since: SystemTime,
    peer_interested: bool,
    haves: Option<Vec<bool>>, // this will be initialized after we have the metadata
    to_peer_tx: Sender<ToPeerMsg>,
    last_sent: SystemTime, // to understand when to send keepalived messages
    to_peer_cancel_tx: Sender<ToPeerCancelMsg>,
    outstanding_incoming_piece_block_requests: usize,
    ut_pex_id: u8,
    last_pex_message_sent: SystemTime,
    ut_metadata_id: u8,
    last_metadata_request_rejection: SystemTime,
    corruption_errors: u32,
    reqq: usize, // reqq received from peer
}

enum MetadataMessage {
    Request(u64),            // piece
    Data(u64, u64, Vec<u8>), // piece, total_size, data (16 kb or less if last piece)
    Reject(u64),             // piece
}

const METADATA_MESSAGE_REQUEST: i64 = 0;
const METADATA_MESSAGE_DATA: i64 = 1;
const METADATA_MESSAGE_REJECT: i64 = 2;

impl Peer {
    pub fn new(
        peer_addr: String,
        num_pieces: Option<usize>,
        to_peer_tx: Sender<ToPeerMsg>,
        to_peer_cancel_tx: Sender<ToPeerCancelMsg>,
    ) -> Self {
        Peer {
            peer_addr,
            am_choking: true,
            am_choking_since: SystemTime::UNIX_EPOCH,
            am_interested: false,
            peer_choking: true,
            peer_choking_since: SystemTime::UNIX_EPOCH,
            peer_interested: false,
            haves: num_pieces.map(|n| vec![false; n]),
            to_peer_tx,
            last_sent: SystemTime::now(), // initally set it to now as there is no need to send them after the handshake
            to_peer_cancel_tx,
            outstanding_incoming_piece_block_requests: 0,
            ut_pex_id: 0, // i.e. no support for pex on this peer, initially
            last_pex_message_sent: SystemTime::UNIX_EPOCH,
            ut_metadata_id: 0, // i.e. no support for metadata on this peer, initially
            last_metadata_request_rejection: SystemTime::UNIX_EPOCH,
            corruption_errors: 0, // number of corrupted block received by this peer
            reqq: DEFAULT_MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER, // the number of outstanding request messages this client supports without dropping any
        }
    }

    pub fn get_reqq(&self) -> usize {
        self.reqq
    }

    pub fn is_peer_choking(&self) -> bool {
        self.peer_choking
    }

    pub fn peer_choking_since(&self) -> SystemTime {
        self.peer_choking_since
    }

    pub fn have_piece(&self, piece_idx: usize) -> bool {
        self.haves.as_ref().map_or(false, |haves| haves[piece_idx])
    }

    async fn send(&mut self, msg: ToPeerMsg) {
        if self.to_peer_tx.capacity() <= 5 {
            log::warn!("low to_peer_tx capacity: {}", self.to_peer_tx.capacity());
        }
        self.last_sent = SystemTime::now();
        let _ = self.to_peer_tx.send(msg).await;
        // ignore errors: it can happen that the channel is closed on the other side if the rx handler loop exited due to network errors,
        // and the peer is still lingering in self.peers because the control message about the error is not yet handled
    }

    fn support_pex_extension(&self) -> bool {
        self.ut_pex_id != 0
    }

    pub fn support_metadata_extension(&self) -> bool {
        self.ut_metadata_id != 0
    }

    pub fn get_last_metadata_request_rejection(&self) -> SystemTime {
        self.last_metadata_request_rejection
    }

    async fn send_pex_extension_message(&mut self, added: Vec<String>, dropped: Vec<String>) {
        let mut h = HashMap::new();
        if added.len() > 0 {
            h.insert(
                b"added".to_vec(),
                Str(ip_port_list_to_compact_format(added)),
            );
        }
        if dropped.len() > 0 {
            h.insert(
                b"dropped".to_vec(),
                Str(ip_port_list_to_compact_format(dropped)),
            );
        }
        self.last_pex_message_sent = SystemTime::now();
        if h.len() > 0 {
            let pex_msg = Message::Extended(self.ut_pex_id, Dict(h, 0, 0), Vec::new());
            log::trace!("sending pex message to peer {}: {pex_msg}", self.peer_addr);
            self.send(ToPeerMsg::Send(pex_msg)).await;
        }
    }

    async fn send_metadata_extension_message(&mut self, metadata_message: MetadataMessage) {
        match metadata_message {
            MetadataMessage::Request(piece) => {
                let h = HashMap::from([
                    (b"msg_type".to_vec(), Int(METADATA_MESSAGE_REQUEST)),
                    (b"piece".to_vec(), Int(piece as i64)),
                ]);
                let metadata_msg =
                    Message::Extended(self.ut_metadata_id, Dict(h, 0, 0), Vec::new());
                log::trace!(
                    "sending metadata request message to peer {}: {metadata_msg}",
                    self.peer_addr
                );
                self.send(ToPeerMsg::Send(metadata_msg)).await;
            }
            MetadataMessage::Data(piece, metadata_size, data) => {
                let h = HashMap::from([
                    (b"msg_type".to_vec(), Int(METADATA_MESSAGE_DATA)),
                    (b"piece".to_vec(), Int(piece as i64)),
                    (b"total_size".to_vec(), Int(metadata_size as i64)),
                ]);
                let metadata_msg = Message::Extended(self.ut_metadata_id, Dict(h, 0, 0), data);
                log::trace!(
                    "sending metadata data message to peer {}: {metadata_msg}",
                    self.peer_addr
                );
                self.send(ToPeerMsg::Send(metadata_msg)).await;
            }
            MetadataMessage::Reject(piece) => {
                let h = HashMap::from([
                    (b"msg_type".to_vec(), Int(METADATA_MESSAGE_REJECT)),
                    (b"piece".to_vec(), Int(piece as i64)),
                ]);
                let metadata_msg =
                    Message::Extended(self.ut_metadata_id, Dict(h, 0, 0), Vec::new());
                log::trace!(
                    "sending metadata reject message to peer {}: {metadata_msg}",
                    self.peer_addr
                );
                self.send(ToPeerMsg::Send(metadata_msg)).await;
            }
        }
    }
}

pub struct TorrentManager {
    file_manager: Option<FileManager>,
    metadata_handler: MetadataHandler,
    base_path: PathBuf,
    tracker_client: Arc<Mutex<TrackerClient>>,
    last_tracker_request_time: SystemTime,
    info_hash: [u8; 20],
    own_peer_id: String,
    listening_torrent_wire_protocol_port: u16,
    peers: HashMap<PeerAddr, Peer>,
    advertised_peers: Arc<Mutex<HashMap<PeerAddr, (tracker::Peer, SystemTime)>>>, // peer addr -> (peer, last connection attempt)
    bad_peers: HashSet<PeerAddr>, // todo: remove old bad peers after a while?
    last_bandwidth_poll: SystemTime,
    uploaded_bytes: u64,
    downloaded_bytes: u64,
    uploaded_bytes_previous_poll: u64,
    downloaded_bytes_previous_poll: u64,
    piece_requestor: PieceRequestor,
    completed_sent_to_tracker: bool,
    listening_dht_port: u16,
    dht_nodes: Vec<String>,
    last_get_peers_requested_time: SystemTime,
    added_dropped_peer_events: Vec<(SystemTime, PeerAddr, PexEvent)>, // time of event, address of peer for this event, pex event. This field is used to support pex
    request_timeout: Duration,

    // internal channels, we store them here to avoid passing them around in nested calls
    to_dht_manager_tx: Sender<ToDhtManagerMsg>,
    to_dht_manager_rx: Option<Receiver<ToDhtManagerMsg>>, // optional bc we will move it to the dht manager at start, todo: should we move creation of this channel there?
    ok_to_accept_connection_tx: Sender<bool>,
    ok_to_accept_connection_rx: Option<Receiver<bool>>, // optional bc we will move it to the incoming peer handler at start, todo: should we move creation of this channel there?
    metadata_size_tx: Sender<i64>,
    metadata_size_rx: Option<Receiver<i64>>, // optional bc we will move it to the incoming peer handler at start, todo: should we move creation of this channel there?
    piece_completion_status_tx: Sender<Vec<bool>>,
    piece_completion_status_rx: Option<Receiver<Vec<bool>>>, // optional bc we will move it to the incoming peer handler at start, todo: should we move creation of this channel there?
    peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
    peers_to_torrent_manager_rx: Receiver<PeersToManagerMsg>,
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum PexEvent {
    Added,
    Dropped,
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
        // dht data
        listening_dht_port: u16,
        dht_nodes: Vec<String>,
        initial_peers: Vec<String>,
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
        let (to_dht_manager_tx, to_dht_manager_rx) = mpsc::channel(1000);
        let (ok_to_accept_connection_tx, ok_to_accept_connection_rx) = mpsc::channel(10);
        let (metadata_size_tx, metadata_size_rx) = mpsc::channel(1);
        let (piece_completion_status_tx, piece_completion_status_rx) = mpsc::channel(100);
        let (peers_to_torrent_manager_tx, peers_to_torrent_manager_rx) =
            mpsc::channel::<PeersToManagerMsg>(PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY);
        TorrentManager {
            file_manager: match files_data {
                Some((file_list, piece_length, piece_hashes)) => Some(FileManager::new(
                    base_path,
                    file_list,
                    piece_length,
                    piece_hashes,
                )),
                None => None,
            },
            metadata_handler: MetadataHandler::new(
                raw_metadata.as_ref().map(|m| m.len() as i64).or(None),
                raw_metadata,
            ),
            base_path: PathBuf::from(base_path),
            tracker_client: Arc::new(Mutex::new(TrackerClient::new(
                own_peer_id.clone(),
                announce_list,
                listening_torrent_wire_protocol_port,
            ))),
            last_tracker_request_time: SystemTime::UNIX_EPOCH,
            info_hash,
            own_peer_id: own_peer_id.clone(),
            listening_torrent_wire_protocol_port,
            peers: HashMap::new(),
            advertised_peers,
            bad_peers: HashSet::new(),
            last_bandwidth_poll: SystemTime::now(),
            uploaded_bytes: 0,
            downloaded_bytes: 0,
            uploaded_bytes_previous_poll: 0,
            downloaded_bytes_previous_poll: 0,
            piece_requestor: PieceRequestor::new(),
            completed_sent_to_tracker: false,
            listening_dht_port,
            dht_nodes,
            last_get_peers_requested_time: SystemTime::now() - DHT_NEW_PEER_COOL_OFF_PERIOD
                + DHT_BOOTSTRAP_TIME, // try to wait a bit before the first request, in hope that the dht has been bootstrapped, so that we don't waste time for the first request with an empty routing table
            added_dropped_peer_events: Vec::new(),
            request_timeout: BASE_REQUEST_TIMEOUT,

            to_dht_manager_tx,
            to_dht_manager_rx: Some(to_dht_manager_rx),
            ok_to_accept_connection_tx,
            ok_to_accept_connection_rx: Some(ok_to_accept_connection_rx),
            metadata_size_tx,
            metadata_size_rx: Some(metadata_size_rx),
            piece_completion_status_tx,
            piece_completion_status_rx: Some(piece_completion_status_rx),
            peers_to_torrent_manager_tx,
            peers_to_torrent_manager_rx,
        }
    }

    pub async fn start(&mut self) {
        // start dht manager
        let (dht_to_torrent_manager_tx, dht_to_torrent_manager_rx) = mpsc::channel(1000);
        let mut dht_manager = DhtManager::new(
            self.listening_torrent_wire_protocol_port,
            self.listening_dht_port,
            self.dht_nodes.clone(),
        );
        let to_dht_manager_rx = self
            .to_dht_manager_rx
            .take()
            .expect("no to_dht_manager_rx, has start been called twice?");
        tokio::spawn(async move {
            dht_manager
                .start(to_dht_manager_rx, dht_to_torrent_manager_tx)
                .await;
        });

        // start incoming peer connections handler
        peer::run_new_incoming_peers_handler(
            self.info_hash.clone(),
            self.own_peer_id.clone(),
            self.listening_torrent_wire_protocol_port.clone(),
            self.file_manager
                .as_ref()
                .map(|f| f.current_piece_completion_status()),
            self.ok_to_accept_connection_rx
                .take()
                .expect("no ok_to_accept_connection_rx, has start been called twice?"),
            self.metadata_size_rx
                .take()
                .expect("no metadata_size_rx, has start been called twice?"),
            self.piece_completion_status_rx
                .take()
                .expect("no piece_completion_status_rx, has start been called twice?"),
            self.peers_to_torrent_manager_tx.clone(),
            self.metadata_handler.raw_metadata_size(),
        )
        .await;

        // start ticker
        let (tick_tx, tick_rx) = mpsc::channel(1);
        start_tick(tick_tx, Duration::from_secs(1)).await;

        // start control loop to handle channel messages - will block forever
        self.control_loop(tick_rx, dht_to_torrent_manager_rx).await;
    }

    async fn control_loop(
        &mut self,
        mut tick_rx: Receiver<()>,
        mut dht_to_torrent_manager_rx: Receiver<DhtToTorrentManagerMsg>,
    ) {
        loop {
            tokio::select! {
                Some(msg) = self.peers_to_torrent_manager_rx.recv() => {
                    match msg {
                        PeersToManagerMsg::Error(peer_addr, error_type) => {
                            self.handle_peer_error(peer_addr, error_type).await;
                        }
                        PeersToManagerMsg::Receive(peer_addr, msg) => {
                            self.handle_receive_message(peer_addr, msg).await;
                        }
                        PeersToManagerMsg::NewPeer(tcp_stream) => {
                            self.handle_new_peer(tcp_stream).await;
                        }
                        PeersToManagerMsg::PieceBlockRequestFulfilled(peer_addr) => {
                            // should we maybe use a separate channel for this?
                            self.handle_piece_block_request_fulfilled(peer_addr);
                        },
                    }
                }
                Some(()) = tick_rx.recv() => {
                    self.handle_ticker().await;
                }
                Some(DhtToTorrentManagerMsg::NewPeer(ip, port)) = dht_to_torrent_manager_rx.recv() => {
                    let p = tracker::Peer{peer_id: None, ip: ip.to_string(), port};
                    let mut advertised_peers_mg = self.advertised_peers.lock().expect("another user panicked while holding the lock");
                    advertised_peers_mg.insert(format!("{ip}:{port}"), (p, SystemTime::UNIX_EPOCH));
                    drop(advertised_peers_mg);
                }
                else => break,
            }
        }
    }

    async fn handle_receive_message(&mut self, peer_addr: String, msg: Message) {
        log::trace!("received message from peer {peer_addr}: {msg}");
        let now = SystemTime::now();

        match msg {
            Message::KeepAlive => {}
            Message::Choke => self.handle_receive_choke_message(peer_addr).await,
            Message::Unchoke => {
                if let Some(peer) = self.peers.get_mut(&peer_addr) {
                    peer.peer_choking = false;
                    log::debug!("received unchoke from {peer_addr}");
                    // since we received an unchoke, we can try to send more requests immediately to this peer, without waiting for a tick
                    self.send_pieces_reqs_for_peer(peer_addr).await;
                }
            }
            Message::Interested => {
                if let Some(peer) = self.peers.get_mut(&peer_addr) {
                    peer.peer_interested = true;
                }
            }
            Message::NotInterested => {
                if let Some(peer) = self.peers.get_mut(&peer_addr) {
                    peer.peer_interested = false;
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
            Message::Piece(piece_idx, begin, data) => {
                self.handle_receive_piece_message(peer_addr, piece_idx, begin, data)
                    .await
            }
            Message::Cancel(block_request) => {
                if let Some(peer) = self.peers.get_mut(&peer_addr) {
                    // we try to let the peer message handler know about the cancellation,
                    // but if the buffer is full, we don't care, it means there were no outstanding messages to be sent
                    // and so the cancellation would have no effect
                    let _ = peer.to_peer_cancel_tx.try_send((block_request, now));
                }
            }
            Message::Port(port) => {
                // we know the peer supports DHT, send this to dht as new node
                let peer_ip_addr = peer_addr.split(":").next().expect(
                    "peer_addr, taken from tcp_stream.peer_addr(), is always of format ip:port",
                );
                let _ = self
                    .to_dht_manager_tx
                    .send(ToDhtManagerMsg::NewNode(format!("{peer_ip_addr}:{port}")))
                    .await;
            }
            Message::Extended(extension_id, extended_message, additional_data) => {
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

    async fn handle_receive_have_message(&mut self, peer_addr: String, piece_idx: u32) {
        let peer = match self.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };

        let file_manager = match &mut self.file_manager {
            Some(file_manager) => file_manager,
            None => return,
        };

        let peer_haves = match &mut peer.haves {
            Some(haves) => haves,
            None => return,
        };

        let pieces = peer_haves.len();
        if (piece_idx as usize) < pieces {
            peer_haves[piece_idx as usize] = true;

            // send interest if needed
            if !peer.am_interested && !file_manager.piece_completion_status(piece_idx as usize) {
                peer.am_interested = true;
                peer.send(ToPeerMsg::Send(Message::Interested)).await;
            }
        } else {
            log::warn!(
                "got message \"have\" {piece_idx} from peer {peer_addr} but the torrent have only {pieces} pieces"
            );
            self.bad_peers.insert(peer_addr.clone());
            self.remove_peer(peer_addr).await;
        }
    }

    async fn handle_receive_bitfield_message(&mut self, peer_addr: String, bitfield: Vec<bool>) {
        let file_manager = match &mut self.file_manager {
            Some(file_manager) => file_manager,
            None => return,
        };

        if bitfield.len() < file_manager.num_pieces() {
            log::warn!(
                "received wrongly sized bitfield from peer {peer_addr}: received {} bits but expected {}",
                bitfield.len(),
                file_manager.num_pieces()
            );
            self.bad_peers.insert(peer_addr.clone());
            self.remove_peer(peer_addr).await;
        } else if let Some(peer) = self.peers.get_mut(&peer_addr) {
            // ignore bitfield if we don't have the torrent file yet, we cannot trust the bitfield from the peer
            if peer.haves.is_none() {
                return;
            }

            // bitfield is byte aligned, it could contain more bits than pieces in the torrent
            peer.haves = Some(bitfield[0..file_manager.num_pieces()].to_vec());
            let haves = peer.haves.as_ref().expect("just added above");
            log::trace!(
                "received bitfield from peer {peer_addr}: it has {}/{} pieces",
                haves
                    .iter()
                    .fold(0, |acc, v| if *v { acc + 1 } else { acc }),
                haves.len()
            );

            // check if we need to send interest
            if !peer.am_interested {
                for piece_idx in 0..haves.len() {
                    if !file_manager.piece_completion_status(piece_idx) && haves[piece_idx] {
                        peer.am_interested = true;
                        peer.send(ToPeerMsg::Send(Message::Interested)).await;
                        break;
                    }
                }
            }
        }
        // todo: maybe re-compute assignations immediately here instead of waiting tick
    }

    async fn handle_receive_choke_message(&mut self, peer_addr: String) {
        let peer = match self.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };

        log::debug!(
            "received choke from peer {peer_addr} with {} outstanding piece block requests",
            self.piece_requestor
                .outstanding_piece_block_request_count_for_peer(&peer_addr)
        );
        peer.peer_choking = true;
        peer.peer_choking_since = SystemTime::now();
    }

    async fn handle_receive_request_message(
        &mut self,
        peer_addr: String,
        block_request: BlockRequest,
    ) {
        let file_manager = match &mut self.file_manager {
            Some(file_manager) => file_manager,
            None => return,
        };

        let peer = match self.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };

        if !peer.am_choking {
            if should_choke(
                self.peers_to_torrent_manager_tx.capacity(),
                peer.outstanding_incoming_piece_block_requests,
                true,
            ) {
                peer.send(ToPeerMsg::Send(Message::Choke)).await;
                peer.am_choking = true;
                peer.am_choking_since = SystemTime::now();
            } else {
                peer.outstanding_incoming_piece_block_requests += 1;
                match file_manager.read_piece_block(
                    block_request.piece_idx as usize,
                    block_request.block_begin as u64,
                    block_request.data_len as u64,
                ) {
                    Err(e) => {
                        log::error!("error reading block: {e}");
                        peer.outstanding_incoming_piece_block_requests -= 1;
                    }

                    Ok(data) => {
                        let data_len = data.len() as u64;
                        peer.send(ToPeerMsg::Send(Message::Piece(
                            block_request.piece_idx,
                            block_request.block_begin,
                            data,
                        )))
                        .await;

                        // todo: this is really naive, must avoid saturating upload

                        self.uploaded_bytes += data_len; // todo: we are not keeping track of cancelled pieces
                    }
                }
            }
        }
    }

    async fn handle_receive_extended_message(
        &mut self,
        peer_addr: String,
        extension_id: u8,
        extended_message: Value,
        additional_data: Vec<u8>,
    ) {
        let peer = match self.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };

        // retrieve reqq, if provided
        if let Dict(d, _, _) = extended_message.clone() {
            let client_version = if let Some(Value::Str(v)) = d.get(&(b"v".to_vec())) {
                force_string(v)
            } else {
                "unknown".to_string()
            };
            if let Some(Value::Int(reqq)) = d.get(&(b"reqq".to_vec())) {
                peer.reqq = *reqq as usize;
                log::debug!("{peer_addr} (version: {client_version}) has reqq: {reqq}");
            }
        }

        match extension_id {
            0 => {
                // this is an extension handshake
                self.handle_receive_extended_message_handshake(extended_message, peer_addr)
                    .await;
            }
            _ if extension_id == peer.ut_pex_id => {
                // this is an ut_pex extended message
                self.handle_receive_extended_message_ut_pex(extended_message, peer_addr);
            }
            _ if extension_id == peer.ut_metadata_id => {
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
        peer_addr: String,
    ) {
        let extended_message_dict = match extended_message {
            Dict(extended_message_dict, _, _) => extended_message_dict,
            _ => {
                log::debug!(
                    "got an ut_metadata extension handshake but data was not a dict, ignoring this message"
                );
                return;
            }
        };
        let m = match extended_message_dict.get(&b"m".to_vec()) {
            Some(Dict(m, _, _)) => m,
            _ => {
                log::debug!(
                    "got an ut_metadata extension handshake but \"m\" entry was not found in dict or was not a dict itself, ignoring this message"
                );
                return;
            }
        };

        let other_active_peers = self
            .peers
            .keys()
            .filter(|k| peer_addr != **k)
            .map(|k| k.clone())
            .collect::<Vec<_>>();
        let peer = match self.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };

        if let Some(Int(ut_pex_id)) = m.get(&b"ut_pex".to_vec()) {
            // this peer supports the PEX extension, registered at number ut_pex_id
            peer.ut_pex_id = *ut_pex_id as u8;
            // send first peer list
            peer.send_pex_extension_message(other_active_peers, Vec::new())
                .await;
        }
        if let Some(Int(ut_metadata_id)) = m.get(&b"ut_metadata".to_vec()) {
            // this peer supports the ut_metadata extension, registered at number ut_metadata_id
            peer.ut_metadata_id = *ut_metadata_id as u8;
            if let Some(Int(metadata_size)) = extended_message_dict.get(&b"metadata_size".to_vec())
            {
                if *metadata_size <= 0 {
                    log::debug!(
                        "got an ut_metadata extension handshake where \"metadata_size\" was <= 0, ignoring this message"
                    );
                } else if self.metadata_handler.raw_metadata_size().is_none() {
                    // we do not know the metadata size yet, take notes
                    self.metadata_handler = MetadataHandler::new(Some(*metadata_size), None);
                }
            }
        }
    }

    fn handle_receive_extended_message_ut_pex(
        &mut self,
        extended_message: Value,
        peer_addr: String,
    ) {
        let d = match extended_message {
            Dict(d, _, _) => d,
            _ => {
                log::debug!(
                    "got a PEX message from {peer_addr}, it was a bencoded value but not a dict, ignoring it"
                );
                return;
            }
        };
        // todo: should we also use the dropped list? atm we are eager to hoard all possible peers so we ignore it
        if let Some(Str(compact_contacts_info)) = d.get(&b"added".to_vec()) {
            // we don't support flags, dropped or ipv6 fields ATM
            if compact_contacts_info.len() % 6 != 0 {
                log::debug!(
                    "got a PEX message from {peer_addr} with an \"added\" field that is not divisible by 6, ignoring it"
                );
                return;
            }
            for i in (0..compact_contacts_info.len()).step_by(6) {
                let mut peer_ip_buf: [u8; 4] = [0; 4];
                peer_ip_buf.copy_from_slice(&compact_contacts_info[i..i + 4]);
                let ip = [
                    peer_ip_buf[0].to_string(),
                    peer_ip_buf[1].to_string(),
                    peer_ip_buf[2].to_string(),
                    peer_ip_buf[3].to_string(),
                ]
                .join(".");
                let mut peer_port_buf: [u8; 2] = [0; 2];
                peer_port_buf.copy_from_slice(&compact_contacts_info[i + 4..i + 6]);
                let port = u16::from_be_bytes(peer_port_buf);
                log::debug!("adding peer advertised by {peer_addr} from PEX: {ip}:{port}");
                let p = tracker::Peer {
                    peer_id: None,
                    ip: ip.clone(),
                    port,
                };
                let mut advertised_peers_mg = self
                    .advertised_peers
                    .lock()
                    .expect("another user panicked while holding the lock");
                advertised_peers_mg.insert(format!("{ip}:{port}"), (p, SystemTime::UNIX_EPOCH));
                drop(advertised_peers_mg);
            }
        }
    }

    async fn handle_receive_extended_message_ut_metadata(
        &mut self,
        value: Value,
        peer_addr: String,
        additional_data: Vec<u8>,
    ) {
        let d = match value {
            Dict(d, _, _) => d,
            _ => {
                log::debug!(
                    "got an ut_metadataextension message from {peer_addr}, it was a bencoded value but not a dict dict, ignoring it"
                );
                return;
            }
        };
        let msg_type = match d.get(&b"msg_type".to_vec()) {
            Some(Int(msg_type)) => msg_type,
            _ => {
                log::debug!(
                    "got an ut_metadataextension message from {peer_addr} but no msg_type key was found in the bencoded dict, ignoring it"
                );
                return;
            }
        };
        let piece = match d.get(&b"piece".to_vec()) {
            Some(Int(piece)) => piece,
            _ => {
                log::debug!(
                    "got an ut_metadataextension message from {peer_addr} but no piece key was found in the bencoded dict, ignoring it"
                );
                return;
            }
        };
        match *msg_type {
            METADATA_MESSAGE_REQUEST => {
                self.handle_receive_extended_message_metadata_message_request(peer_addr, *piece)
                    .await;
            }
            METADATA_MESSAGE_DATA => {
                let metadata_size = match d.get(&b"total_size".to_vec()) {
                    Some(Int(metadata_size)) => metadata_size,
                    _ => {
                        log::debug!(
                            "got a metadata message data without the required total_size field or total_size is not an integer, ignoring it"
                        );
                        return;
                    }
                };
                self.handle_receive_extended_message_metadata_message_data(
                    *metadata_size,
                    *piece,
                    additional_data,
                )
                .await;
            }
            METADATA_MESSAGE_REJECT => {
                if let Some(peer) = self.peers.get_mut(&peer_addr) {
                    (*peer).last_metadata_request_rejection = SystemTime::now();
                }
            }
            _ => {
                log::debug!(
                    "got an ut_metadataextension message from {peer_addr} but msg_type was not recognized as an extension we registered: {msg_type}"
                );
            }
        }
    }

    async fn handle_receive_extended_message_metadata_message_request(
        &mut self,
        peer_addr: String,
        piece_idx: i64,
    ) {
        let peer = match self.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };
        match self.metadata_handler.get_piece(piece_idx as usize) {
            None => {
                log::debug!(
                    "rejecting metadata message request for {piece_idx} piece from {peer_addr}: full metadata not yet known or requested pieces is out of range"
                );
                peer.send_metadata_extension_message(MetadataMessage::Reject(piece_idx as u64))
                    .await
            }
            Some((piece, raw_metadata_size)) => {
                peer.send_metadata_extension_message(MetadataMessage::Data(
                    piece_idx as u64,
                    raw_metadata_size as u64,
                    piece,
                ))
                .await
            }
        }
    }

    async fn handle_receive_extended_message_metadata_message_data(
        &mut self,
        raw_metadata_size: i64,
        piece_idx: i64,
        piece_data: Vec<u8>,
    ) {
        let raw_metadata_size = match self.metadata_handler.raw_metadata_size() {
            Some(raw_metadata_size) => raw_metadata_size,
            None => {
                // we do not know the metadata size yet, take notes
                self.metadata_handler = MetadataHandler::new(Some(raw_metadata_size), None);
                raw_metadata_size
            }
        };

        if self.file_manager.is_some() || piece_idx < 0 {
            // we are not interested in this message
            return;
        }

        self.metadata_handler
            .insert_piece(piece_idx as usize, piece_data);

        // check if metadata is complete
        if !self.metadata_handler.full_metadata_known() {
            return;
        }

        let raw_metadata = self
            .metadata_handler
            .get_raw_metadata()
            .as_ref()
            .expect("it must exist, full metadata is known");

        // check hash
        let info_hash: [u8; 20] = Sha1::digest(raw_metadata).into();
        if info_hash != self.info_hash {
            self.corrupted_metadata(Error::msg("hash mismatch"));
            return;
        }

        let info_dict = match Value::new(raw_metadata) {
            Dict(info_dict, _, _) => info_dict,
            _ => {
                self.corrupted_metadata(Error::msg("not a bencoded dict"));
                return;
            }
        };

        match infodict::get_infodict(&info_dict) {
            Ok((piece_length, piece_hashes, m)) => {
                log::warn!(
                    "metadata download completed, we can now start downloading the actual torrent data..."
                );
                self.file_manager = Some(FileManager::new(
                    self.base_path.as_path(),
                    get_files(&m),
                    piece_length,
                    piece_hashes,
                ));
                // we finally have the metadata and can exchange files
                // we discarded have messages (we could not save them because we could not know how many pieces there were in total)
                // and, most importantly, bitfield messages from peers till now, so, let's disconnect from the current peers:
                // the reconnection will trigger the necessary bitfield message that we need to request pieces
                // let's also ignore the last connection attempt for connected peers
                log::warn!(
                    "resetting current connected peers to retrieve which block each peer has..."
                );
                let mut advertised_peers_mg = self
                    .advertised_peers
                    .lock()
                    .expect("another user panicked while holding the lock");
                for (peer_addr, _) in self.peers.iter() {
                    if let Some((advertised_peer, _)) = advertised_peers_mg.remove(peer_addr) {
                        advertised_peers_mg
                            .insert(peer_addr.clone(), (advertised_peer, SystemTime::UNIX_EPOCH));
                    }
                }
                drop(advertised_peers_mg);
                self.peers = HashMap::new();
                self.metadata_size_tx
                    .send(raw_metadata_size)
                    .await
                    .expect("metadata_size_tx receiver half closed");
            }
            Err(e) => {
                self.corrupted_metadata(e);
            }
        }
    }

    async fn handle_receive_piece_message(
        &mut self,
        peer_addr: String,
        piece_idx: u32,
        begin: u32,
        data: Vec<u8>,
    ) {
        if self.file_manager.is_none() {
            return;
        }

        let data_len = data.len() as u64;
        match self
            .file_manager
            .as_mut()
            .expect("invariant checked above")
            .write_piece_block(piece_idx as usize, data, begin as u64)
        {
            Ok(piece_completed) => {
                self.downloaded_bytes += data_len;

                self.piece_requestor.block_request_completed(
                    &peer_addr,
                    &BlockRequest {
                        piece_idx,
                        block_begin: begin,
                        data_len: data_len as u32,
                    },
                );
                if piece_completed {
                    self.piece_requestor
                        .piece_request_completed(&peer_addr, piece_idx as usize);

                    if !self.completed_sent_to_tracker
                        && self
                            .file_manager
                            .as_mut()
                            .expect("invariant checked above")
                            .completed()
                    {
                        log::warn!("torrent download completed");
                        self.completed_sent_to_tracker = true;
                        self.async_request_to_tracker(Event::Completed).await;
                    }

                    // ignore errors here: it can happen that the channel is closed on the other side if the rx handler loop exited
                    // due to network errors and the peer is still lingering in self.peers because the control message about the error is not yet been handled
                    let _ = self
                        .piece_completion_status_tx
                        .send(
                            self.file_manager
                                .as_mut()
                                .expect("invariant checked above")
                                .current_piece_completion_status(),
                        )
                        .await;

                    for (_, peer) in self.peers.iter_mut() {
                        if peer.haves.is_none() {
                            continue;
                        }
                        // send "have" to interested peers
                        if peer.peer_interested
                            && !peer
                                .haves
                                .as_ref()
                                .expect("haves is initialized if file_manager is")
                                [piece_idx as usize]
                        {
                            peer.send(ToPeerMsg::Send(Message::Have(piece_idx))).await;
                        }
                        // send "not interested" if needed
                        if peer.am_interested {
                            let mut am_still_interested = false;
                            for i in 0..peer
                                .haves
                                .as_ref()
                                .expect("haves is initialized if file_manager is")
                                .len()
                            {
                                if !self
                                    .file_manager
                                    .as_mut()
                                    .expect("invariant checked above")
                                    .piece_completion_status(piece_idx as usize)
                                    && peer
                                        .haves
                                        .as_ref()
                                        .expect("haves is initialized if file_manager is")[i]
                                {
                                    am_still_interested = true;
                                    break;
                                }
                            }
                            if !am_still_interested {
                                peer.send(ToPeerMsg::Send(Message::NotInterested)).await;
                            }
                        }
                    }
                }
                // since we received a piece block, we can try to send more requests immediately to this peer, without waiting for a tick
                self.send_pieces_reqs_for_peer(peer_addr).await;
            }
            Err(e) => {
                log::error!("cannot write block received from {peer_addr}: {e}");

                // keep track of corruptions, remove if too many
                if let Some(_) = e.downcast_ref::<ShaCorruptedError>() {
                    let peer = match self.peers.get_mut(&peer_addr) {
                        Some(peer) => peer,
                        None => return,
                    };
                    peer.corruption_errors += 1;
                    if peer.corruption_errors > MAX_CORRUPTION_ERRORS {
                        log::warn!(
                            "removing peer {peer_addr} due to too many corrupted pieces received"
                        );
                        peer.send(ToPeerMsg::Disconnect()).await;
                        self.remove_peer(peer_addr).await;
                    }
                }
            }
        }
    }

    async fn send_pieces_reqs_for_peer(&mut self, peer_addr: String) {
        let file_manager = match &self.file_manager {
            Some(file_manager) => file_manager,
            None => return,
        };

        let peer = match self.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };

        // compute requests from piece requestor
        let reqs_to_send = self.piece_requestor.generate_requests_to_send_for_peer(
            &peer_addr,
            &peer,
            file_manager,
        );

        // finally send requests
        for block_request in reqs_to_send {
            peer.send(ToPeerMsg::Send(Message::Request(block_request)))
                .await;
        }
    }

    async fn handle_peer_error(&mut self, peer_addr: String, error_type: PeerError) {
        log::debug!("removing errored peer {peer_addr}");
        if error_type == PeerError::HandshakeError {
            // todo: understand other error cases that are not recoverable and should stop trying again on this peer
            self.bad_peers.insert(peer_addr.clone());
        }
        self.remove_peer(peer_addr).await;
    }

    async fn remove_peer(&mut self, peer_addr: String) {
        self.added_dropped_peer_events.push((
            SystemTime::now(),
            peer_addr.clone(),
            PexEvent::Dropped,
        ));
        if let Some(_) = self.peers.remove(&peer_addr) {
            self.piece_requestor.remove_assigments_to_peer(&peer_addr);
        }
        if self.peers.len() < CONNECTED_PEERS_TO_STOP_INCOMING_PEER_CONNECTIONS {
            self.ok_to_accept_connection_tx
                .send(true)
                .await
                .expect("ok_to_accept_connection_tx receiver half closed");
        }
    }

    fn handle_piece_block_request_fulfilled(&mut self, peer_addr: String) {
        if let Some(peer) = self.peers.get_mut(&peer_addr)
            && peer.outstanding_incoming_piece_block_requests > 0
        {
            peer.outstanding_incoming_piece_block_requests -= 1;
        }
    }

    async fn handle_ticker(&mut self) {
        self.log_stats(self.peers_to_torrent_manager_tx.capacity());
        self.connect_to_new_peers().await;
        self.send_keep_alives().await;
        self.send_status_to_tracker().await;
        self.unchoke_peers().await;
        self.check_endgame_status().await;
        self.request_new_peers_to_dht_manager().await;
        self.send_pex_messages().await;
        self.send_metadata_reqs().await;
        self.send_pieces_reqs().await;
    }

    async fn connect_to_new_peers(&mut self) {
        let current_peers_n = self.peers.len();
        if current_peers_n < CONNECTED_PEERS_TO_START_NEW_PEER_CONNECTIONS {
            let possible_peers_mg = self
                .advertised_peers
                .lock()
                .expect("another user panicked while holding the lock");
            let possible_peers = possible_peers_mg.clone();
            let now = SystemTime::now();
            drop(possible_peers_mg);
            let possible_peers = possible_peers
                .iter()
                .filter(|(k, (_, last_connection_attempt))| {
                    // avoid selecting peers we are already connected to
                    !self.peers.contains_key(*k)
                    // avoid selecting peers we know are bad
                    && !self.bad_peers.contains(*k)
                    // use peers we didn't try to connect to recently
                    // this cool-off time is also important to avoid new connections to peers we attempted few secs ago
                    // and for which a connection attempt is still inflight
                    && now.duration_since(*last_connection_attempt).unwrap_or_default() > NEW_CONNECTION_COOL_OFF_PERIOD
                })
                .collect::<Vec<_>>();

            let candidates_for_new_connections: Vec<_> = possible_peers
                .choose_multiple(
                    &mut rand::rng(),
                    CONNECTED_PEERS_TO_START_NEW_PEER_CONNECTIONS - current_peers_n,
                )
                .collect();
            // todo: better algorithm to select new peers
            for (_, (peer, _)) in candidates_for_new_connections.iter() {
                tokio::spawn(peer::connect_to_new_peer(
                    peer.ip.clone(),
                    peer.port,
                    self.info_hash,
                    self.own_peer_id.clone(),
                    self.listening_torrent_wire_protocol_port,
                    self.file_manager
                        .as_ref()
                        .map(|f| f.current_piece_completion_status()),
                    self.metadata_handler.raw_metadata_size(),
                    self.peers_to_torrent_manager_tx.clone(),
                ));
            }
            // update last connection attempt
            let mut possible_peers_mg = self
                .advertised_peers
                .lock()
                .expect("another user panicked while holding the lock");
            for (peer_addr, _) in candidates_for_new_connections.iter() {
                let possible_peer_entry = possible_peers_mg
                    .get_mut(*peer_addr)
                    .expect("we filtered on this above");
                possible_peer_entry.1 = now;
            }
            drop(possible_peers_mg);
        }
    }

    async fn send_keep_alives(&mut self) {
        let now = SystemTime::now();
        for (_, peer) in self.peers.iter_mut() {
            if let Ok(elapsed) = now.duration_since(peer.last_sent) {
                if elapsed > KEEP_ALIVE_FREQ {
                    peer.send(ToPeerMsg::Send(Message::KeepAlive)).await;
                }
            }
        }
    }

    async fn send_status_to_tracker(&mut self) {
        let now = SystemTime::now();
        let tracker_client_mg = self
            .tracker_client
            .lock()
            .expect("another user panicked while holding the lock");
        let tracker_request_interval = tracker_client_mg.tracker_request_interval;
        drop(tracker_client_mg);
        if let Ok(elapsed) = now.duration_since(self.last_tracker_request_time) {
            if elapsed > tracker_request_interval {
                let event = if self.last_tracker_request_time == SystemTime::UNIX_EPOCH {
                    Event::Started
                } else {
                    Event::None
                };
                self.async_request_to_tracker(event).await;
            }
        }
    }

    async fn unchoke_peers(&mut self) {
        let now = SystemTime::now();
        for (_, peer) in self.peers.iter_mut() {
            if peer.am_choking
                && now
                    .duration_since(peer.am_choking_since)
                    .unwrap_or_default()
                    > MIN_CHOKE_TIME
                && !should_choke(
                    self.peers_to_torrent_manager_tx.capacity(),
                    peer.outstanding_incoming_piece_block_requests,
                    self.file_manager.is_some(),
                )
            {
                peer.am_choking = false;
                peer.send(ToPeerMsg::Send(Message::Unchoke)).await;
            }
        }
    }

    async fn check_endgame_status(&mut self) {
        // check endgame status a decrease request timeout if needed
        if let Some(file_manager) = &self.file_manager {
            if self.request_timeout != ENDGAME_REQUEST_TIMEOUT {
                let completed_pieces = file_manager.completed_pieces();
                let total_pieces = file_manager.num_pieces();
                if (completed_pieces as f64) / (total_pieces as f64) * 100.
                    > ENDGAME_START_AT_COMPLETION_PERCENTAGE
                {
                    log::warn!("entering endgame phase");
                    self.request_timeout = ENDGAME_REQUEST_TIMEOUT;
                }
            }
        }
    }

    async fn request_new_peers_to_dht_manager(&mut self) {
        let now = SystemTime::now();
        if self.peers.len() < MAX_CONNECTED_PEERS_TO_ASK_DHT_FOR_MORE
            && now
                .duration_since(self.last_get_peers_requested_time)
                .unwrap_or_default()
                > DHT_NEW_PEER_COOL_OFF_PERIOD
        {
            self.last_get_peers_requested_time = now;
            let _ = self
                .to_dht_manager_tx
                .send(ToDhtManagerMsg::GetNewPeers(self.info_hash))
                .await;
        }
    }

    async fn send_pex_messages(&mut self) {
        // remove old added / dropped events
        let now = SystemTime::now();
        self.added_dropped_peer_events
            .retain(|(event_timestamp, _, _)| {
                now.duration_since(*event_timestamp).unwrap_or_default()
                    < ADDED_DROPPED_PEER_EVENTS_RETENTION
            });

        for peer in self.peers.values_mut().filter(|p| {
            p.support_pex_extension()
                && now
                    .duration_since(p.last_pex_message_sent)
                    .unwrap_or_default()
                    > PEX_MESSAGE_COOL_OFF_PERIOD
        }) {
            let elided_events = self
                .added_dropped_peer_events
                .iter()
                .filter(|(event_timestamp, _, _)| *event_timestamp > peer.last_pex_message_sent)
                .fold(HashMap::new(), |mut map, (_, addr, event_type)| {
                    map.insert(addr.clone(), *event_type);
                    map
                });
            let added = elided_events
                .iter()
                .filter(|(_, event_type)| **event_type == PexEvent::Added)
                .map(|(p, _)| (*p).clone())
                .collect();
            let dropped = elided_events
                .iter()
                .filter(|(_, event_type)| **event_type == PexEvent::Dropped)
                .map(|(p, _)| (*p).clone())
                .collect();
            peer.send_pex_extension_message(added, dropped).await;
        }
    }

    async fn send_metadata_reqs(&mut self) {
        if self.file_manager.is_some() {
            return;
        }
        // we still have to download the metadata, ask metadata pieces to peers
        let new_medatada_piece_requests = self
            .metadata_handler
            .generate_metadata_piece_reqs(&self.peers);
        for (peer_addr, piece_to_request) in new_medatada_piece_requests {
            if let Some(peer) = self.peers.get_mut(&peer_addr) {
                log::debug!("sending metadata piece request to {peer_addr}: {piece_to_request}");
                peer.send_metadata_extension_message(MetadataMessage::Request(
                    piece_to_request as u64,
                ))
                .await;
            }
        }
    }

    async fn send_pieces_reqs(&mut self) {
        let file_manager = match &self.file_manager {
            Some(file_manager) => file_manager,
            None => return,
        };

        // remove requests to choked peers that have been lingering for some time
        // or requests that have not been fulfilled for some time, even if unchoked,
        // most probably they have been silently dropped by the peer even if it is still alive
        let expired_piece_blocks_requests = self
            .piece_requestor
            .remove_stale_requests(self.request_timeout, &self.peers);
        for (peer_addr, req) in expired_piece_blocks_requests {
            if let Some(peer) = self.peers.get_mut(&peer_addr) {
                peer.send(ToPeerMsg::Send(Message::Cancel(req))).await;
            }
        }

        // compute requests from piece requestor
        let reqs_to_send = self
            .piece_requestor
            .generate_requests_to_send(&self.peers, file_manager);

        // finally send requests
        for (peer_addr, block_requests) in reqs_to_send {
            if let Some(peer) = self.peers.get_mut(&peer_addr) {
                for block_request in block_requests {
                    peer.send(ToPeerMsg::Send(Message::Request(block_request)))
                        .await;
                }
            } else {
                log::warn!(
                    "could not find peer for which piece_requestor assigned requests, this should never happen"
                );
                self.piece_requestor.remove_assigments_to_peer(&peer_addr);
            }
        }
    }

    async fn async_request_to_tracker(&mut self, event: Event) {
        self.last_tracker_request_time = SystemTime::now();
        let bytes_left = self.file_manager.as_ref().map(|f| f.bytes_left());
        let info_hash = self.info_hash;
        let uploaded_bytes = self.uploaded_bytes;
        let downloaded_bytes = self.downloaded_bytes;
        let advertised_peers = self.advertised_peers.clone();
        let tracker_client_mg = self
            .tracker_client
            .lock()
            .expect("another user panicked while holding the lock");
        let tracker_client = tracker_client_mg.clone();
        drop(tracker_client_mg);
        let tracker_client_arc = self.tracker_client.clone();
        tokio::spawn(async move {
            if let Ok((updated_tracker_client, latest_advertised_peers)) = request_to_tracker(
                tracker_client,
                event,
                bytes_left,
                info_hash,
                uploaded_bytes,
                downloaded_bytes,
            )
            .await
            {
                update_tracker_client_and_advertised_peers(
                    tracker_client_arc,
                    advertised_peers,
                    updated_tracker_client,
                    latest_advertised_peers,
                );
            }
        });
    }

    async fn handle_new_peer(&mut self, tcp_stream: TcpStream) {
        let peer_addr = match tcp_stream.peer_addr() {
            Ok(s) => {
                // send to dht manager the fact that we know a new good peer
                let peer_port = s.port();
                if let IpAddr::V4(peer_addr) = s.ip() {
                    self.to_dht_manager_tx
                        .send(ToDhtManagerMsg::ConnectedToNewPeer(
                            self.info_hash,
                            peer_addr,
                            peer_port,
                        ))
                        .await
                        .expect("to_dht_manager_tx receiver half closed");
                }
                s.to_string()
            }
            Err(e) => {
                log::trace!(
                    "new peer initialization failed because we could not get peer_addr: {e}"
                );
                return;
            }
        };
        let (to_peer_tx, to_peer_rx) = mpsc::channel(TO_PEER_CHANNEL_CAPACITY);
        let (to_peer_cancel_tx, to_peer_cancel_rx) = mpsc::channel(TO_PEER_CANCEL_CHANNEL_CAPACITY);
        peer::start_peer_msg_handlers(
            peer_addr.clone(),
            tcp_stream,
            self.peers_to_torrent_manager_tx.clone(),
            to_peer_rx,
            to_peer_cancel_rx,
        );
        self.peers.insert(
            peer_addr.clone(),
            Peer::new(
                peer_addr.clone(),
                self.file_manager.as_ref().map(|f| f.num_pieces()),
                to_peer_tx,
                to_peer_cancel_tx,
            ),
        );
        log::debug!("new peer initialized: {peer_addr}");
        self.added_dropped_peer_events
            .push((SystemTime::now(), peer_addr, PexEvent::Added));
        if self.peers.len() > CONNECTED_PEERS_TO_STOP_INCOMING_PEER_CONNECTIONS {
            log::trace!("stop accepting new peers");
            self.ok_to_accept_connection_tx
                .send(false)
                .await
                .expect("ok_to_accept_connection_tx receiver half closed");
        }
    }

    fn log_stats(&mut self, peers_to_torrent_manager_channel_capacity: usize) {
        let advertised_peers_lock = self
            .advertised_peers
            .lock()
            .expect("another user panicked while holding the lock");
        let advertised_peers_len = advertised_peers_lock.len();
        drop(advertised_peers_lock);
        let now = SystemTime::now();
        let elapsed_s = now
            .duration_since(self.last_bandwidth_poll)
            .unwrap_or_default()
            .as_millis() as f64
            / 1000f64;
        let bandwidth_up =
            (self.uploaded_bytes - self.uploaded_bytes_previous_poll) as f64 / elapsed_s;
        let bandwidth_down =
            (self.downloaded_bytes - self.downloaded_bytes_previous_poll) as f64 / elapsed_s;
        self.uploaded_bytes_previous_poll = self.uploaded_bytes;
        self.downloaded_bytes_previous_poll = self.downloaded_bytes;
        self.last_bandwidth_poll = now;
        log::info!(
            "left: {left}, pieces: {completed_pieces}/{total_pieces} metadata pieces: {known_metadata_pieces}/{total_metadata_pieces} | Up: {up_band}/s, Down: {down_band}/s (tot.: {tot_up}, {tot_down}), wasted: {wasted} | known peers from advertising: {known_peers} (bad: {bad_peers}), connected: {connected_peers}, unchoked: {unchoked_peers} | pending peers_to_torrent_manager msgs: {cur_ch_cap}/{tot_ch_cap}",
            left = self
                .file_manager
                .as_ref()
                .map(|f| Size::from_bytes(f.bytes_left()).to_string())
                .unwrap_or("?".to_string()),
            completed_pieces = self
                .file_manager
                .as_ref()
                .map(|f| f.completed_pieces())
                .unwrap_or(0),
            total_pieces = self
                .file_manager
                .as_ref()
                .map(|f| f.num_pieces().to_string())
                .unwrap_or("?".to_string()),
            up_band = Size::from_bytes(bandwidth_up)
                .format()
                .with_style(Style::Abbreviated),
            down_band = Size::from_bytes(bandwidth_down)
                .format()
                .with_style(Style::Abbreviated),
            tot_up = Size::from_bytes(self.uploaded_bytes)
                .format()
                .with_style(Style::Abbreviated),
            tot_down = Size::from_bytes(self.downloaded_bytes)
                .format()
                .with_style(Style::Abbreviated),
            wasted = Size::from_bytes(
                self.file_manager
                    .as_ref()
                    .map(|f| f.wasted_bytes())
                    .unwrap_or(0)
            )
            .format()
            .with_style(Style::Abbreviated),
            known_peers = advertised_peers_len,
            bad_peers = self.bad_peers.len(),
            connected_peers = self.peers.len(),
            unchoked_peers = self.peers.iter().fold(0, |acc, (_, p)| if !p.peer_choking {
                acc + 1
            } else {
                acc
            }),
            cur_ch_cap = PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY
                - peers_to_torrent_manager_channel_capacity,
            known_metadata_pieces = self.metadata_handler.total_metadata_pieces_downloaded(),
            total_metadata_pieces = match self.metadata_handler.total_metadata_pieces() {
                0 => "?".to_string(),
                c => c.to_string(),
            },
            tot_ch_cap = PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY,
        );
    }

    fn corrupted_metadata(&mut self, error: Error) {
        log::warn!(
            "downloaded metadata is corrupted ({}), starting over its download...",
            error
        );
        self.metadata_handler = MetadataHandler::new(None, None);
    }
}

fn generate_peer_id() -> String {
    const CHARSET: &[u8] = b"0123456789";
    let mut rng = rand::rng();
    let one_char = || CHARSET[rng.random_range(0..CHARSET.len())] as char;
    let random_string: String = iter::repeat_with(one_char).take(12).collect();
    format!("-YT0001-{random_string}")
}

async fn request_to_tracker(
    mut tracker_client: TrackerClient,
    event: Event,
    bytes_left: Option<u64>,
    info_hash: [u8; 20],
    uploaded_bytes: u64,
    downloaded_bytes: u64,
) -> Result<(TrackerClient, Vec<tracker::Peer>)> {
    match tracker_client
        .request(
            info_hash,
            uploaded_bytes,
            downloaded_bytes,
            bytes_left,
            event,
        )
        .await
    {
        Err(e) => {
            match e.downcast_ref() {
                Some(NoTrackerError) => log::info!("could not perform request to tracker: {e}"),
                _ => log::error!("could not perform request to tracker: {e}"),
            }
            bail!(e);
        }
        Ok(Response::Failure(msg)) => {
            log::error!("tracker responded with failure: {msg}");
            bail!(msg);
        }
        Ok(Response::Ok(ok_response)) => {
            if let Some(msg) = ok_response.warning_message.clone() {
                log::error!("tracker sent a warning: {msg}");
            }
            log::info!(
                "tracker request succeeded: seeders: {}; leechers: {}; peers provided: {}",
                ok_response.complete,
                ok_response.incomplete,
                ok_response.peers.len()
            );
            log::trace!("full tracker response:\n{ok_response:?}");

            Ok((tracker_client, ok_response.peers))
        }
    }
}

fn update_tracker_client_and_advertised_peers(
    tracker_client: Arc<Mutex<TrackerClient>>,
    advertised_peers: Arc<Mutex<HashMap<PeerAddr, (tracker::Peer, SystemTime)>>>,
    updated_tracker_client: TrackerClient,
    latest_advertised_peers: Vec<tracker::Peer>,
) {
    let mut tracker_client_mg = tracker_client
        .lock()
        .expect("another user panicked while holding the lock");
    *tracker_client_mg = updated_tracker_client;
    drop(tracker_client_mg);
    let mut advertised_peers = advertised_peers
        .lock()
        .expect("another user panicked while holding the lock");
    latest_advertised_peers.iter().for_each(|p| {
        advertised_peers.insert(
            format!("{}:{}", p.ip, p.port),
            (p.clone(), SystemTime::UNIX_EPOCH),
        );
    });
    drop(advertised_peers);
}

fn should_choke(
    peers_to_torrent_manager_channel_capacity: usize,
    outstanding_incoming_piece_block_requests_for_this_peer: usize,
    file_manager_initialized: bool,
) -> bool {
    peers_to_torrent_manager_channel_capacity < PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY / 2
        || !file_manager_initialized
        || outstanding_incoming_piece_block_requests_for_this_peer
            > MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER as usize
}

fn ip_port_list_to_compact_format(addrs: Vec<String>) -> Vec<u8> {
    let mut compact_format: Vec<u8> = Vec::new();
    for addr in addrs {
        let ip_port: Vec<_> = addr.split(':').collect();
        if ip_port.len() != 2 {
            panic!("addr string was not of the format ip:port");
        }
        let ipv4_addr: Ipv4Addr = ip_port[0].parse().expect("addr was not an ipv4");
        compact_format.append(&mut ipv4_addr.octets().to_vec());
        let port: u16 = ip_port[1].parse().expect("port was not a u16");
        compact_format.append(&mut port.to_be_bytes().to_vec());
    }
    compact_format
}

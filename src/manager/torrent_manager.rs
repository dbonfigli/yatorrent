use anyhow::{Error, Result, bail};
use sha1::{Digest, Sha1};
use std::cmp::{Ordering, min};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{iter, path::Path};

use rand::Rng;
use rand::seq::{IndexedRandom, SliceRandom};
use size::{Size, Style};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::dht::dht_manager::{DhtManager, DhtToTorrentManagerMsg, ToDhtManagerMsg};
use crate::manager::peer::{self, PeerAddr, PeersToManagerMsg, ToPeerCancelMsg, ToPeerMsg};
use crate::metadata::infodict::{self};
use crate::metadata::metainfo::get_files;
use crate::persistence::file_manager::ShaCorruptedError;
use crate::persistence::piece::Piece;
use crate::torrent_protocol::wire_protocol::Message;
use crate::tracker;
use crate::util::start_tick;
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
const MAX_OUTSTANDING_REQUESTS_PER_PEER: usize = 500; // can be retrieved per peer if it supports extensions, dict key "reqq", seen: deluge: 2000, qbittorrent: 500, transmission: 500, utorrent: 255, freebox bittorrent 2: 768, maybe variable. This parameter is extremelly important: a too low value will waste bandwidth in case a peer is really fast, a too high value will make the peer choke the connection and also saturate the channel capacity (see TO_PEER_CHANNEL_CAPACITY)
const MAX_OUTSTANDING_PIECES: usize = 2000;
const BLOCK_SIZE_B: u64 = 16384;
const METADATA_BLOCK_SIZE_B: usize = 16384;
const TO_PEER_CHANNEL_CAPACITY: usize = 2000;
const TO_PEER_CANCEL_CHANNEL_CAPACITY: usize = 1000;
const PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY: usize = 50000;
const BASE_REQUEST_TIMEOUT: Duration = Duration::from_secs(120); // decreasing this will waste more bandwidth (needlessly requesting the same block again even if a peer sends it eventually) but will make retries for pieces requested to slow peers faster
const ENDGAME_REQUEST_TIMEOUT: Duration = Duration::from_secs(15); // request timeout during the endgame phase: this will re-request a lot of pieces, wasting bandwidth, but will make endgame faster churning slow peers
const ENDGAME_START_AT_COMPLETION_PERCENTAGE: f64 = 98.; // start endgame when we have this percentage of the torrent
const MIN_CHOKE_TIME: Duration = Duration::from_secs(60);
const NEW_CONNECTION_COOL_OFF_PERIOD: Duration = Duration::from_secs(180); // time to wait before attempting a new connection to a non bad (i.e. with no permanent errors) peer
const ADDED_DROPPED_PEER_EVENTS_RETENTION: Duration = Duration::from_secs(90);
const PEX_MESSAGE_COOL_OFF_PERIOD: Duration = Duration::from_secs(60);
const METADATA_BLOCK_REQUEST_TIMEOUT: Duration = Duration::from_secs(15); // timeout for waiting a requested metadata block
const MAX_OUTSTANDING_BLOCK_REQUESTS_PER_PEER: i64 = 100;
const PEER_METADATA_REQUEST_REJECTION_COOL_OFF_PERIOD: Duration = Duration::from_secs(30);
const MAX_CORRUPTION_ERRORS: u32 = 20; // max sha1 corruption errors on blocks a peer can have before marking it as bad

pub struct Peer {
    peer_addr: String,
    am_choking: bool,
    am_choking_since: SystemTime,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    haves: Option<Vec<bool>>, // this will be initialized after we have the metadata
    to_peer_tx: Sender<ToPeerMsg>,
    last_sent: SystemTime, // to understand when to send keepalived messages
    to_peer_cancel_tx: Sender<ToPeerCancelMsg>,
    outstanding_block_requests: HashMap<(u32, u32, u32), SystemTime>, // (piece idx, block begin, data len) -> request time
    requested_pieces: HashMap<usize, Piece>, // piece idx -> piece status with all the requested fragments
    ut_pex_id: u8,
    last_pex_message_sent: SystemTime,
    ut_metadata_id: u8,
    last_metadata_request_rejection: SystemTime,
    corruption_errors: u32,
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
            peer_interested: false,
            haves: num_pieces.map(|n| vec![false; n]),
            to_peer_tx,
            last_sent: SystemTime::now(), // initally set it to now as there is no need to send them after the handshake
            to_peer_cancel_tx,
            outstanding_block_requests: HashMap::new(),
            requested_pieces: HashMap::new(),
            ut_pex_id: 0, // i.e. no support for pex on this peer, initially
            last_pex_message_sent: SystemTime::UNIX_EPOCH,
            ut_metadata_id: 0, // i.e. no support for metadata on this peer, initially
            last_metadata_request_rejection: SystemTime::UNIX_EPOCH,
            corruption_errors: 0, // number of corrupted block received by this peer
        }
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

    fn support_metadata_extension(&self) -> bool {
        self.ut_metadata_id != 0
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

    async fn send_requests_for_piece(&mut self, piece_idx: usize, mut incomplete_piece: Piece) {
        while self.outstanding_block_requests.len() < MAX_OUTSTANDING_REQUESTS_PER_PEER {
            if let Some((begin, end)) = incomplete_piece.get_next_fragment(BLOCK_SIZE_B) {
                let request = (piece_idx as u32, begin as u32, (end - begin + 1) as u32);
                self.send(ToPeerMsg::Send(Message::Request(
                    request.0, request.1, request.2,
                )))
                .await;

                self.outstanding_block_requests
                    .insert(request, SystemTime::now());
                incomplete_piece.add_fragment(begin, end);
            } else {
                break;
            }
        }

        self.requested_pieces
            .insert(piece_idx, incomplete_piece.clone());
    }
}

pub struct TorrentManager {
    file_manager: Option<FileManager>,
    raw_metadata: Option<Vec<u8>>,
    raw_metadata_size: Option<i64>,
    downloaded_metadata_blocks: Vec<(bool, PeerAddr, SystemTime)>, // downloaded, peer addr we requested block to, request time
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
    outstanding_piece_assignments: HashMap<usize, String>, // piece idx -> peer_addr
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
        let raw_metadata_size = raw_metadata.as_ref().map(|m| m.len() as i64).or(None);
        let downloaded_metadata_blocks = match raw_metadata_size {
            Some(size) => metadata_blocks_from_size(size, true),
            None => Vec::<(bool, PeerAddr, SystemTime)>::new(),
        };
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
            raw_metadata,
            raw_metadata_size,
            downloaded_metadata_blocks,
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
            outstanding_piece_assignments: HashMap::new(),
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
                .map(|f| f.piece_completion_status.clone()),
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
            self.raw_metadata_size,
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
                    // todo: maybe re-compute assignations immediately here instead of waiting tick
                    // this is especially important in case of really fast peers, where outstanding
                    // requests are handled within a single tick, so for the rest of the tick such peers are idle
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
            Message::Request(piece_idx, begin, length) => {
                self.handle_receive_request_message(peer_addr, piece_idx, begin, length)
                    .await
            }
            Message::Piece(piece_idx, begin, data) => {
                self.handle_receive_piece_message(peer_addr, piece_idx, begin, data)
                    .await
            }
            Message::Cancel(piece_idx, begin, length) => {
                if let Some(peer) = self.peers.get_mut(&peer_addr) {
                    // we try to let the peer message handler know about the cancellation,
                    // but if the buffer is full, we don't care, it means there were no outstanding messages to be sent
                    // and so the cancellation would have no effect
                    let _ = peer
                        .to_peer_cancel_tx
                        .try_send((piece_idx, begin, length, now));
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
            if !peer.am_interested && !file_manager.piece_completion_status[piece_idx as usize] {
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
                    if !file_manager.piece_completion_status[piece_idx] && haves[piece_idx] {
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
            "received choked from peer {peer_addr} with {} outstanding requests",
            peer.outstanding_block_requests.len()
        );
        peer.peer_choking = true;

        // remove outstanding requests that will be discarded because the peer is choking
        for (piece_idx, _) in peer.requested_pieces.iter() {
            self.outstanding_piece_assignments.remove(&(*piece_idx));
        }
        peer.outstanding_block_requests = HashMap::new();
        peer.requested_pieces = HashMap::new();

        // todo: maybe re-compute assignations immediately here instead of waiting tick
    }

    async fn handle_receive_request_message(
        &mut self,
        peer_addr: String,
        piece_idx: u32,
        begin: u32,
        length: u32,
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
            if should_choke(self.peers_to_torrent_manager_tx.capacity(), true) {
                peer.send(ToPeerMsg::Send(Message::Choke)).await;
                peer.am_choking = true;
                peer.am_choking_since = SystemTime::now();
            } else {
                match file_manager.read_piece_block(piece_idx as usize, begin as u64, length as u64)
                {
                    Err(e) => {
                        log::error!("error reading block: {e}");
                    }

                    Ok(data) => {
                        let data_len = data.len() as u64;
                        peer.send(ToPeerMsg::Send(Message::Piece(piece_idx, begin, data)))
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
        let peer = match self.peers.get(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };
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
                    "got an extension message from {peer_addr} but id was not recognized as an extension we registered: {extension_id}"
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
                } else if self.raw_metadata_size.is_none() {
                    // we do not know the metadata size yet, take notes
                    self.raw_metadata_size = Some(*metadata_size);
                    self.downloaded_metadata_blocks =
                        metadata_blocks_from_size(*metadata_size, false);
                    self.raw_metadata = Some(vec![0; *metadata_size as usize]);
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
        piece: i64,
    ) {
        match (
            &mut self.file_manager,
            self.raw_metadata_size,
            &mut self.raw_metadata,
        ) {
            // if file_manager is initialized then raw_metadata_size is also completely known
            (Some(_), Some(raw_metadata_size), Some(raw_metadata)) => {
                let block_start = piece as usize * METADATA_BLOCK_SIZE_B;
                let block_end = min(
                    raw_metadata_size as usize,
                    block_start + METADATA_BLOCK_SIZE_B,
                );
                if block_start < block_end {
                    let block = raw_metadata[block_start..block_end].to_vec();
                    if let Some(peer) = self.peers.get_mut(&peer_addr) {
                        peer.send_metadata_extension_message(MetadataMessage::Data(
                            piece as u64,
                            raw_metadata_size as u64,
                            block,
                        ))
                        .await;
                    }
                } else {
                    log::debug!(
                        "rejecting metadata message request for {piece} piece from {peer_addr}, requested pieces is out of range"
                    );
                    if let Some(peer) = self.peers.get_mut(&peer_addr) {
                        peer.send_metadata_extension_message(MetadataMessage::Reject(piece as u64))
                            .await;
                    }
                }
            }
            (_, _, _) => {
                // send reject, we don't have the full metadata
                let peer = match self.peers.get_mut(&peer_addr) {
                    Some(peer) => peer,
                    None => return,
                };
                peer.send_metadata_extension_message(MetadataMessage::Reject(piece as u64))
                    .await;
            }
        }
    }

    async fn handle_receive_extended_message_metadata_message_data(
        &mut self,
        raw_metadata_size: i64,
        piece: i64,
        piece_block_data: Vec<u8>,
    ) {
        let raw_metadata_size = match self.raw_metadata_size {
            Some(raw_metadata_size) => raw_metadata_size,
            None => {
                // we do not know the metadata size yet, take notes
                self.raw_metadata_size = Some(raw_metadata_size);
                self.downloaded_metadata_blocks =
                    metadata_blocks_from_size(raw_metadata_size, false);
                self.raw_metadata = Some(vec![0; raw_metadata_size as usize]);
                raw_metadata_size
            }
        };

        if self.file_manager.is_some()
            || piece < 0
            || (piece as usize) >= self.downloaded_metadata_blocks.len()
            || self.downloaded_metadata_blocks[piece as usize].0
        {
            // we are not interested in this message
            return;
        }

        let raw_metadata_start = piece as usize * METADATA_BLOCK_SIZE_B;
        let raw_metadata_end = min(
            raw_metadata_size as usize,
            raw_metadata_start + METADATA_BLOCK_SIZE_B,
        );
        self.raw_metadata
            .as_deref_mut()
            .expect("just inserted above")[raw_metadata_start..raw_metadata_end]
            .copy_from_slice(&piece_block_data[..raw_metadata_end - raw_metadata_start]);
        self.downloaded_metadata_blocks[piece as usize].0 = true;

        // check if metadata is complete
        let metadata_download_complete = self
            .downloaded_metadata_blocks
            .iter()
            .all(|(completed, _, _)| *completed);
        if !metadata_download_complete {
            return;
        }

        let raw_metadata = self.raw_metadata.as_ref().expect("just inserted above");

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

                let peer = match self.peers.get_mut(&peer_addr) {
                    Some(peer) => peer,
                    None => return,
                };

                // remove outstanding request associated with this block
                peer.outstanding_block_requests
                    .remove(&(piece_idx, begin, data_len as u32));
                if piece_completed {
                    peer.requested_pieces.remove(&(piece_idx as usize));
                    self.outstanding_piece_assignments
                        .remove(&(piece_idx as usize));

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
                                .piece_completion_status
                                .clone(),
                        )
                        .await;

                    for (_, peer) in self.peers.iter_mut() {
                        if peer.haves.is_none() {
                            continue;
                        }
                        // send have to interested peers
                        if peer.peer_interested
                            && !peer
                                .haves
                                .as_ref()
                                .expect("haves is initialized if file_manager is")
                                [piece_idx as usize]
                        {
                            peer.send(ToPeerMsg::Send(Message::Have(piece_idx))).await;
                        }
                        // send not interested if needed
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
                                    .piece_completion_status
                                    [piece_idx as usize]
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
                // todo: maybe re-compute assignations immediately here instead of waiting tick
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
                            "removing peer {peer_addr} due to too many corrupted block received"
                        );
                        peer.send(ToPeerMsg::Disconnect()).await;
                        self.remove_peer(peer_addr).await;
                    }
                }
            }
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
        if let Some(removed_peer) = self.peers.remove(&peer_addr) {
            for (piece_idx, _) in removed_peer.requested_pieces {
                self.outstanding_piece_assignments.remove(&piece_idx);
            }
        }
        if self.peers.len() < CONNECTED_PEERS_TO_STOP_INCOMING_PEER_CONNECTIONS {
            self.ok_to_accept_connection_tx
                .send(true)
                .await
                .expect("ok_to_accept_connection_tx receiver half closed");
        }
    }

    async fn handle_ticker(&mut self) {
        self.log_stats(self.peers_to_torrent_manager_tx.capacity());

        // connect to new peers
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
                        .map(|f| f.piece_completion_status.clone()),
                    self.raw_metadata_size,
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

        // send keep-alives
        let now = SystemTime::now();
        for (_, peer) in self.peers.iter_mut() {
            if let Ok(elapsed) = now.duration_since(peer.last_sent) {
                if elapsed > KEEP_ALIVE_FREQ {
                    peer.send(ToPeerMsg::Send(Message::KeepAlive)).await;
                }
            }
        }

        // send status to tracker
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

        // unchoke peers
        let choking = should_choke(
            self.peers_to_torrent_manager_tx.capacity(),
            self.file_manager.is_some(),
        );
        if !choking {
            let now = SystemTime::now();
            for (_, peer) in self.peers.iter_mut() {
                if peer.am_choking
                    && now
                        .duration_since(peer.am_choking_since)
                        .unwrap_or_default()
                        > MIN_CHOKE_TIME
                {
                    peer.am_choking = false;
                    peer.send(ToPeerMsg::Send(Message::Unchoke)).await;
                }
            }
        }

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

        // remove requests that have not been fulfilled for some time,
        // most probably they have been silently dropped by the peer even if it is still alive and not choked
        self.remove_stale_requests();

        // ask DHT manager for new peers if we need this
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

        // remove old added / dropped events
        self.added_dropped_peer_events
            .retain(|(event_timestamp, _, _)| {
                now.duration_since(*event_timestamp).unwrap_or_default()
                    < ADDED_DROPPED_PEER_EVENTS_RETENTION
            });

        // send PEX messages
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

        // send torrent file request
        self.send_metadata_reqs().await;

        // send piece requests
        self.send_pieces_reqs().await;
    }

    async fn send_metadata_reqs(&mut self) {
        if self.file_manager.is_some() {
            return;
        }
        // we still have to download the metadata, ask metadata blocks to peers

        // get inflight requests
        let mut inflight_metadata_block_requests_per_peer: HashMap<PeerAddr, i64> = HashMap::new();
        for (downloaded, peer_addr, _) in self.downloaded_metadata_blocks.iter() {
            if *downloaded {
                continue;
            }
            if let Some(outstanding_requests) =
                inflight_metadata_block_requests_per_peer.get(peer_addr)
            {
                inflight_metadata_block_requests_per_peer
                    .insert(peer_addr.clone(), *outstanding_requests + 1);
            } else {
                inflight_metadata_block_requests_per_peer.insert(peer_addr.clone(), 1);
            }
        }

        // get possible peers we can ask for metadata blocks, sorted by outstanding reqs
        let now = SystemTime::now();
        let mut possible_peers = self
            .peers
            .iter_mut()
            .filter(|(_, peer)| {
                peer.support_metadata_extension()
                    && now
                        .duration_since(peer.last_metadata_request_rejection)
                        .unwrap_or_default()
                        > PEER_METADATA_REQUEST_REJECTION_COOL_OFF_PERIOD
            })
            .map(|(peer_addr, _)| {
                let outstanding_req = inflight_metadata_block_requests_per_peer
                    .get(peer_addr)
                    .map(|reqs| *reqs)
                    .unwrap_or_default();
                (outstanding_req, peer_addr.clone())
            })
            .collect::<Vec<(i64, String)>>();
        possible_peers.shuffle(&mut rand::rng());
        possible_peers.sort_by_key(|k| k.0);

        if possible_peers.len() == 0 {
            return;
        }

        let mut metadata_blocks_to_request: Vec<usize> = Vec::new();
        for n in 0..self.downloaded_metadata_blocks.len() {
            if !self.downloaded_metadata_blocks[n].0
                && now
                    .duration_since(self.downloaded_metadata_blocks[n].2)
                    .unwrap_or_default()
                    > METADATA_BLOCK_REQUEST_TIMEOUT
            {
                metadata_blocks_to_request.push(n);
            }
        }

        for (outstanding_reqs, peer_addr) in possible_peers.iter_mut() {
            while metadata_blocks_to_request.len() > 0 {
                if *outstanding_reqs > MAX_OUTSTANDING_BLOCK_REQUESTS_PER_PEER {
                    break;
                } else {
                    let block_to_request = metadata_blocks_to_request[0];
                    self.downloaded_metadata_blocks[block_to_request] =
                        (false, peer_addr.clone(), now);
                    log::debug!(
                        "sending metadata block request to {peer_addr}: {block_to_request}"
                    );
                    self.peers
                        .get_mut(peer_addr)
                        .expect("we filtered on this above")
                        .send_metadata_extension_message(MetadataMessage::Request(
                            block_to_request as u64,
                        ))
                        .await;
                    metadata_blocks_to_request.remove(0);
                }
            }
        }
    }

    fn remove_stale_requests(&mut self) {
        let now = SystemTime::now();
        for peer in self.peers.values_mut() {
            peer.outstanding_block_requests
                .retain(|(piece_idx, block_begin, data_len), req_time| {
                    return if now.duration_since(*req_time).unwrap_or_default() < self.request_timeout {
                        true
                    } else {
                        log::debug!("removed stale request to peer: {}: (piece idx: {piece_idx}, block begin: {block_begin}, length: {data_len})", peer.peer_addr);
                        peer.requested_pieces.remove(&(*piece_idx as usize));
                        self.outstanding_piece_assignments.remove(&(*piece_idx as usize));
                        false
                    }
                })
        }
    }

    async fn send_pieces_reqs(&mut self) {
        let file_manager = match &mut self.file_manager {
            Some(file_manager) => file_manager,
            None => return,
        };

        // send requests for new blocks for pieces currently downloading
        let mut piece_idx_to_remove = Vec::new();
        for (piece_idx, peer_addr) in self.outstanding_piece_assignments.iter() {
            if let Some(peer) = self.peers.get_mut(peer_addr) {
                if let Some(incomplete_piece) = peer.requested_pieces.get(&piece_idx) {
                    peer.send_requests_for_piece(*piece_idx, incomplete_piece.clone())
                        .await;
                } else {
                    log::warn!(
                        "could not find requested piece {piece_idx} for peer {peer_addr}, this should never happen"
                    );
                    piece_idx_to_remove.push(*piece_idx);
                }
            } else {
                log::warn!(
                    "could not find a peer for outstanding piece assigment (piece_idx: {piece_idx}, peer_addr: {peer_addr}), this should never happen"
                );
                piece_idx_to_remove.push(*piece_idx);
            }
        }
        for idx in piece_idx_to_remove {
            self.outstanding_piece_assignments.remove(&idx);
        }

        // assign incomplete pieces if not assigned yet
        for (piece_idx, piece) in file_manager.incomplete_pieces.iter() {
            if !self.outstanding_piece_assignments.contains_key(piece_idx) {
                assign_and_send_piece_reqs(
                    *piece_idx,
                    &mut self.peers,
                    &mut self.outstanding_piece_assignments,
                    piece,
                )
                .await;
            }
        }

        // assign other pieces, in order
        for piece_idx in 0..file_manager.num_pieces() {
            if self.outstanding_piece_assignments.len() > MAX_OUTSTANDING_PIECES {
                break;
            }
            if !file_manager.piece_completion_status[piece_idx]
                && !self.outstanding_piece_assignments.contains_key(&piece_idx)
            {
                assign_and_send_piece_reqs(
                    piece_idx,
                    &mut self.peers,
                    &mut self.outstanding_piece_assignments,
                    &Piece::new(file_manager.piece_length(piece_idx)),
                )
                .await;
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
            "left: {left}, pieces: {completed_pieces}/{total_pieces} metadata blocks: {known_metadata_blocks}/{total_metadata_blocks} | Up: {up_band}/s, Down: {down_band}/s (tot.: {tot_up}, {tot_down}), wasted: {wasted} | known peers from advertising: {known_peers} (bad: {bad_peers}), connected: {connected_peers}, unchoked: {unchoked_peers} | pending peers_to_torrent_manager msgs: {cur_ch_cap}/{tot_ch_cap}",
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
                    .map(|f| f.wasted_bytes)
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
            known_metadata_blocks = self
                .downloaded_metadata_blocks
                .iter()
                .fold(0, |acc, v| if v.0 { acc + 1 } else { acc }),
            total_metadata_blocks = if self.downloaded_metadata_blocks.len() == 0 {
                "?".to_string()
            } else {
                self.downloaded_metadata_blocks.len().to_string()
            },
            tot_ch_cap = PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY,
        );
    }

    fn corrupted_metadata(&mut self, error: Error) {
        log::warn!(
            "downloaded metadata is corrupted ({}), starting over its download...",
            error
        );
        self.raw_metadata_size = None;
        self.downloaded_metadata_blocks = Vec::<(bool, PeerAddr, SystemTime)>::new();
        self.raw_metadata = None;
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
    file_manager_initialized: bool,
) -> bool {
    peers_to_torrent_manager_channel_capacity < PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY / 2
        || !file_manager_initialized
}

async fn assign_and_send_piece_reqs(
    piece_idx: usize,
    peers: &mut HashMap<String, Peer>,
    outstanding_piece_assignments: &mut HashMap<usize, String>,
    incomplete_piece: &Piece,
) {
    let mut possible_peers = peers
        .iter_mut()
        .filter(|(_, peer)| {
            !peer.peer_choking
                && peer.haves.as_ref().map_or(false, |haves| haves[piece_idx])
                && peer.outstanding_block_requests.len() < MAX_OUTSTANDING_REQUESTS_PER_PEER
        })
        .map(|(peer_addr, peer)| {
            let outstanding_block_requests = peer.outstanding_block_requests.len();
            let concurrent_requested_pieces = peer
                .outstanding_block_requests
                .keys()
                .map(|(piece_idx, _, _)| *piece_idx)
                .collect::<HashSet<u32>>()
                .len();
            (
                peer_addr,
                peer,
                concurrent_requested_pieces,
                outstanding_block_requests,
            )
        })
        .collect::<Vec<(&String, &mut Peer, usize, usize)>>();

    possible_peers.shuffle(&mut rand::rng());

    possible_peers.sort_by(|a, b| {
        return if a.2 < b.2 {
            Ordering::Less
        } else if a.2 > b.2 {
            Ordering::Greater
        } else if a.3 < b.3 {
            Ordering::Less
        } else if a.3 > b.3 {
            Ordering::Greater
        } else {
            Ordering::Equal
        };
    });

    if possible_peers.len() > 0 {
        let peer_addr = possible_peers[0].0;
        let peer = &mut possible_peers[0].1;
        peer.send_requests_for_piece(piece_idx, incomplete_piece.clone())
            .await;
        outstanding_piece_assignments.insert(piece_idx, peer_addr.clone());
    }
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

fn metadata_blocks_from_size(size: i64, default_value: bool) -> Vec<(bool, PeerAddr, SystemTime)> {
    vec![
        (
            default_value,
            "0.0.0.0:0".to_string(),
            SystemTime::UNIX_EPOCH
        );
        (size as f64 / METADATA_BLOCK_SIZE_B as f64).ceil() as usize
    ]
}

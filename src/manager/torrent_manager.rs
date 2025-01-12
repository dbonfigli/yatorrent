use anyhow::{bail, Result};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{iter, path::Path};

use rand::seq::SliceRandom;
use rand::Rng;
use size::{Size, Style};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::dht::dht_manager::{DhtManager, DhtToTorrentManagerMsg, ToDhtManagerMsg};
use crate::manager::peer::{self, PeerAddr, PeersToManagerMsg, ToPeerCancelMsg, ToPeerMsg};
use crate::persistence::piece::Piece;
use crate::torrent_protocol::wire_protocol::Message;
use crate::tracker;
use crate::util::start_tick;
use crate::{
    persistence::file_manager::FileManager,
    tracker::{Event, Response, TrackerClient},
};

use crate::bencoding::Value::{self, Dict, Int, Str};

use super::peer::PeerError;

static CONNECTED_PEERS_TO_STOP_INCOMING_PEER_CONNECTIONS: usize = 500;
static CONNECTED_PEERS_TO_START_NEW_PEER_CONNECTIONS: usize = 350;
static MAX_CONNECTED_PEERS_TO_ASK_DHT_FOR_MORE: usize = 10;
static DHT_NEW_PEER_COOL_OFF_PERIOD: Duration = Duration::from_secs(15);
static DHT_BOOTSTRAP_TIME: Duration = Duration::from_secs(5);
static KEEP_ALIVE_FREQ: Duration = Duration::from_secs(90);
static MAX_OUTSTANDING_REQUESTS_PER_PEER: usize = 500; // can be retrieved per peer if it supports extensions, dict key "reqq", seen: deluge: 2000, qbittorrent: 500, transmission: 500, utorrent: 255, freebox bittorrent 2: 768, maybe variable
static MAX_OUTSTANDING_PIECES: usize = 2000;
static BLOCK_SIZE_B: u64 = 16384;
static TO_PEER_CHANNEL_CAPACITY: usize = 2000;
static TO_PEER_CANCEL_CHANNEL_CAPACITY: usize = 1000;
static PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY: usize = 50000;
static BASE_REQUEST_TIMEOUT: Duration = Duration::from_secs(120); // decreasing this will wast more bandwidth (needlessy requesting the same block again even if a peer would send it eventually) but will make retries for pieces requested to slow peers faster
static ENDGAME_REQUEST_TIMEOUT: Duration = Duration::from_secs(15); // request timeout during the endgame phase: this will re-request a lot of pieces, wasting bandwidth, but will make endgame faster churning slow peers
static ENDGAME_START_AT_COMPLETION_PERCENTAGE: f64 = 98.; // start endgame when we have this percentage of the torrent
static MIN_CHOKE_TIME: Duration = Duration::from_secs(60);
static NEW_CONNECTION_COOL_OFF_PERIOD: Duration = Duration::from_secs(180);
static ADDED_DROPPED_PEER_EVENTS_RETENTION: Duration = Duration::from_secs(90);
static PEX_MESSAGE_COOLOFF_PERIOD: Duration = Duration::from_secs(60);

pub struct Peer {
    am_choking: bool,
    am_choking_since: SystemTime,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    haves: Option<Vec<bool>>,
    to_peer_tx: Sender<ToPeerMsg>,
    last_sent: SystemTime,
    to_peer_cancel_tx: Sender<ToPeerCancelMsg>,
    outstanding_block_requests: HashMap<(u32, u32, u32), SystemTime>, // (piece idx, block begin, data len) -> request time
    requested_pieces: HashMap<usize, Piece>, // piece idx -> piece status with all the requested fragments
    ut_pex_id: u8,
    last_pex_message_sent: SystemTime,
}

impl Peer {
    pub fn new(
        num_pieces: Option<usize>,
        to_peer_tx: Sender<ToPeerMsg>,
        to_peer_cancel_tx: Sender<ToPeerCancelMsg>,
        am_choking: bool,
    ) -> Self {
        let now = SystemTime::now();
        return Peer {
            am_choking,
            am_choking_since: SystemTime::UNIX_EPOCH,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            haves: num_pieces.map(|n| vec![false; n]),
            to_peer_tx,
            last_sent: now,
            to_peer_cancel_tx,
            outstanding_block_requests: HashMap::new(),
            requested_pieces: HashMap::new(),
            ut_pex_id: 0, // i.e. no support for pex on this peer, initially
            last_pex_message_sent: SystemTime::UNIX_EPOCH,
        };
    }

    async fn send(&mut self, msg: ToPeerMsg) {
        if self.to_peer_tx.capacity() <= 5 {
            log::warn!("low to_peer_tx capacity: {}", self.to_peer_tx.capacity());
        }
        self.last_sent = SystemTime::now();
        let _ = self.to_peer_tx.send(msg).await;
        // ignore errors: it can happen that the channel is closed on the other side if the rx handler loop exited due to network errors
        // and the peer is still lingering in self.peers because the control message about the error is not yet been handled
    }

    fn support_pex(&self) -> bool {
        self.ut_pex_id != 0
    }

    async fn send_pex_message(
        &mut self,
        peer_addr: &String,
        added: Vec<String>,
        dropped: Vec<String>,
    ) {
        let mut h = HashMap::new();
        if added.len() > 0 {
            h.insert(
                b"added".to_vec(),
                Value::Str(ip_port_list_to_compact_format(added)),
            );
        }
        if dropped.len() > 0 {
            h.insert(
                b"dropped".to_vec(),
                Value::Str(ip_port_list_to_compact_format(dropped)),
            );
        }
        self.last_pex_message_sent = SystemTime::now();
        if h.len() > 0 {
            let pex_msg = Message::Extended(self.ut_pex_id, Value::Dict(h, 0, 0));
            log::trace!("sending pex message to peer {peer_addr}: {pex_msg}");
            self.send(ToPeerMsg::Send(pex_msg)).await;
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
    outstanding_piece_assigments: HashMap<usize, String>, // piece idx -> peer_addr
    completed_sent_to_tracker: bool,
    listening_dht_port: u16,
    dht_nodes: Vec<String>,
    last_get_peers_requested_time: SystemTime,
    added_dropped_peer_events: Vec<(SystemTime, PeerAddr, PexEvent)>, // time of event, address of peer for this event, pex event. This field is used to support pex
    request_timeout: Duration,
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
            u64,                // piece_lenght
            Vec<[u8; 20]>,      // piece_hashes
        )>,
        // dht data
        listening_dht_port: u16,
        dht_nodes: Vec<String>,
    ) -> Self {
        let own_peer_id = generate_peer_id();
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
            advertised_peers: Arc::new(Mutex::new(HashMap::new())),
            bad_peers: HashSet::new(),
            last_bandwidth_poll: SystemTime::now(),
            uploaded_bytes: 0,
            downloaded_bytes: 0,
            uploaded_bytes_previous_poll: 0,
            downloaded_bytes_previous_poll: 0,
            outstanding_piece_assigments: HashMap::new(),
            completed_sent_to_tracker: false,
            listening_dht_port,
            dht_nodes,
            last_get_peers_requested_time: SystemTime::now() - DHT_NEW_PEER_COOL_OFF_PERIOD
                + DHT_BOOTSTRAP_TIME, // try to wait a bit before the first request, in hope that the dht has been bootstrapped, so that we don't waste time for the first request with an empty routing table
            added_dropped_peer_events: Vec::new(),
            request_timeout: BASE_REQUEST_TIMEOUT,
        }
    }

    pub async fn start(&mut self) {
        // start dht manager
        let (to_dht_manager_tx, to_dht_manager_rx) = mpsc::channel(1000);
        let (dht_to_torrent_manager_tx, dht_to_torrent_manager_rx) = mpsc::channel(1000);
        let mut dht_manager = DhtManager::new(
            self.listening_torrent_wire_protocol_port,
            self.listening_dht_port,
            self.dht_nodes.clone(),
        );
        tokio::spawn(async move {
            dht_manager
                .start(to_dht_manager_rx, dht_to_torrent_manager_tx)
                .await;
        });

        // start incoming peer connections handler
        let (ok_to_accept_connection_tx, ok_to_accept_connection_rx) = mpsc::channel(10);
        let (piece_completion_status_tx, piece_completion_status_rx) = mpsc::channel(100);
        let (peers_to_torrent_manager_tx, peers_to_torrent_manager_rx) =
            mpsc::channel::<PeersToManagerMsg>(PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY);
        peer::run_new_incoming_peers_handler(
            self.info_hash.clone(),
            self.own_peer_id.clone(),
            self.listening_torrent_wire_protocol_port.clone(),
            self.file_manager
                .as_ref()
                .map(|f| f.piece_completion_status.clone()),
            ok_to_accept_connection_rx,
            piece_completion_status_rx,
            peers_to_torrent_manager_tx.clone(),
        )
        .await;

        // start ticker
        let (tick_tx, tick_rx) = mpsc::channel(1);
        start_tick(tick_tx, Duration::from_secs(1)).await;

        // start contro loop to handle channel messages - will block forever
        self.control_loop(
            ok_to_accept_connection_tx.clone(),
            piece_completion_status_tx.clone(),
            peers_to_torrent_manager_tx,
            peers_to_torrent_manager_rx,
            tick_rx,
            to_dht_manager_tx,
            dht_to_torrent_manager_rx,
        )
        .await;
    }

    async fn control_loop(
        &mut self,
        ok_to_accept_connection_tx: Sender<bool>,
        piece_completion_status_channel_tx: Sender<Vec<bool>>,
        peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
        mut peers_to_torrent_manager_rx: Receiver<PeersToManagerMsg>,
        mut tick_rx: Receiver<()>,
        to_dht_manager_tx: Sender<ToDhtManagerMsg>,
        mut dht_to_torrent_manager_rx: Receiver<DhtToTorrentManagerMsg>,
    ) {
        loop {
            tokio::select! {
                Some(msg) = peers_to_torrent_manager_rx.recv() => {
                    match msg {
                        PeersToManagerMsg::Error(peer_addr, error_type) => {
                            self.handle_peer_error(peer_addr, error_type, ok_to_accept_connection_tx.clone()).await;
                        }
                        PeersToManagerMsg::Receive(peer_addr, msg) => {
                            self.handle_receive_message(peer_addr, msg, piece_completion_status_channel_tx.clone(), peers_to_torrent_manager_tx.capacity(), to_dht_manager_tx.clone()).await;
                        }
                        PeersToManagerMsg::NewPeer(tcp_stream) => {
                            self.handle_new_peer(tcp_stream, peers_to_torrent_manager_tx.clone(), ok_to_accept_connection_tx.clone(), to_dht_manager_tx.clone()).await;
                        }
                    }
                }
                Some(()) = tick_rx.recv() => {
                    self.handle_ticker(peers_to_torrent_manager_tx.clone(), to_dht_manager_tx.clone()).await;
                }
                Some(DhtToTorrentManagerMsg::NewPeer(ip, port)) = dht_to_torrent_manager_rx.recv() => {
                    let p = tracker::Peer{peer_id: None, ip: ip.to_string(), port: port};
                    let mut advertised_peers_mg = self.advertised_peers.lock().unwrap();
                    advertised_peers_mg.insert(format!("{ip}:{port}"), (p, SystemTime::UNIX_EPOCH));
                    drop(advertised_peers_mg);
                }
                else => break,
            }
        }
    }

    async fn handle_receive_message(
        &mut self,
        peer_addr: String,
        msg: Message,
        piece_completion_status_tx: Sender<Vec<bool>>,
        peers_to_torrent_manager_channel_capacity: usize,
        to_dht_manager_tx: Sender<ToDhtManagerMsg>,
    ) {
        log::trace!("received message from peer {peer_addr}: {msg}");
        let now = SystemTime::now();

        match msg {
            Message::KeepAlive => {}
            Message::Choke => self.handle_receive_choke_message(peer_addr).await,
            Message::Unchoke => {
                if let Some(peer) = self.peers.get_mut(&peer_addr) {
                    peer.peer_choking = false;
                    // todo: maybe re-compute assignations immediately here instead of waiting tick
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
            Message::Request(piece_idx, begin, lenght) => {
                self.handle_receive_request_message(
                    peer_addr,
                    piece_idx,
                    begin,
                    lenght,
                    peers_to_torrent_manager_channel_capacity,
                )
                .await
            }
            Message::Piece(piece_idx, begin, data) => {
                self.handle_receive_piece_message(
                    peer_addr,
                    piece_idx,
                    begin,
                    data,
                    piece_completion_status_tx,
                )
                .await
            }
            Message::Cancel(piece_idx, begin, lenght) => {
                if let Some(peer) = self.peers.get_mut(&peer_addr) {
                    // we try to let the peer message handler know about the cancellation,
                    // but it the buffer is full, we don't care, it means there were no outstunding messages to be sent
                    // and so the cancellation would have no effect
                    let _ = peer
                        .to_peer_cancel_tx
                        .try_send((piece_idx, begin, lenght, now));
                }
            }
            Message::Port(port) => {
                // we know the peer supports DHT, send this to dht as new node
                let peer_ip_addr = peer_addr.split(":").next().unwrap();
                let _ = to_dht_manager_tx
                    .send(ToDhtManagerMsg::NewNode(format!(
                        "{}:{}",
                        peer_ip_addr, port
                    )))
                    .await;
            }
            Message::Extended(extension_id, value) => {
                self.handle_receive_extended_message(peer_addr, extension_id, value)
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
            log::warn!("got message \"have\" {piece_idx} from peer {peer_addr} but the torrent have only {pieces} pieces");
            // todo: close connection with this bad peer
        }
    }

    async fn handle_receive_bitfield_message(
        &mut self,
        peer_addr: String,
        bitfield: Vec<bool>,
    ) {
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
            // todo: close connection with this bad peer
        } else if let Some(peer) = self.peers.get_mut(&peer_addr) {
            // ignore bitfield if we don't have the torrent file yet, we cannot trust the bitfield from the peer
            if peer.haves.is_none() {
                return;
            }

            // bitfield is byte aligned, it could contain more bits than pieces in the torrent
            peer.haves = Some(bitfield[0..file_manager.num_pieces()].to_vec());

            // check if we need to send interest
            if !peer.am_interested {
                for piece_idx in 0..peer.haves.as_ref().unwrap().len() {
                    if !file_manager.piece_completion_status[piece_idx]
                        && peer.haves.as_ref().unwrap()[piece_idx]
                    {
                        peer.am_interested = true;
                        peer.send(ToPeerMsg::Send(Message::Interested)).await;
                        break;
                    }
                }
            }

            log::trace!(
                "received bitfield from peer {peer_addr}: it has {}/{} pieces",
                peer.haves
                    .as_ref()
                    .unwrap()
                    .iter()
                    .fold(0, |acc, v| if *v { acc + 1 } else { acc }),
                peer.haves.as_ref().unwrap().len()
            );
        }
        // todo: maybe re-compute assignations immediately here instead of waiting tick
    }

    async fn handle_receive_choke_message(&mut self, peer_addr: String) {
        let peer = match self.peers.get_mut(&peer_addr) {
            Some(peer) => peer,
            None => return,
        };

        log::debug!(
            "received choked from peer {peer_addr} with {} outstandig requests",
            peer.outstanding_block_requests.len()
        );
        peer.peer_choking = true;

        // remove outstandig requests that will be discarded because the peer is choking
        for (piece_idx, _) in peer.requested_pieces.iter() {
            self.outstanding_piece_assigments
                .remove(&(*piece_idx as usize));
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
        lenght: u32,
        peers_to_torrent_manager_channel_capacity: usize,
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
            if should_choke(peers_to_torrent_manager_channel_capacity, true) {
                peer.send(ToPeerMsg::Send(Message::Choke)).await;
                peer.am_choking = true;
                peer.am_choking_since = SystemTime::now();
            } else {
                match file_manager.read_piece_block(piece_idx as usize, begin as u64, lenght as u64)
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
        value: Value,
    ) {
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

        if extension_id == 0 {
            // this is an extension handshake
            if let Dict(d, _, _) = value {
                if let Some(Dict(m, _, _)) = d.get(&b"m".to_vec()) {
                    if let Some(Int(ut_pex_id)) = m.get(&b"ut_pex".to_vec()) {
                        // this peer supports the PEX extension, registered at number ut_pex_id
                        peer.ut_pex_id = *ut_pex_id as u8;
                        // send first peer list
                        peer.send_pex_message(&peer_addr, other_active_peers, Vec::new())
                            .await;
                    }
                }
            }
        } else if peer.ut_pex_id == extension_id {
            // this is an ut_pex extended message
            if let Dict(d, _, _) = value {
                // todo: should we also use the dropped list? atm we are eager to hoarde all possible peers so we ignore it
                if let Some(Str(compact_contacts_info)) = d.get(&b"added".to_vec()) {
                    // we don't support flags, dropped or ipv6 fields ATM
                    if compact_contacts_info.len() % 6 != 0 {
                        log::debug!("got a PEX message with an \"added\" field that is not divisible by 6, ignoring this message");
                    } else {
                        for i in (0..compact_contacts_info.len()).step_by(6) {
                            let mut peer_ip_buf: [u8; 4] = [0; 4];
                            peer_ip_buf.copy_from_slice(&compact_contacts_info[i..i + 4]);
                            let ip = [
                                (peer_ip_buf[0]).to_string(),
                                (peer_ip_buf[1]).to_string(),
                                (peer_ip_buf[2]).to_string(),
                                peer_ip_buf[3].to_string(),
                            ]
                            .join(".");
                            let mut peer_port_buf: [u8; 2] = [0; 2];
                            peer_port_buf.copy_from_slice(&compact_contacts_info[i + 4..i + 6]);
                            let port = u16::from_be_bytes(peer_port_buf);
                            log::debug!(
                                "adding peer advertised by {peer_addr} from PEX: {ip}:{port}"
                            );
                            let p = tracker::Peer {
                                peer_id: None,
                                ip: ip.clone(),
                                port,
                            };
                            let mut advertised_peers_mg = self.advertised_peers.lock().unwrap();
                            advertised_peers_mg
                                .insert(format!("{}:{}", ip, port), (p, SystemTime::UNIX_EPOCH));
                            drop(advertised_peers_mg);
                        }
                    }
                }
            }
        } else {
            log::debug!("got an extension message from {peer_addr} but id was not recognized as an extension we registered: {extension_id}");
        }
    }

    async fn handle_receive_piece_message(
        &mut self,
        peer_addr: String,
        piece_idx: u32,
        begin: u32,
        data: Vec<u8>,
        piece_completion_status_tx: Sender<Vec<bool>>,
    ) {
        if self.file_manager.is_none() {
            return;
        }

        let data_len = data.len() as u64;
        match self.file_manager.as_mut().unwrap().write_piece_block(
            piece_idx as usize,
            data,
            begin as u64,
        ) {
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
                    self.outstanding_piece_assigments
                        .remove(&(piece_idx as usize));

                    if !self.completed_sent_to_tracker
                        && self.file_manager.as_mut().unwrap().completed()
                    {
                        log::warn!("torrent download completed");
                        self.completed_sent_to_tracker = true;
                        self.async_request_to_tracker(Event::Completed).await;
                    }

                    // ignore errors here: it can happen that the channel is closed on the other side if the rx handler loop exited
                    // due to network errors and the peer is still lingering in self.peers because the control message about the error is not yet been handled
                    let _ = piece_completion_status_tx
                        .send(
                            self.file_manager
                                .as_mut()
                                .unwrap()
                                .piece_completion_status
                                .clone(),
                        )
                        .await;

                    for (_, peer) in self.peers.iter_mut() {
                        if peer.haves.is_none() {
                            continue;
                        }
                        // send have to interested peers
                        if peer.peer_interested && !peer.haves.as_ref().unwrap()[piece_idx as usize] {
                            peer.send(ToPeerMsg::Send(Message::Have(piece_idx))).await;
                        }
                        // send not interested if needed
                        if peer.am_interested {
                            let mut am_still_interested = false;
                            for i in 0..peer.haves.as_ref().unwrap().len() {
                                if !self.file_manager.as_mut().unwrap().piece_completion_status
                                    [piece_idx as usize]
                                    && peer.haves.as_ref().unwrap()[i]
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
                log::error!("cannot write block: {e}");
            }
        }
    }

    async fn handle_peer_error(
        &mut self,
        peer_addr: String,
        error_type: PeerError,
        ok_to_accept_connection_tx: Sender<bool>,
    ) {
        log::debug!("removing errored peer {peer_addr}");
        self.added_dropped_peer_events.push((
            SystemTime::now(),
            peer_addr.clone(),
            PexEvent::Dropped,
        ));
        if let Some(removed_peer) = self.peers.remove(&peer_addr) {
            for (piece_idx, _) in removed_peer.requested_pieces {
                self.outstanding_piece_assigments.remove(&piece_idx);
            }
        }
        if error_type == PeerError::HandshakeError {
            // todo: understand other error cases that are not recoverable and should stop trying again on this peer
            self.bad_peers.insert(peer_addr);
        }
        if self.peers.len() < CONNECTED_PEERS_TO_STOP_INCOMING_PEER_CONNECTIONS {
            ok_to_accept_connection_tx.send(true).await.unwrap();
        }
    }

    async fn handle_ticker(
        &mut self,
        peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
        to_dht_manager_tx: Sender<ToDhtManagerMsg>,
    ) {
        self.log_stats(peers_to_torrent_manager_tx.capacity());

        // connect to new peers
        let current_peers_n = self.peers.len();
        if current_peers_n < CONNECTED_PEERS_TO_START_NEW_PEER_CONNECTIONS {
            let possible_peers_mg = self.advertised_peers.lock().unwrap();
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
                        // this cooloff time is also important to avoid new connections to peers we attempted few secs ago
                        // and for which a connection attempt is still inflight
                        && now.duration_since(*last_connection_attempt).unwrap()
                            > NEW_CONNECTION_COOL_OFF_PERIOD
                })
                .collect::<Vec<_>>();

            let candidates_for_new_connections: Vec<_> = possible_peers
                .choose_multiple(
                    &mut rand::thread_rng(),
                    CONNECTED_PEERS_TO_START_NEW_PEER_CONNECTIONS - current_peers_n,
                )
                .collect();
            // todo:
            // * better algorithm to select new peers
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
                    peers_to_torrent_manager_tx.clone(),
                ));
            }
            // update last connection attempt
            let mut possible_peers_mg = self.advertised_peers.lock().unwrap();
            for (peer_addr, _) in candidates_for_new_connections.iter() {
                let possible_peer_entry = possible_peers_mg.get_mut(*peer_addr).unwrap();
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
        let tracker_client_mg = self.tracker_client.lock().unwrap();
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
            peers_to_torrent_manager_tx.capacity(),
            self.file_manager.is_some(),
        );
        if !choking {
            let now = SystemTime::now();
            for (_, peer) in self.peers.iter_mut() {
                if peer.am_choking
                    && now.duration_since(peer.am_choking_since).unwrap() > MIN_CHOKE_TIME
                {
                    peer.am_choking = false;
                    peer.send(ToPeerMsg::Send(Message::Unchoke)).await;
                }
            }
        }

        // check endgame status an decrease request timeout if needed
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
                .unwrap()
                > DHT_NEW_PEER_COOL_OFF_PERIOD
        {
            self.last_get_peers_requested_time = now;
            let _ = to_dht_manager_tx
                .send(ToDhtManagerMsg::GetNewPeers(self.info_hash))
                .await;
        }

        // remove old added / dropped events
        self.added_dropped_peer_events
            .retain(|(event_timestamp, _, _)| {
                now.duration_since(*event_timestamp).unwrap() < ADDED_DROPPED_PEER_EVENTS_RETENTION
            });

        // send PEX messages
        for (peer_addr, peer) in self.peers.iter_mut().filter(|(_, p)| {
            p.support_pex()
                && now.duration_since(p.last_pex_message_sent).unwrap() > PEX_MESSAGE_COOLOFF_PERIOD
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
            peer.send_pex_message(peer_addr, added, dropped).await;
        }

        // send torrent file request
        self.send_torrent_file_reqs().await;

        // send piece requests
        self.send_pieces_reqs().await;
    }


    async fn send_torrent_file_reqs(&mut self) {
        if self.file_manager.is_some() {
            return;
        }
        // todo: implement
    }

    fn remove_stale_requests(&mut self) {
        let now = SystemTime::now();
        for (peer_addr, peer) in self.peers.iter_mut() {
            peer.outstanding_block_requests
                .retain(|(piece_idx, block_begin, data_len), req_time| {
                    if now.duration_since(*req_time).unwrap() < self.request_timeout {
                        return true;
                    } else {
                        log::debug!("removed stale request to peer: {peer_addr}: (piece idx: {piece_idx}, block begin: {block_begin}, lenght: {data_len})");
                        peer.requested_pieces.remove(&(*piece_idx as usize));
                        self.outstanding_piece_assigments.remove(&(*piece_idx as usize));
                        return false;
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
        for (piece_idx, peer_addr) in self.outstanding_piece_assigments.iter() {
            if let Some(peer) = self.peers.get_mut(peer_addr) {
                if let Some(incomplete_piece) = peer.requested_pieces.get(&piece_idx) {
                    peer.send_requests_for_piece(*piece_idx, incomplete_piece.clone())
                        .await;
                } else {
                    log::warn!("could not find requested piece {piece_idx} for peer {peer_addr}, this should never happen");
                    piece_idx_to_remove.push(*piece_idx);
                }
            } else {
                log::warn!("could not find a peer for outstanding piece assigment (piece_idx: {piece_idx}, peer_addr: {peer_addr}), this should never happen");
                piece_idx_to_remove.push(*piece_idx);
            }
        }
        for idx in piece_idx_to_remove {
            self.outstanding_piece_assigments.remove(&idx);
        }

        // assign incomplete pieces if not assigned yet
        for (piece_idx, piece) in file_manager.incomplete_pieces.iter() {
            if !self.outstanding_piece_assigments.contains_key(piece_idx) {
                assign_and_send_piece_reqs(
                    *piece_idx,
                    &mut self.peers,
                    &mut self.outstanding_piece_assigments,
                    piece,
                )
                .await;
            }
        }

        // assign other pieces, in order
        for piece_idx in 0..file_manager.num_pieces() {
            if self.outstanding_piece_assigments.len() > MAX_OUTSTANDING_PIECES {
                break;
            }
            if !file_manager.piece_completion_status[piece_idx]
                && !self.outstanding_piece_assigments.contains_key(&piece_idx)
            {
                assign_and_send_piece_reqs(
                    piece_idx,
                    &mut self.peers,
                    &mut self.outstanding_piece_assigments,
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
        let tracker_client_mg = self.tracker_client.lock().unwrap();
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

    async fn handle_new_peer(
        &mut self,
        tcp_stream: TcpStream,
        peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
        ok_to_accept_connection_tx: Sender<bool>,
        to_dht_manager_tx: Sender<ToDhtManagerMsg>,
    ) {
        let peer_addr = match tcp_stream.peer_addr() {
            Ok(s) => {
                // send to dht manager the fact that we know a new good peer
                let peer_port = s.port();
                if let IpAddr::V4(peer_addr) = s.ip() {
                    to_dht_manager_tx
                        .send(ToDhtManagerMsg::ConnectedToNewPeer(
                            self.info_hash,
                            peer_addr,
                            peer_port,
                        ))
                        .await
                        .unwrap();
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
            peers_to_torrent_manager_tx.clone(),
            to_peer_rx,
            to_peer_cancel_rx,
        );
        self.peers.insert(
            peer_addr.clone(),
            Peer::new(
                self.file_manager.as_ref().map(|f| f.num_pieces()),
                to_peer_tx,
                to_peer_cancel_tx,
                should_choke(
                    peers_to_torrent_manager_tx.capacity(),
                    self.file_manager.is_some(),
                ),
            ),
        );
        log::debug!("new peer initialized: {peer_addr}");
        self.added_dropped_peer_events
            .push((SystemTime::now(), peer_addr, PexEvent::Added));
        if self.peers.len() > CONNECTED_PEERS_TO_STOP_INCOMING_PEER_CONNECTIONS {
            log::trace!("stop accepting new peers");
            ok_to_accept_connection_tx.send(false).await.unwrap();
        }
    }

    fn log_stats(&mut self, peers_to_torrent_manager_channel_capacity: usize) {
        let advertised_peers_lock = self.advertised_peers.lock().unwrap();
        let advertised_peers_len = advertised_peers_lock.len();
        drop(advertised_peers_lock);
        let now = SystemTime::now();
        let elapsed_s = now
            .duration_since(self.last_bandwidth_poll)
            .unwrap()
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
            "left: {left}, pieces: {completed_pieces}/{total_pieces} | Up: {up_band}/s, Down: {down_band}/s (tot.: {tot_up}, {tot_down}), wasted: {wasted} | known peers: {known_peers} (bad: {bad_peers}), connected: {connected_peers}, unchoked: {unchoked_peers} | pending peers_to_torrent_manager msgs: {cur_ch_cap}/{tot_ch_cap}",            
            left=self.file_manager.as_ref().map(|f| Size::from_bytes(f.bytes_left()).to_string()).unwrap_or("?".to_string()),
            completed_pieces=self.file_manager.as_ref().map(|f| f.completed_pieces()).unwrap_or(0),
            total_pieces=self.file_manager.as_ref().map(|f| f.num_pieces().to_string()).unwrap_or("?".to_string()),
            up_band=Size::from_bytes(bandwidth_up).format().with_style(Style::Abbreviated),
            down_band=Size::from_bytes(bandwidth_down).format().with_style(Style::Abbreviated),
            tot_up=Size::from_bytes(self.uploaded_bytes).format().with_style(Style::Abbreviated),
            tot_down=Size::from_bytes(self.downloaded_bytes).format().with_style(Style::Abbreviated),
            wasted=Size::from_bytes(self.file_manager.as_ref().map(|f| f.wasted_bytes).unwrap_or(0)).format().with_style(Style::Abbreviated),
            known_peers=advertised_peers_len,
            bad_peers=self.bad_peers.len(),
            connected_peers=self.peers.len(),
            unchoked_peers=self.peers.iter().fold(0, |acc, (_,p)| if !p.peer_choking { acc + 1 } else { acc }),
            cur_ch_cap=PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY - peers_to_torrent_manager_channel_capacity,
            tot_ch_cap=PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY,
        );
    }
}

fn generate_peer_id() -> String {
    const CHARSET: &[u8] = b"0123456789";
    let mut rng = rand::thread_rng();
    let one_char = || CHARSET[rng.gen_range(0..CHARSET.len())] as char;
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
            log::error!("could not perform request to tracker: {e}");
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
            log::trace!("full tracker response:\n{:?}", ok_response);

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
    let mut tracker_client_mg = tracker_client.lock().unwrap();
    *tracker_client_mg = updated_tracker_client;
    drop(tracker_client_mg);
    let mut advertised_peers = advertised_peers.lock().unwrap();
    latest_advertised_peers.iter().for_each(|p| {
        advertised_peers.insert(
            format!("{}:{}", p.ip, p.port),
            (p.clone(), SystemTime::UNIX_EPOCH),
        );
    });
    drop(advertised_peers);
}

fn should_choke(
    peers_to_torrent_manager_channel_pending_msgs: usize,
    file_manager_initialized: bool,
) -> bool {
    peers_to_torrent_manager_channel_pending_msgs > PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY / 2
        || !file_manager_initialized
}

async fn assign_and_send_piece_reqs(
    piece_idx: usize,
    peers: &mut HashMap<String, Peer>,
    outstanding_piece_assigments: &mut HashMap<usize, String>,
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

    possible_peers.shuffle(&mut rand::thread_rng());

    possible_peers.sort_by(|a, b| {
        if a.2 < b.2 {
            return Ordering::Less;
        } else if a.2 > b.2 {
            return Ordering::Greater;
        } else if a.3 < b.3 {
            return Ordering::Less;
        } else if a.3 > b.3 {
            return Ordering::Greater;
        } else {
            return Ordering::Equal;
        }
    });

    if possible_peers.len() > 0 {
        let peer_addr = possible_peers[0].0;
        let peer = &mut possible_peers[0].1;
        peer.send_requests_for_piece(piece_idx, incomplete_piece.clone())
            .await;
        outstanding_piece_assigments.insert(piece_idx, peer_addr.clone());
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
        let port = ip_port[1].as_bytes();
        compact_format.append(&mut port.to_vec());
    }
    compact_format
}

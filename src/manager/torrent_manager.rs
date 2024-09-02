use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{error::Error, iter, path::Path};

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
    metainfo::Metainfo,
    persistence::file_manager::FileManager,
    tracker::{Event, Response, TrackerClient},
};

use super::peer::PeerError;

static CONNECTED_PEERS_TO_STOP_INCOMING_PEER_CONNECTIONS: usize = 100;
static CONNECTED_PEERS_TO_START_NEW_PEER_CONNECTIONS: usize = 80;
static MAX_CONNECTED_PEERS_TO_ASK_DHT_FOR_MORE: usize = 10;
static DHT_NEW_PEER_COOL_OFF_PERIOD: Duration = Duration::from_secs(60);
static KEEP_ALIVE_FREQ: Duration = Duration::from_secs(90);
static MAX_OUTSTANDING_REQUESTS_PER_PEER: usize = 500;
static MAX_OUTSTANDING_PIECES: usize = 100;
static BLOCK_SIZE_B: u64 = 16384;
static TO_PEER_CHANNEL_CAPACITY: usize = 2000;
static TO_PEER_CANCEL_CHANNEL_CAPACITY: usize = 1000;
static PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY: usize = 50000;
static REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
static MIN_CHOKE_TIME: Duration = Duration::from_secs(60);
static NEW_CONNECTION_COOL_OFF_PERIOD: Duration = Duration::from_secs(180);

pub struct Peer {
    am_choking: bool,
    am_choking_since: SystemTime,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    haves: Vec<bool>,
    to_peer_tx: Sender<ToPeerMsg>,
    last_sent: SystemTime,
    to_peer_cancel_tx: Sender<ToPeerCancelMsg>,
    outstanding_block_requests: HashMap<(u32, u32, u32), SystemTime>, // (piece idx, block begin, data len) -> request time
    requested_pieces: HashMap<usize, Piece>, // piece idx -> piece status with all the requested fragments
}

impl Peer {
    pub fn new(
        num_pieces: usize,
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
            haves: vec![false; num_pieces],
            to_peer_tx,
            last_sent: now,
            to_peer_cancel_tx,
            outstanding_block_requests: HashMap::new(),
            requested_pieces: HashMap::new(),
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
}

pub struct TorrentManager {
    file_manager: FileManager,
    tracker_client: Arc<Mutex<TrackerClient>>,
    last_tracker_request_time: SystemTime,
    info_hash: [u8; 20],
    own_peer_id: String,
    listening_torrent_wire_protocol_port: u16,
    peers: HashMap<PeerAddr, Peer>,
    advertised_peers: Arc<Mutex<HashMap<PeerAddr, (tracker::Peer, SystemTime)>>>, // peer addr -> (peer, last connection attempt)
    bad_peers: HashSet<PeerAddr>,
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
}

impl TorrentManager {
    pub fn new(
        base_path: &Path,
        listening_torrent_wire_protocol_port: u16,
        listening_dht_port: u16,
        metainfo: Metainfo,
    ) -> Self {
        let own_peer_id = generate_peer_id();
        TorrentManager {
            file_manager: FileManager::new(
                base_path,
                metainfo.get_files(),
                metainfo.piece_length as u64,
                metainfo.pieces,
            ),
            tracker_client: Arc::new(Mutex::new(TrackerClient::new(
                own_peer_id.clone(),
                metainfo.announce_list.clone(),
                listening_torrent_wire_protocol_port,
            ))),
            last_tracker_request_time: SystemTime::UNIX_EPOCH,
            info_hash: metainfo.info_hash,
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
            dht_nodes: metainfo.nodes,
            last_get_peers_requested_time: SystemTime::UNIX_EPOCH,
        }
    }

    pub async fn start(&mut self) {
        self.file_manager.refresh_completed_pieces();
        self.file_manager.refresh_completed_files();
        self.file_manager.log_file_completion_stats();

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
            self.file_manager.piece_completion_status.clone(),
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
                            self.peer_error(peer_addr, error_type, ok_to_accept_connection_tx.clone()).await;
                        }
                        PeersToManagerMsg::Receive(peer_addr, msg) => {
                            self.receive(peer_addr, msg, piece_completion_status_channel_tx.clone(), peers_to_torrent_manager_tx.capacity(), to_dht_manager_tx.clone()).await;
                        }
                        PeersToManagerMsg::NewPeer(tcp_stream) => {
                            self.new_peer(tcp_stream, peers_to_torrent_manager_tx.clone(), ok_to_accept_connection_tx.clone(), to_dht_manager_tx.clone()).await;
                        }
                    }
                }
                Some(()) = tick_rx.recv() => {
                    self.tick(peers_to_torrent_manager_tx.clone(), to_dht_manager_tx.clone()).await;
                }

                Some(DhtToTorrentManagerMsg::NewPeer(ip, port)) = dht_to_torrent_manager_rx.recv() => {
                    let p = tracker::Peer{peer_id: None, ip: ip.to_string(), port: port};
                    let mut advertised_peers_mg = self.advertised_peers.lock().unwrap();
                    advertised_peers_mg.insert(format!("{}:{}", ip, port), (p, SystemTime::UNIX_EPOCH));
                    drop(advertised_peers_mg);
                }
                else => break,
            }
        }
    }

    async fn receive(
        &mut self,
        peer_addr: String,
        msg: Message,
        piece_completion_status_tx: Sender<Vec<bool>>,
        peers_to_torrent_manager_channel_capacity: usize,
        to_dht_manager_tx: Sender<ToDhtManagerMsg>,
    ) {
        log::trace!("received message from peer {}: {}", peer_addr, msg);
        let now = SystemTime::now();
        if let Some(peer) = self.peers.get_mut(&peer_addr) {
            match msg {
                Message::KeepAlive => {}
                Message::Choke => {
                    log::debug!(
                        "received choked from peer {} with {} outstandig requests",
                        peer_addr,
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
                Message::Unchoke => {
                    peer.peer_choking = false;
                    // todo: maybe re-compute assignations immediately here instead of waiting tick
                }
                Message::Interested => {
                    peer.peer_interested = true;
                }
                Message::NotInterested => {
                    peer.peer_interested = false;
                }
                Message::Have(piece_idx) => {
                    let pieces = peer.haves.len();
                    if (piece_idx as usize) < pieces {
                        peer.haves[piece_idx as usize] = true;

                        // send interest if needed
                        if !peer.am_interested
                            && !self.file_manager.piece_completion_status[piece_idx as usize]
                        {
                            peer.am_interested = true;
                            peer.send(ToPeerMsg::Send(Message::Interested)).await;
                        }
                    } else {
                        log::warn!(
                            "got message \"have\" {} from peer {} but the torrent have only {} pieces",
                            piece_idx,
                            peer_addr,
                            pieces
                        );
                        // todo: close connection with this bad peer
                    }
                }
                Message::Bitfield(bitfield) => {
                    if bitfield.len() < self.file_manager.num_pieces() {
                        log::warn!(
                            "received wrongly sized bitfield from peer {}: received {} bits but expected {}",
                            peer_addr,
                            bitfield.len(),
                            self.file_manager.num_pieces()
                        );
                        // todo: close connection with this bad peer
                    } else {
                        if let Some(peer) = self.peers.get_mut(&peer_addr) {
                            // bitfield is byte aligned, it could contain more bits than pieces in the torrent
                            peer.haves = bitfield[0..self.file_manager.num_pieces()].to_vec();

                            // check if we need to send interest
                            if !peer.am_interested {
                                for piece_idx in 0..peer.haves.len() {
                                    if !self.file_manager.piece_completion_status[piece_idx]
                                        && peer.haves[piece_idx]
                                    {
                                        peer.am_interested = true;
                                        peer.send(ToPeerMsg::Send(Message::Interested)).await;
                                        break;
                                    }
                                }
                            }

                            log::trace!(
                                "received bitfield from peer {}: it has {}/{} pieces",
                                peer_addr,
                                peer.haves
                                    .iter()
                                    .fold(0, |acc, v| if *v { acc + 1 } else { acc }),
                                peer.haves.len()
                            );
                        }
                    }
                    // todo: maybe re-compute assignations immediately here instead of waiting tick
                }
                Message::Request(piece_idx, begin, lenght) => {
                    if !peer.am_choking {
                        if should_choke(peers_to_torrent_manager_channel_capacity) {
                            peer.send(ToPeerMsg::Send(Message::Choke)).await;
                            peer.am_choking = true;
                            peer.am_choking_since = SystemTime::now();
                        } else {
                            match self.file_manager.read_piece_block(
                                piece_idx as usize,
                                begin as u64,
                                lenght as u64,
                            ) {
                                Err(e) => {
                                    log::error!("error reading block: {}", e);
                                }

                                Ok(data) => {
                                    let data_len = data.len() as u64;
                                    peer.send(ToPeerMsg::Send(Message::Piece(
                                        piece_idx, begin, data,
                                    )))
                                    .await;

                                    // todo: this is really naive, must avoid saturating upload

                                    self.uploaded_bytes += data_len; // todo: we are not keeping track of cancelled pieces
                                }
                            }
                        }
                    }
                }
                Message::Piece(piece_idx, begin, data) => {
                    let data_len = data.len() as u64;
                    match self.file_manager.write_piece_block(
                        piece_idx as usize,
                        data,
                        begin as u64,
                    ) {
                        Ok(piece_completed) => {
                            self.downloaded_bytes += data_len;

                            // remove outstanding request associated with this block
                            peer.outstanding_block_requests.remove(&(
                                piece_idx,
                                begin,
                                data_len as u32,
                            ));
                            if piece_completed {
                                peer.requested_pieces.remove(&(piece_idx as usize));
                                self.outstanding_piece_assigments
                                    .remove(&(piece_idx as usize));

                                if !self.completed_sent_to_tracker && self.file_manager.completed()
                                {
                                    self.completed_sent_to_tracker = true;
                                    self.tracker_request_async(Event::Completed).await;
                                }

                                let _ = piece_completion_status_tx
                                    .send(self.file_manager.piece_completion_status.clone())
                                    .await; // ignore in case of error: it can happen that the channel is closed on the other side if the rx handler loop exited due to network errors and the peer is still lingering in self.peers because the control message about the error is not yet been handled

                                for (_, peer) in self.peers.iter_mut() {
                                    // send have to interested peers
                                    if peer.peer_interested && !peer.haves[piece_idx as usize] {
                                        peer.send(ToPeerMsg::Send(Message::Have(piece_idx))).await;
                                    }
                                    // send not interested if needed
                                    if peer.am_interested {
                                        let mut am_still_interested = false;
                                        for i in 0..peer.haves.len() {
                                            if !self.file_manager.piece_completion_status
                                                [piece_idx as usize]
                                                && peer.haves[i]
                                            {
                                                am_still_interested = true;
                                                break;
                                            }
                                        }
                                        if !am_still_interested {
                                            peer.send(ToPeerMsg::Send(Message::NotInterested))
                                                .await;
                                        }
                                    }
                                }
                            }
                            // todo: maybe re-compute assignations immediately here instead of waiting tick
                        }
                        Err(e) => {
                            log::error!("cannot write block: {}", e);
                        }
                    }
                }
                Message::Cancel(piece_idx, begin, lenght) => {
                    // we try to let the peer message handler know about the cancellation,
                    // but it the buffer is full, we don't care, it means there were no outstunding messages to be sent
                    // and so the cancellation would have not effect
                    let _ = peer
                        .to_peer_cancel_tx
                        .try_send((piece_idx, begin, lenght, now));
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
            }
        }
    }

    async fn peer_error(
        &mut self,
        peer_addr: String,
        error_type: PeerError,
        ok_to_accept_connection_tx: Sender<bool>,
    ) {
        log::debug!("removing errored peer {}", peer_addr);
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

    async fn tick(
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
                    self.file_manager.piece_completion_status.clone(),
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
                self.tracker_request_async(event).await;
            }
        }

        // unchoke peers
        let choking = should_choke(peers_to_torrent_manager_tx.capacity());
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

        // remove requests that have not been fulfilled for some time,
        // most probably they have been silently dropped by the peer even if it is still alive and not choked
        self.remove_stale_requests();

        // ash DHT manager for new peers if we need this
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

        // send piece requests
        self.assign_piece_reqs().await;
    }

    fn remove_stale_requests(&mut self) {
        let now = SystemTime::now();
        for (peer_addr, peer) in self.peers.iter_mut() {
            peer.outstanding_block_requests
                .retain(|(piece_idx, block_begin, data_len), req_time| {
                    if now.duration_since(*req_time).unwrap() < REQUEST_TIMEOUT {
                        return true;
                    } else {
                        log::debug!("removed stale request to peer: {}: (piece idx: {}, block begin: {}, lenght: {})", peer_addr, piece_idx, block_begin, data_len);
                        peer.requested_pieces.remove(&(*piece_idx as usize));
                        self.outstanding_piece_assigments.remove(&(*piece_idx as usize));
                        return false;
                    }
                })
        }
    }

    async fn assign_piece_reqs(&mut self) {
        // send requests for new blocks for pieces currently downloading
        for (piece_idx, peer_addr) in self.outstanding_piece_assigments.iter() {
            let peer = self.peers.get_mut(peer_addr).unwrap();
            send_requests(
                peer,
                *piece_idx,
                peer.requested_pieces.get(&piece_idx).unwrap().clone(),
            )
            .await;
        }

        // assign incomplete pieces if not assigned yet
        for (piece_idx, piece) in self.file_manager.incomplete_pieces.iter() {
            if !self.outstanding_piece_assigments.contains_key(piece_idx) {
                if let Some(peer_addr) = assign_piece(*piece_idx, &self.peers) {
                    let peer = self.peers.get_mut(&peer_addr).unwrap();
                    send_requests(peer, *piece_idx, piece.clone()).await;
                    self.outstanding_piece_assigments
                        .insert(*piece_idx, peer_addr.clone());
                }
            }
        }

        // assign other pieces, in order
        for piece_idx in 0..self.file_manager.num_pieces() {
            if self.outstanding_piece_assigments.len() > MAX_OUTSTANDING_PIECES {
                break;
            }
            if !self.file_manager.piece_completion_status[piece_idx]
                && !self.outstanding_piece_assigments.contains_key(&piece_idx)
            {
                if let Some(peer_addr) = assign_piece(piece_idx, &self.peers) {
                    let peer = self.peers.get_mut(&peer_addr).unwrap();
                    send_requests(
                        peer,
                        piece_idx,
                        Piece::new(self.file_manager.piece_length(piece_idx)),
                    )
                    .await;
                    self.outstanding_piece_assigments
                        .insert(piece_idx, peer_addr.clone());
                }
            }
        }
    }

    async fn tracker_request_async(&mut self, event: Event) {
        self.last_tracker_request_time = SystemTime::now();
        let bytes_left = self.file_manager.bytes_left();
        let info_hash = self.info_hash;
        let uploaded_bytes = self.uploaded_bytes;
        let downloaded_bytes = self.downloaded_bytes;
        let advertised_peers = self.advertised_peers.clone();
        let tracker_client_mg = self.tracker_client.lock().unwrap();
        let tracker_client = tracker_client_mg.clone();
        drop(tracker_client_mg);
        let tracker_client_arc = self.tracker_client.clone();
        tokio::spawn(async move {
            if let Ok((updated_tracker_client, latest_advertised_peers)) = tracker_request(
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

    async fn new_peer(
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
                    "new peer initialization failed because we could not get peer_addr: {}",
                    e
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
                self.file_manager.num_pieces(),
                to_peer_tx,
                to_peer_cancel_tx,
                should_choke(peers_to_torrent_manager_tx.capacity()),
            ),
        );
        log::debug!("new peer initialized: {}", peer_addr);
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
            "left: {}, pieces: {}/{} | Up: {}/s, Down: {}/s (tot.: {}, {}), wasted: {} | peers known: {} (bad: {}), connected: {}, unchoked: {} | pending peers_to_torrent_manager msgs: {}/{}",
            Size::from_bytes(self.file_manager.bytes_left()),
            self.file_manager.completed_pieces(),
            self.file_manager.num_pieces(),
            Size::from_bytes(bandwidth_up).format().with_style(Style::Abbreviated),
            Size::from_bytes(bandwidth_down).format().with_style(Style::Abbreviated),
            Size::from_bytes(self.uploaded_bytes).format().with_style(Style::Abbreviated),
            Size::from_bytes(self.downloaded_bytes).format().with_style(Style::Abbreviated),
            Size::from_bytes(self.file_manager.wasted_bytes).format().with_style(Style::Abbreviated),
            advertised_peers_len,
            self.bad_peers.len(),
            self.peers.len(),
            self.peers.iter()
            .fold(0, |acc, (_,p)| if !p.peer_choking { acc + 1 } else { acc }),
            PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY - peers_to_torrent_manager_channel_capacity,
            PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY,
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

async fn tracker_request(
    mut tracker_client: TrackerClient,
    event: Event,
    bytes_left: u64,
    info_hash: [u8; 20],
    uploaded_bytes: u64,
    downloaded_bytes: u64,
) -> Result<(TrackerClient, Vec<tracker::Peer>), Box<dyn Error + Sync + Send>> {
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
            log::debug!("could not perform request to tracker: {}", e);
            return Err(Box::from(e.to_string()));
        }
        Ok(Response::Failure(msg)) => {
            log::error!("tracker responded with failure: {}", msg);
            return Err(Box::from(msg));
        }
        Ok(Response::Ok(ok_response)) => {
            if let Some(msg) = ok_response.warning_message.clone() {
                log::warn!("tracker sent a warning: {}", msg);
            }
            log::trace!(
                "tracker request succeeded, tracker response:\n{:?}",
                ok_response
            );

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

fn should_choke(peers_to_torrent_manager_channel_pending_msgs: usize) -> bool {
    peers_to_torrent_manager_channel_pending_msgs > PEERS_TO_TORRENT_MANAGER_CHANNEL_CAPACITY / 2
}

fn assign_piece(piece_idx: usize, peers: &HashMap<String, Peer>) -> Option<String> {
    let mut possible_peers = peers
        .iter()
        .filter(|(_, peer)| {
            !peer.peer_choking
                && peer.haves[piece_idx]
                && peer.outstanding_block_requests.len() < MAX_OUTSTANDING_REQUESTS_PER_PEER
        })
        .map(|(peer_addr, peer)| {
            (
                peer_addr,
                peer,
                peer.outstanding_block_requests
                    .keys()
                    .map(|(piece_idx, _, _)| *piece_idx)
                    .collect::<HashSet<u32>>()
                    .len(),
                peer.outstanding_block_requests.len(),
            )
        })
        .collect::<Vec<(&String, &Peer, usize, usize)>>();

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
        return Some(possible_peers[0].0.clone());
    } else {
        return None;
    }
}

async fn send_requests(peer: &mut Peer, piece_idx: usize, mut incomplete_piece: Piece) {
    while peer.outstanding_block_requests.len() < MAX_OUTSTANDING_REQUESTS_PER_PEER {
        if let Some((begin, end)) = incomplete_piece.get_next_fragment(BLOCK_SIZE_B) {
            let request = (piece_idx as u32, begin as u32, (end - begin + 1) as u32);
            peer.send(ToPeerMsg::Send(Message::Request(
                request.0, request.1, request.2,
            )))
            .await;

            peer.outstanding_block_requests
                .insert(request, SystemTime::now());
            incomplete_piece.add_fragment(begin, end);
        } else {
            break;
        }
    }

    peer.requested_pieces
        .insert(piece_idx, incomplete_piece.clone());
}

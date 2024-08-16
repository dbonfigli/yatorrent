use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{error::Error, iter, path::Path};

use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use size::Size;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time;

use crate::peer::{self, PeerAddr, ToManagerMsg, ToPeerCancelMsg, ToPeerMsg};
use crate::piece::Piece;
use crate::tracker;
use crate::wire_protocol::Message;
use crate::{
    file_manager::FileManager,
    metainfo::Metainfo,
    tracker::{Event, Response, TrackerClient},
};

static ENOUGH_PEERS: usize = 100;
static LOW_ENOUGH_PEERS: usize = 80;
static KEEP_ALIVE_FREQ: Duration = Duration::from_secs(90);
static MAX_OUTSTANDING_REQUESTS_PER_PEER: usize = 500;
static MAX_OUTSTANDING_PIECES: usize = 100;
static BLOCK_SIZE_B: u64 = 16384;
static TO_PEER_CHANNEL_CAPACITY: usize = 2000;
static TO_PEER_CANCEL_CHANNEL_CAPACITY: usize = 1000;
static TO_MANAGER_CHANNEL_CAPACITY: usize = 50000;
static REQUEST_TIMEOUT: Duration = Duration::from_secs(45);

pub struct Peer {
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    haves: Vec<bool>,
    to_peer_tx: Sender<ToPeerMsg>,
    last_sent: SystemTime,
    last_received: SystemTime,
    to_peer_cancel_tx: Sender<ToPeerCancelMsg>,
    outstanding_block_requests: HashMap<(u32, u32, u32), SystemTime>,
    requested_pieces: HashMap<usize, Piece>, // piece idx -> piece status with all the requested fragments
}

impl Peer {
    pub fn new(
        num_pieces: usize,
        to_peer_tx: Sender<ToPeerMsg>,
        to_peer_cancel_tx: Sender<ToPeerCancelMsg>,
    ) -> Self {
        let now = SystemTime::now();
        return Peer {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            haves: vec![false; num_pieces],
            to_peer_tx,
            last_sent: now,
            last_received: now,
            to_peer_cancel_tx,
            outstanding_block_requests: HashMap::new(),
            requested_pieces: HashMap::new(),
        };
    }
}

pub struct TorrentManager {
    file_manager: FileManager,
    tracker_client: TrackerClient,
    tracker_id: Arc<Mutex<Option<String>>>,
    info_hash: [u8; 20],
    own_peer_id: String,
    listening_port: i32,
    peers: HashMap<PeerAddr, Peer>,
    advertised_peers: Arc<Mutex<HashMap<PeerAddr, tracker::Peer>>>,
    bad_peers: HashSet<PeerAddr>,
    last_tracker_request_time: Arc<Mutex<SystemTime>>,
    tracker_request_interval: Arc<Mutex<Duration>>,
    uploaded_bytes: u64,
    downloaded_bytes: u64,
    outstanding_piece_assigments: HashMap<usize, String>, // piece idx -> peer_addr
    completed_sent_to_tracker: bool,
}

impl TorrentManager {
    pub async fn new(
        base_path: &Path,
        listening_port: i32,
        metainfo: Metainfo,
    ) -> Result<Self, Box<dyn Error>> {
        let own_peer_id = generate_peer_id();
        Ok(TorrentManager {
            file_manager: FileManager::new(
                base_path,
                metainfo.get_files(),
                metainfo.piece_length as u64,
                metainfo.pieces,
            ),
            tracker_client: TrackerClient::new(
                own_peer_id.clone(),
                metainfo.announce,
                listening_port,
            ),
            tracker_id: Arc::new(Mutex::new(Option::None)),
            info_hash: metainfo.info_hash,
            own_peer_id,
            listening_port,
            peers: HashMap::new(),
            advertised_peers: Arc::new(Mutex::new(HashMap::new())),
            bad_peers: HashSet::new(),
            last_tracker_request_time: Arc::new(Mutex::new(SystemTime::UNIX_EPOCH)),
            tracker_request_interval: Arc::new(Mutex::new(Duration::from_secs(0))),
            uploaded_bytes: 0,
            downloaded_bytes: 0,
            outstanding_piece_assigments: HashMap::new(),
            completed_sent_to_tracker: false,
        })
    }

    pub async fn start(&mut self) {
        self.file_manager.refresh_completed_pieces();
        self.file_manager.refresh_completed_files();
        self.file_manager.log_file_completion_stats();
        match self.tracker_request(Event::Started).await {
            Err(e) => {
                log::error!("could not perform first request to tracker: {}", e);
            }
            Ok(()) => {
                let (ok_to_accept_connection_tx, ok_to_accept_connection_rx) = mpsc::channel(10);
                let (piece_completion_status_tx, piece_completion_status_rx) = mpsc::channel(10);
                let (to_manager_tx, to_manager_rx) =
                    mpsc::channel::<ToManagerMsg>(TO_MANAGER_CHANNEL_CAPACITY);

                let (tick_tx, tick_rx) = mpsc::channel(1);
                start_tick(tick_tx).await;

                peer::run_new_incoming_peers_handler(
                    self.listening_port.clone(),
                    self.info_hash.clone(),
                    self.own_peer_id.clone(),
                    self.file_manager.piece_completion_status.clone(),
                    ok_to_accept_connection_rx,
                    piece_completion_status_rx,
                    to_manager_tx.clone(),
                )
                .await;

                // block forever
                self.control_loop(
                    ok_to_accept_connection_tx.clone(),
                    piece_completion_status_tx.clone(),
                    to_manager_tx,
                    to_manager_rx,
                    tick_rx,
                )
                .await;
            }
        }
    }

    async fn control_loop(
        &mut self,
        ok_to_accept_connection_tx: Sender<bool>,
        piece_completion_status_channel_tx: Sender<Vec<bool>>,
        to_manager_tx: Sender<ToManagerMsg>,
        mut to_manager_rx: Receiver<ToManagerMsg>,
        mut tick_rx: Receiver<()>,
    ) {
        loop {
            tokio::select! {
                Some(msg) = to_manager_rx.recv() => {
                    match msg {
                        ToManagerMsg::Error(peer_addr) => {
                            self.peer_error(peer_addr, ok_to_accept_connection_tx.clone()).await;
                        }
                        ToManagerMsg::Receive(peer_addr, msg) => {
                            self.receive(peer_addr, msg, piece_completion_status_channel_tx.clone()).await;
                        }
                        ToManagerMsg::NewPeer(tcp_stream) => {
                            self.new_peer(tcp_stream,to_manager_tx.clone(), ok_to_accept_connection_tx.clone()).await;
                        }
                    }
                }
                Some(()) = tick_rx.recv() => {
                    self.tick(to_manager_tx.clone()).await;
                }
                else => break,
            }
        }
    }

    async fn receive(
        &mut self,
        peer_addr: String,
        msg: Message,
        piece_completion_status_channel_tx: Sender<Vec<bool>>,
    ) {
        log::debug!("received message from peer {}: {}", peer_addr, msg);
        let now = SystemTime::now();
        if let Some(peer) = self.peers.get_mut(&peer_addr) {
            peer.last_received = now;
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

                    //self.assign_piece_reqs().await; // todo
                }
                Message::Unchoke => {
                    peer.peer_choking = false;
                    //self.assign_piece_reqs().await; // todo
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
                            peer.last_sent = SystemTime::now();
                            if peer.to_peer_tx.capacity() <= 5 {
                                log::warn!(
                                    "before sending interested, to_peer_tx capacity: {}",
                                    peer.to_peer_tx.capacity()
                                );
                            }
                            let _ = peer
                                .to_peer_tx
                                .send(ToPeerMsg::Send(Message::Interested))
                                .await; // ignore in case of error: it can happen that the channel is closed on the other side if the rx handler loop exited due to network errors and the peer is still lingering in self.peers because the control message about the error is not yet been handled

                            // todo: we block if channel is full, not good
                        }

                        //self.assign_piece_reqs().await; // todo
                    } else {
                        log::warn!(
                            "got message have {} from peer {} but the torrent have only {} pieces",
                            piece_idx,
                            peer_addr,
                            pieces
                        );
                        // todo: close connection with this peer
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
                        // todo: close connection with this peer
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
                                        peer.last_sent = SystemTime::now();
                                        if peer.to_peer_tx.capacity() <= 5 {
                                            log::warn!("before sending interested, to_peer_tx capacity: {}", peer.to_peer_tx.capacity());
                                        }
                                        let _ = peer
                                            .to_peer_tx
                                            .send(ToPeerMsg::Send(Message::Interested))
                                            .await; // ignore in case of error: it can happen that the channel is closed on the other side if the rx handler loop exited due to network errors and the peer is still lingering in self.peers because the control message about the error is not yet been handled

                                        // todo: we block if channel is full, not good
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
                    //self.assign_piece_reqs().await; // todo
                }
                Message::Request(piece_idx, begin, lenght) => {
                    // todo: really naive, must avoid saturate bandwidth and reciprocate on upload / download
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

                            if peer.to_peer_tx.capacity() <= 5 {
                                log::warn!(
                                    "before sending piece, to_peer_tx capacity: {}",
                                    peer.to_peer_tx.capacity()
                                );
                            }
                            let _ = peer
                                .to_peer_tx
                                .send(ToPeerMsg::Send(Message::Piece(piece_idx, begin, data)))
                                .await; // ignore in case of error: it can happen that the channel is closed on the other side if the rx handler loop exited due to network errors and the peer is still lingering in self.peers because the control message about the error is not yet been handled

                            // todo: really naive, must avoid saturating upload

                            self.uploaded_bytes += data_len; // todo: we are not keeping track of cancelled pieces
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

                                if self.file_manager.completed() {
                                    let _ = self.tracker_request(Event::Completed).await;
                                }

                                if piece_completion_status_channel_tx.capacity() <= 5 {
                                    log::warn!(
                                        "before sending piece_completion_status_channel_tx, piece_completion_status_channel_tx capacity: {}",
                                        piece_completion_status_channel_tx.capacity()
                                    );
                                }
                                let _ = piece_completion_status_channel_tx
                                    .send(self.file_manager.piece_completion_status.clone())
                                    .await; // ignore in case of error: it can happen that the channel is closed on the other side if the rx handler loop exited due to network errors and the peer is still lingering in self.peers because the control message about the error is not yet been handled

                                // todo: we block if channel is full, not good

                                let now = SystemTime::now();
                                for (_, peer) in self.peers.iter_mut() {
                                    // send have to interested peers
                                    if peer.peer_interested && !peer.haves[piece_idx as usize] {
                                        if peer.to_peer_tx.capacity() <= 5 {
                                            log::warn!(
                                                "before sending have, to_peer_tx capacity: {}",
                                                peer.to_peer_tx.capacity()
                                            );
                                        }
                                        peer.last_sent = now;
                                        let _ = peer
                                            .to_peer_tx
                                            .send(ToPeerMsg::Send(Message::Have(piece_idx)))
                                            .await; // ignore in case of error: it can happen that the channel is closed on the other side if the rx handler loop exited due to network errors and the peer is still lingering in self.peers because the control message about the error is not yet been handled

                                        // todo: we block if channel is full, not good
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
                                            peer.last_sent = now;
                                            if peer.to_peer_tx.capacity() <= 5 {
                                                log::warn!(
                                                    "before sending interested, peer.to_peer_tx capacity: {}",
                                                    peer.to_peer_tx.capacity()
                                                );
                                            }
                                            let _ = peer
                                                .to_peer_tx
                                                .send(ToPeerMsg::Send(Message::NotInterested))
                                                .await; // ignore in case of error: it can happen that the channel is closed on the other side if the rx handler loop exited due to network errors and the peer is still lingering in self.peers because the control message about the error is not yet been handled

                                            // todo: we block if channel is full, not good
                                        }
                                    }
                                }
                            }
                            //self.assign_piece_reqs().await; // todo maybe this is needed to speed up things
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
                Message::Port(_) => {
                    // feature not supported
                }
            }
        }
    }

    async fn peer_error(&mut self, peer_addr: String, ok_to_accept_connection_tx: Sender<bool>) {
        log::debug!("removing errored peer {}", peer_addr);
        if let Some(removed_peer) = self.peers.remove(&peer_addr) {
            for (piece_idx, _) in removed_peer.requested_pieces {
                self.outstanding_piece_assigments
                    .remove(&(piece_idx as usize));
            }
        }
        self.bad_peers.insert(peer_addr);
        if self.peers.len() < ENOUGH_PEERS {
            if ok_to_accept_connection_tx.capacity() <= 5 {
                log::warn!(
                    "before sending ok_to_accept_connection_tx, ok_to_accept_connection_tx capacity: {}",
                    ok_to_accept_connection_tx.capacity()
                );
            }
            ok_to_accept_connection_tx.send(true).await.unwrap();
        }
    }

    async fn tick(&mut self, to_manager_tx: Sender<ToManagerMsg>) {
        let advertised_peers_lock = self.advertised_peers.lock().unwrap();
        let advertised_peers_len = advertised_peers_lock.len();
        drop(advertised_peers_lock);
        log::info!(
            "left: {}, pieces: {}/{} | up: {}, down: {} | peers known: {}, connected: {}, unchocked: {} | pending to_manager msgs: {}/{}",
            Size::from_bytes(self.file_manager.bytes_left()),
            self.file_manager.completed_pieces(),
            self.file_manager.num_pieces(),
            Size::from_bytes(self.uploaded_bytes),
            Size::from_bytes(self.downloaded_bytes),
            advertised_peers_len,
            self.peers.len(),
            self.peers.iter()
            .fold(0, |acc, (_,p)| if !p.peer_choking { acc + 1 } else { acc }),
            TO_MANAGER_CHANNEL_CAPACITY - to_manager_tx.capacity(),
            TO_MANAGER_CHANNEL_CAPACITY,
        );

        // connect to new peers
        let current_peers_n = self.peers.len();
        if current_peers_n < LOW_ENOUGH_PEERS {
            let possible_peers_mg = self.advertised_peers.lock().unwrap();
            let possible_peers = possible_peers_mg.clone();
            drop(possible_peers_mg);
            let possible_peers = possible_peers
                .iter()
                .filter(|(k, _)| !self.peers.contains_key(*k))
                .map(|(_, v)| v)
                .collect::<Vec<&tracker::Peer>>();
            let candidates_for_new_connections: Vec<&&tracker::Peer> = possible_peers
                .choose_multiple(&mut rand::thread_rng(), LOW_ENOUGH_PEERS - current_peers_n)
                .collect();
            // todo:
            // * better algorithm to select new peers
            // * avoid selecting bad peers?
            for p in candidates_for_new_connections.iter() {
                tokio::spawn(peer::connect_to_new_peer(
                    p.ip.clone(),
                    p.port,
                    self.info_hash,
                    self.own_peer_id.clone(),
                    self.file_manager.piece_completion_status.clone(),
                    to_manager_tx.clone(),
                ));
            }
        }

        // send keep-alives
        let now = SystemTime::now();
        for (_, peer) in self.peers.iter_mut() {
            if let Ok(elapsed) = now.duration_since(peer.last_sent) {
                if elapsed > KEEP_ALIVE_FREQ {
                    if peer.to_peer_tx.capacity() <= 5 {
                        log::warn!(
                            "before sending keep-alive, to_peer_tx capacity: {}",
                            peer.to_peer_tx.capacity()
                        );
                    }
                    let _ = peer
                        .to_peer_tx
                        .send(ToPeerMsg::Send(Message::KeepAlive))
                        .await; // ignore in case of error: it can happen that the channel is closed on the other side if the rx handler loop exited due to network errors and the peer is still lingering in self.peers because the control message about the error is not yet been handled

                    // todo: we block if channel is full, not good
                    peer.last_sent = now;
                }
            }
        }

        // send status to tracker
        let last_tracker_request_time_mg = self.last_tracker_request_time.lock().unwrap();
        let last_tracker_request_time = last_tracker_request_time_mg.clone();
        drop(last_tracker_request_time_mg);
        if let Ok(elapsed) = now.duration_since(last_tracker_request_time) {
            let tracker_request_interval_mg = self.tracker_request_interval.lock().unwrap();
            let tracker_request_interval = tracker_request_interval_mg.clone();
            drop(tracker_request_interval_mg);
            if elapsed > tracker_request_interval {
                self.tracker_request_async().await;
            }
        }

        // remove requests that have not been fulfilled for some time,
        // most probably they have been silently dropped by the peer even if it is still alive and not choked
        self.remove_stale_requests();

        // send piece requests
        self.assign_piece_reqs().await;
    }

    fn remove_stale_requests(&mut self) {
        let now = SystemTime::now();
        for (peer_addr, peer) in self.peers.iter_mut() {
            peer.outstanding_block_requests
                .retain(|req, req_time| {
                    if now.duration_since(*req_time).unwrap() < REQUEST_TIMEOUT {
                        return true;
                    } else {
                        log::debug!("removed stale request to peer: {}: (piece idx: {}, block begin: {}, lenght: {})", peer_addr, req.0, req.1, req.2);
                        return false;
                    }
                })
        }
    }

    async fn assign_piece_reqs(&mut self) {
        // send requests for new blocks for pieces currently downloading
        for (piece_idx, peer_addr) in self.outstanding_piece_assigments.iter() {
            if let Some(peer) = self.peers.get_mut(peer_addr) {
                file_requests(peer, *piece_idx, None).await;
            }
        }

        // assign incomplete pieces if not assigned yet
        for (piece_idx, piece) in self.file_manager.incomplete_pieces.iter() {
            if !self.outstanding_piece_assigments.contains_key(piece_idx) {
                if let Some(peer_addr) = assign_piece(*piece_idx, &self.peers) {
                    let peer = self.peers.get_mut(&peer_addr).unwrap();
                    file_requests(peer, *piece_idx, Some(piece.clone())).await;
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
                    file_requests(
                        peer,
                        piece_idx,
                        Some(Piece::new(self.file_manager.piece_length(piece_idx))),
                    )
                    .await;
                    self.outstanding_piece_assigments
                        .insert(piece_idx, peer_addr.clone());
                }
            }
        }
    }

    async fn tracker_request(&mut self, event: Event) -> Result<(), Box<dyn Error + Sync + Send>> {
        let tracker_id_mg = self.tracker_id.lock().unwrap();
        self.tracker_client.tracker_id = tracker_id_mg.clone();
        drop(tracker_id_mg);
        tracker_request(
            event,
            self.file_manager.bytes_left(),
            self.info_hash,
            self.uploaded_bytes,
            self.downloaded_bytes,
            self.tracker_client.clone(),
            self.advertised_peers.clone(),
            self.tracker_request_interval.clone(),
            self.last_tracker_request_time.clone(),
            self.tracker_id.clone(),
        )
        .await
    }

    async fn tracker_request_async(&mut self) {
        let mut event = Event::None;
        if !self.completed_sent_to_tracker && self.file_manager.completed() {
            self.completed_sent_to_tracker = true;
            event = Event::Completed
        }
        let tracker_id_mg = self.tracker_id.lock().unwrap();
        self.tracker_client.tracker_id = tracker_id_mg.clone();
        drop(tracker_id_mg);
        tokio::spawn(tracker_request(
            event,
            self.file_manager.bytes_left(),
            self.info_hash,
            self.uploaded_bytes,
            self.downloaded_bytes,
            self.tracker_client.clone(),
            self.advertised_peers.clone(),
            self.tracker_request_interval.clone(),
            self.last_tracker_request_time.clone(),
            self.tracker_id.clone(),
        ));
    }

    async fn new_peer(
        &mut self,
        tcp_stream: TcpStream,
        to_manager_tx: Sender<ToManagerMsg>,
        ok_to_accept_connection_tx: Sender<bool>,
    ) {
        let peer_addr = match tcp_stream.peer_addr() {
            Ok(s) => s.to_string(),
            Err(e) => {
                log::trace!(
                    "new peer initialization failed because we could not get peer_addr: {}",
                    e
                );
                return;
            }
        };
        log::debug!("got message with new peer: {}", peer_addr);
        let (to_peer_tx, to_peer_rx) = mpsc::channel(TO_PEER_CHANNEL_CAPACITY);
        let (to_peer_cancel_tx, to_peer_cancel_rx) = mpsc::channel(TO_PEER_CANCEL_CHANNEL_CAPACITY);
        peer::start_peer_msg_handlers(
            peer_addr.clone(),
            tcp_stream,
            to_manager_tx.clone(),
            to_peer_rx,
            to_peer_cancel_rx,
        )
        .await;
        self.peers.insert(
            peer_addr,
            Peer::new(
                self.file_manager.num_pieces(),
                to_peer_tx,
                to_peer_cancel_tx,
            ),
        );
        if self.peers.len() > ENOUGH_PEERS {
            log::trace!("stop accepting new peers");
            if ok_to_accept_connection_tx.capacity() <= 5 {
                log::warn!(
                    "before sending ok_to_accept_connection_tx, ok_to_accept_connection_tx capacity: {}",
                    ok_to_accept_connection_tx.capacity()
                );
            }
            ok_to_accept_connection_tx.send(false).await.unwrap();
        }
    }
}

async fn start_tick(tick_tx: Sender<()>) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            tick_tx.send(()).await.unwrap();
        }
    });
}

fn generate_peer_id() -> String {
    const CHARSET: &[u8] = b"0123456789";
    let mut rng = rand::thread_rng();
    let one_char = || CHARSET[rng.gen_range(0..CHARSET.len())] as char;
    let random_string: String = iter::repeat_with(one_char).take(12).collect();
    format!("-YT0001-{random_string}")
}

async fn tracker_request(
    event: Event,
    bytes_left: u64,
    info_hash: [u8; 20],
    uploaded_bytes: u64,
    downloaded_bytes: u64,
    tracker_client: TrackerClient,
    advertised_peers: Arc<Mutex<HashMap<PeerAddr, tracker::Peer>>>,
    tracker_request_interval: Arc<Mutex<Duration>>,
    last_tracker_request_time: Arc<Mutex<SystemTime>>,
    tracker_id: Arc<Mutex<Option<String>>>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
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
            log::error!("could not perform request to tracker: {}", e);
            return Err(Box::from(e.to_string()));
        }
        Ok(Response::Failure(msg)) => {
            log::error!("tracker responded with failure: {}", msg);
            return Err(Box::from(msg));
        }
        Ok(Response::Ok(ok_response)) => {
            if let Some(msg) = ok_response.warning_message.clone() {
                log::warn!("tracker send a warning: {}", msg);
            }
            log::trace!(
                "tracker request succeeded, tracker response:\n{:?}",
                ok_response
            );

            let mut advertised_peers = advertised_peers.lock().unwrap();
            ok_response.peers.iter().for_each(|p| {
                advertised_peers.insert(format!("{}:{}", p.ip, p.port), p.clone());
            });
            drop(advertised_peers);

            let mut last_tracker_request_time = last_tracker_request_time.lock().unwrap();
            *last_tracker_request_time = SystemTime::now();
            drop(last_tracker_request_time);

            let mut tracker_request_interval = tracker_request_interval.lock().unwrap();
            *tracker_request_interval = Duration::from_secs(ok_response.interval as u64);
            drop(tracker_request_interval);

            if let Some(id) = ok_response.tracker_id {
                let mut tracker_id = tracker_id.lock().unwrap();
                *tracker_id = Some(id);
                drop(tracker_id);
            }

            Ok(())
        }
    }
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

    possible_peers.shuffle(&mut thread_rng());

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

async fn file_requests(peer: &mut Peer, piece_idx: usize, incomplete_piece: Option<Piece>) {
    let mut piece: Piece;
    if peer.requested_pieces.contains_key(&piece_idx) {
        piece = peer.requested_pieces.get(&piece_idx).unwrap().clone();
    } else {
        piece = incomplete_piece.unwrap(); // todo: re-check if this can never panic
    }

    while peer.outstanding_block_requests.len() < MAX_OUTSTANDING_REQUESTS_PER_PEER {
        if let Some((begin, end)) = piece.get_next_fragment(BLOCK_SIZE_B) {
            let request = (piece_idx as u32, begin as u32, (end - begin + 1) as u32);
            if peer.to_peer_tx.capacity() <= 5 {
                log::warn!(
                    "before seding request, to_peer_tx capacity: {}",
                    peer.to_peer_tx.capacity()
                );
            }
            let _ = peer
                .to_peer_tx
                .send(ToPeerMsg::Send(Message::Request(
                    request.0, request.1, request.2,
                )))
                .await; // ignore in case of error: it can happen that the channel is closed on the other side if the rx handler loop exited due to network errors and the peer is still lingering in self.peers because the control message about the error is not yet been handled

            // todo: we block if channel is full, not good
            peer.outstanding_block_requests
                .insert(request, SystemTime::now());
            piece.add_fragment(begin, end);
        } else {
            break;
        }
    }

    peer.requested_pieces.insert(piece_idx, piece.clone());
}

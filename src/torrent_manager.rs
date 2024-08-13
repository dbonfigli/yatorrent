use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{error::Error, iter, path::Path};

use rand::seq::SliceRandom;
use rand::Rng;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time;

use crate::peer::{self, PeerAddr, ToManagerMsg, ToPeerCancelMsg, ToPeerMsg};
use crate::tracker;
use crate::wire_protocol::Message;
use crate::{
    file_manager::FileManager,
    metainfo::Metainfo,
    tracker::{Event, Response, TrackerClient},
};

static ENOUGH_PEERS: usize = 55;
static LOW_ENOUGH_PEERS: usize = 55;
static KEEP_ALIVE_FREQ: Duration = Duration::from_secs(90);

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
                let (to_manager_tx, to_manager_rx) = mpsc::channel::<ToManagerMsg>(1000);

                start_tick(to_manager_tx.clone()).await;

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
    ) {
        while let Some(msg) = to_manager_rx.recv().await {
            match msg {
                ToManagerMsg::Error(peer_addr) => {
                    self.peer_error(peer_addr, ok_to_accept_connection_tx.clone())
                        .await;
                }
                ToManagerMsg::Receive(peer_addr, msg) => {
                    self.receive(peer_addr, msg, piece_completion_status_channel_tx.clone())
                        .await;
                }
                ToManagerMsg::Tick => {
                    self.tick(to_manager_tx.clone()).await;
                }
                ToManagerMsg::NewPeer(tcp_stream) => {
                    self.new_peer(
                        tcp_stream,
                        to_manager_tx.clone(),
                        ok_to_accept_connection_tx.clone(),
                    )
                    .await;
                }
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
                    peer.peer_choking = true;
                }
                Message::Unchoke => {
                    peer.peer_choking = false;
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
                            peer.to_peer_tx
                                .send(ToPeerMsg::Send(Message::Interested))
                                .await
                                .unwrap();
                        }
                    } else {
                        log::debug!(
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
                        log::debug!(
                            "received wrongly sized bitfield from peer {}: received {} bits but expected {}",
                            peer_addr,
                            bitfield.len(),
                            self.file_manager.num_pieces()
                        );
                        // todo: cut connection with this peer
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
                                        peer.to_peer_tx
                                            .send(ToPeerMsg::Send(Message::Interested))
                                            .await
                                            .unwrap();
                                        break;
                                    }
                                }
                            }

                            log::debug!(
                                "received bitfield from peer {}: it has {}/{} pieces",
                                peer_addr,
                                peer.haves
                                    .iter()
                                    .fold(0, |acc, v| if *v { acc + 1 } else { acc }),
                                peer.haves.len()
                            );
                        }
                    }
                }
                Message::Request(piece_idx, begin, lenght) => {
                    // todo
                    // remember to update uploaded_bytes (todo: we are not keeping track of cancelled pieces)
                }
                Message::Piece(piece_idx, begin, data) => {
                    let data_len = data.len() as u64;
                    match self.file_manager.write_piece_block(
                        piece_idx as usize,
                        data,
                        begin as u64,
                    ) {
                        Ok(false) => {
                            self.downloaded_bytes += data_len;
                        }
                        Ok(true) => {
                            self.downloaded_bytes += data_len;
                            if self.file_manager.completed() {
                                log::info!("torrent download complete");
                                let _ = self.tracker_request(Event::Completed).await;
                            }

                            piece_completion_status_channel_tx
                                .send(self.file_manager.piece_completion_status.clone())
                                .await
                                .unwrap();

                            let now = SystemTime::now();
                            for (_, peer) in self.peers.iter_mut() {
                                // send have to interested peers
                                if peer.peer_interested && !peer.haves[piece_idx as usize] {
                                    peer.last_sent = now;
                                    peer.to_peer_tx
                                        .send(ToPeerMsg::Send(Message::Have(piece_idx)))
                                        .await
                                        .unwrap();
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
                                        peer.to_peer_tx
                                            .send(ToPeerMsg::Send(Message::NotInterested))
                                            .await
                                            .unwrap();
                                    }
                                }
                            }
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
        self.peers.remove(&peer_addr);
        self.bad_peers.insert(peer_addr);
        log::debug!("total current peers: {}", self.peers.len());
        if self.peers.len() < ENOUGH_PEERS {
            ok_to_accept_connection_tx.send(true).await.unwrap();
        }
    }

    async fn tick(&mut self, to_manager_tx: Sender<ToManagerMsg>) {
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
                    peer.to_peer_tx
                        .send(ToPeerMsg::Send(Message::KeepAlive))
                        .await
                        .unwrap();
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
                self.tracker_request_async(Event::None).await;
            }
        }

        log::info!("connected peers: {}", self.peers.len());

        // todo: send piece requests
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

    async fn tracker_request_async(&mut self, event: Event) {
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
        let peer_addr = tcp_stream.peer_addr().unwrap().to_string();
        log::debug!("got message with new peer: {}", peer_addr);
        let (to_peer_tx, to_peer_rx) = mpsc::channel(1000);
        let (to_peer_cancel_tx, to_peer_cancel_rx) = mpsc::channel(1000);
        peer::start_peer_msg_handlers(
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
        log::debug!("total current peers: {}", self.peers.len());
        if self.peers.len() > ENOUGH_PEERS {
            log::debug!("stop accepting new peers");
            ok_to_accept_connection_tx.send(false).await.unwrap();
        }
    }
}

async fn start_tick(to_manager_tx: Sender<ToManagerMsg>) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            to_manager_tx.send(ToManagerMsg::Tick).await.unwrap();
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
    // todo: need timeout here?
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
            log::debug!(
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

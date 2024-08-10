use std::collections::{HashMap, HashSet};
use std::time::Duration;
use std::{error::Error, iter, path::Path};

use rand::seq::SliceRandom;
use rand::Rng;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time;

use crate::peer::{self, PeerAddr, ToManagerMsg, ToPeerMsg};
use crate::tracker;
use crate::{
    file_manager::FileManager,
    metainfo::Metainfo,
    tracker::{Event, Response, TrackerClient},
};

static MAX_PEERS: usize = 55;

pub struct Peer {
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    haves: Vec<bool>,
    to_peer_tx: Sender<ToPeerMsg>,
}

impl Peer {
    pub fn new(num_pieces: usize, to_peer_tx: Sender<ToPeerMsg>) -> Self {
        return Peer {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            haves: vec![false; num_pieces],
            to_peer_tx,
        };
    }
}

pub struct TorrentManager {
    file_manager: FileManager,
    tracker_client: TrackerClient,
    info_hash: [u8; 20],
    own_peer_id: String,
    listening_port: i32,
    peers: HashMap<PeerAddr, Peer>,
    advertised_peers: Vec<tracker::Peer>,
    bad_peers: HashSet<PeerAddr>,
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
            info_hash: metainfo.info_hash,
            own_peer_id,
            listening_port,
            peers: HashMap::new(),
            advertised_peers: Vec::new(),
            bad_peers: HashSet::new(),
        })
    }

    pub async fn start(&mut self) {
        self.file_manager.refresh_completed_pieces();
        self.file_manager.refresh_completed_files();
        self.file_manager.log_file_completion_stats();
        match self
            .tracker_client
            .request(
                self.info_hash,
                0,
                0,
                self.file_manager.bytes_left(),
                Event::Started,
            )
            .await
        {
            Err(e) => {
                log::error!("could not perform first request to tracker: {}", e);
            }
            Ok(Response::Failure(msg)) => {
                log::error!("tracker responded with failure: {}", msg);
            }
            Ok(Response::Ok(ok_response)) => {
                if let Some(msg) = ok_response.warning_message.clone() {
                    log::warn!("tracker send a warning: {}", msg);
                }

                log::info!(
                    "tracker request succeeded, tracker response:\n{:?}",
                    ok_response
                );

                self.advertised_peers = ok_response.peers.clone();

                let (ok_to_accept_connection_tx, ok_to_accept_connection_rx) = mpsc::channel(10);
                let (piece_completion_status_tx, piece_completion_status_rx) = mpsc::channel(10);
                let (to_manager_tx, to_manager_rx) = mpsc::channel::<ToManagerMsg>(1000);

                start_tick_channel(to_manager_tx.clone()).await;

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
                    log::debug!("removing errored peer {}", peer_addr);
                    self.peers.remove(&peer_addr);
                    self.bad_peers.insert(peer_addr);
                    log::debug!("total current peers: {}", self.peers.len());
                }
                ToManagerMsg::Receive(_) => {}
                ToManagerMsg::Tick => {
                    self.tick(to_manager_tx.clone()).await;
                }
                ToManagerMsg::NewPeerMessage(tcp_stream) => {
                    self.new_peer(tcp_stream, to_manager_tx.clone()).await;
                }
            }
        }
    }

    async fn tick(&mut self, to_manager_tx: Sender<ToManagerMsg>) {
        let current_peers_n = self.peers.len();
        if current_peers_n < MAX_PEERS {
            let candidates_for_new_connections: Vec<&tracker::Peer> = self
                .advertised_peers
                .choose_multiple(&mut rand::thread_rng(), MAX_PEERS - current_peers_n)
                .collect();
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
    }

    async fn new_peer(&mut self, tcp_stream: TcpStream, to_manager_tx: Sender<ToManagerMsg>) {
        let peer_addr = tcp_stream.peer_addr().unwrap().to_string();
        log::debug!("got message with new peer: {}", peer_addr);
        let (to_peer_tx, to_peer_rx) = mpsc::channel(10);
        peer::start_peer_msg_handlers(tcp_stream, to_manager_tx.clone(), to_peer_rx).await;
        self.peers.insert(
            peer_addr,
            Peer {
                am_choking: true,
                am_interested: false,
                peer_choking: true,
                peer_interested: false,
                haves: vec![false; self.file_manager.num_pieces()],
                to_peer_tx,
            },
        );
        log::debug!("total current peers: {}", self.peers.len());
    }
}

async fn start_tick_channel(to_manager_tx: Sender<ToManagerMsg>) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            to_manager_tx.send(ToManagerMsg::Tick).await;
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

use std::sync::{Arc, Mutex};
use std::{error::Error, iter, path::Path};

use rand::Rng;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::peer::{self, ManagerToPeerMsg, PeerToManagerMsg};
use crate::{
    file_manager::FileManager,
    metainfo::Metainfo,
    tracker::{Event, Response, TrackerClient},
};

pub struct Peer {
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    haves: Vec<bool>,
    manager_to_peer_tx: Sender<ManagerToPeerMsg>,
    peer_to_manager_rx: Receiver<PeerToManagerMsg>,
}

impl Peer {
    pub fn new(
        num_pieces: usize,
        manager_to_peer_tx: Sender<ManagerToPeerMsg>,
        peer_to_manager_rx: Receiver<PeerToManagerMsg>,
    ) -> Self {
        return Peer {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            haves: vec![false; num_pieces],
            manager_to_peer_tx,
            peer_to_manager_rx,
        };
    }
}

pub struct TorrentManager {
    file_manager: FileManager,
    tracker_client: TrackerClient,
    info_hash: [u8; 20],
    own_peer_id: String,
    listening_port: i32,
    peers: Arc<Mutex<Vec<Peer>>>,
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
            peers: Arc::new(Mutex::new(Vec::new())),
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

                let (ok_to_accept_connection_tx, ok_to_accept_connection_rx) = mpsc::channel(10);
                let (piece_completion_status_channel_tx, piece_completion_status_channel_rx) =
                    mpsc::channel(10);
                let (new_peer_channel_tx, new_peer_channel_rx) = mpsc::channel::<TcpStream>(100);

                // new peers handler
                let peers = self.peers.clone();
                let num_pieces = self.file_manager.num_pieces();
                tokio::spawn(run_new_peer_handler(new_peer_channel_rx, peers, num_pieces));

                // connect to several peers
                let peers_from_tracker = ok_response.peers.clone();
                let info_hash = self.info_hash.clone();
                let own_peer_id = self.own_peer_id.clone();
                let piece_completion_status = self.file_manager.piece_completion_status.clone();
                let new_peer_channel_tx_for_connecting = new_peer_channel_tx.clone();
                tokio::spawn(async move {
                    for p in peers_from_tracker.iter() {
                        // todo: reduce this to something like max ~30
                        let own_peer_id = own_peer_id.clone();
                        let piece_completion_status = piece_completion_status.clone();
                        let new_peer_channel_tx_for_connecting =
                            new_peer_channel_tx_for_connecting.clone();
                        let p = p.clone();
                        tokio::spawn(async move {
                            peer::connect_to_new_peer(
                                p.ip.clone(),
                                p.port,
                                info_hash,
                                own_peer_id.clone(),
                                piece_completion_status.clone(),
                                new_peer_channel_tx_for_connecting,
                            )
                            .await;
                        });
                    }
                });

                // incoming peer handler: this will block forever
                peer::run_new_incoming_peers_handler(
                    self.listening_port.clone(),
                    self.info_hash.clone(),
                    self.own_peer_id.clone(),
                    self.file_manager.piece_completion_status.clone(),
                    ok_to_accept_connection_rx,
                    piece_completion_status_channel_rx,
                    new_peer_channel_tx,
                )
                .await;
            }
        }
    }
}

pub async fn run_new_peer_handler(
    mut new_peer_channel_rx: Receiver<TcpStream>,
    peers: Arc<Mutex<Vec<Peer>>>,
    torrent_num_pieces: usize,
) {
    while let Some(msg) = new_peer_channel_rx.recv().await {
        let peer_addr = msg.peer_addr().unwrap().to_string();
        log::debug!("got message with new peer: {}", peer_addr);
        let (manager_to_peer_tx, manager_to_peer_rx) = mpsc::channel(10);
        let (peer_to_manager_tx, peer_to_manager_rx) = mpsc::channel(10);
        peer::start_peer_msg_handlers(msg, peer_to_manager_tx, manager_to_peer_rx).await;
        let mut peers = peers.lock().unwrap();
        peers.push(Peer {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            haves: vec![false; torrent_num_pieces],
            manager_to_peer_tx,
            peer_to_manager_rx,
        });
        log::debug!("total current peers: {}", peers.len());
    }
}

pub fn generate_peer_id() -> String {
    const CHARSET: &[u8] = b"0123456789";
    let mut rng = rand::thread_rng();
    let one_char = || CHARSET[rng.gen_range(0..CHARSET.len())] as char;
    let random_string: String = iter::repeat_with(one_char).take(12).collect();
    format!("-YT0001-{random_string}")
}

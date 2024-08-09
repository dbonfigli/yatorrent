use core::str;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{error::Error, iter, path::Path};

use rand::Rng;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::time::timeout;

use crate::wire_protocol::{ProtocolReadHalf, ProtocolWriteHalf};
use crate::{
    file_manager::FileManager,
    metainfo::{pretty_info_hash, Metainfo},
    tracker::{Event, Response, TrackerClient},
    wire_protocol::{Message, Protocol},
};

static DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
static MAX_CONNECTIONS: usize = 55;

struct Peer {
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

enum ManagerToPeerMsg {
    Send(Message),
}

enum PeerToManagerMsg {
    Error,
    Receive(Message),
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
                            connect_to_new_peer(
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
                run_new_incoming_peers_handler(
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

async fn run_new_peer_handler(
    mut new_peer_channel_rx: Receiver<TcpStream>,
    peers: Arc<Mutex<Vec<Peer>>>,
    torrent_num_pieces: usize,
) {
    while let Some(msg) = new_peer_channel_rx.recv().await {
        let peer_addr = msg.peer_addr().unwrap().to_string();
        log::debug!("got message with new peer: {}", peer_addr);
        let (manager_to_peer_tx, manager_to_peer_rx) = mpsc::channel(10);
        let (peer_to_manager_tx, peer_to_manager_rx) = mpsc::channel(10);
        let peer_to_manager_tx_for_snd_message_handler = peer_to_manager_tx.clone();
        let (read, write) = tokio::io::split(msg);
        tokio::spawn(rcv_message_handler(
            peer_addr.clone(),
            peer_to_manager_tx,
            read,
        ));
        tokio::spawn(snd_message_handler(
            peer_addr.clone(),
            manager_to_peer_rx,
            peer_to_manager_tx_for_snd_message_handler,
            write,
        ));
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

async fn rcv_message_handler<T: ProtocolReadHalf + 'static>(
    peer_addr: String,
    peer_to_manager_tx: Sender<PeerToManagerMsg>,
    mut wire_proto: T,
) {
    loop {
        match timeout(Duration::from_secs(180), wire_proto.receive()).await {
            Err(_elapsed) => {
                log::debug!(
                    "did not receive anything (not even keep-alive messages) from peer in 3 minutes {}",
                    peer_addr
                );
                peer_to_manager_tx
                    .send(PeerToManagerMsg::Error)
                    .await
                    .unwrap();
            }
            Ok(Err(e)) => {
                log::debug!("receive failed with peer {}: {}", peer_addr, e);
                peer_to_manager_tx
                    .send(PeerToManagerMsg::Error)
                    .await
                    .unwrap();
            }
            Ok(Ok(proto_msg)) => {
                log::debug!("received from {}: {}", peer_addr, proto_msg);
                peer_to_manager_tx
                    .send(PeerToManagerMsg::Receive(proto_msg))
                    .await
                    .unwrap();
            }
        }
    }
}

async fn snd_message_handler<T: ProtocolWriteHalf + 'static>(
    peer_addr: String,
    mut manager_to_peer_rx: Receiver<ManagerToPeerMsg>,
    peer_to_manager_tx: Sender<PeerToManagerMsg>,
    mut wire_proto: T,
) {
    while let Some(manager_msg) = manager_to_peer_rx.recv().await {
        match manager_msg {
            ManagerToPeerMsg::Send(proto_msg) => {
                match timeout(DEFAULT_TIMEOUT, wire_proto.send(proto_msg)).await {
                    Err(_elapsed) => {
                        log::debug!("timeout sending message to peer {}", peer_addr);
                        peer_to_manager_tx
                            .send(PeerToManagerMsg::Error)
                            .await
                            .unwrap();
                    }
                    Ok(Err(e)) => {
                        log::debug!("sending failed with peer {}: {}", peer_addr, e);
                        peer_to_manager_tx
                            .send(PeerToManagerMsg::Error)
                            .await
                            .unwrap();
                    }
                    Ok(Ok(_)) => {}
                }
            }
        }
    }
}

pub fn generate_peer_id() -> String {
    const CHARSET: &[u8] = b"0123456789";
    let mut rng = rand::thread_rng();
    let one_char = || CHARSET[rng.gen_range(0..CHARSET.len())] as char;
    let random_string: String = iter::repeat_with(one_char).take(12).collect();
    format!("-YT0001-{random_string}")
}

async fn handshake(
    mut stream: TcpStream,
    info_hash: [u8; 20],
    own_peer_id: String,
    piece_completion_status: Vec<bool>,
) -> Result<TcpStream, Box<dyn Error + Send + Sync>> {
    let (peer_protocol, _reserved, peer_info_hash, peer_id) = stream
        .handshake(info_hash, own_peer_id.as_bytes().try_into()?)
        .await?;
    log::debug!(
        "received handshake info from {}: peer protocol: {}, info_hash: {}, peer_id: {}",
        stream.peer_addr().unwrap(),
        peer_protocol,
        pretty_info_hash(peer_info_hash),
        str::from_utf8(&peer_id)?,
    );
    if peer_info_hash != info_hash {
        log::warn!("info hash received during handshake does not match to the one we want (own: {}, theirs: {}), aborting connection", pretty_info_hash(info_hash), pretty_info_hash(peer_info_hash));
        return Err(Box::from("own and their infohash did not match"));
    }

    // send bitfield
    let peer_addr = stream.peer_addr().unwrap();
    let (read, mut write) = tokio::io::split(stream);
    write
        .send(Message::Bitfield(piece_completion_status))
        .await?;
    log::debug!("bitfield sent to peer {}", peer_addr);
    let stream = read.unsplit(write);

    // handshake completed successfully
    Ok(stream)
}

// this will never return
async fn run_new_incoming_peers_handler(
    listening_port: i32,
    info_hash: [u8; 20],
    own_peer_id: String,
    piece_completion_status: Vec<bool>,
    mut ok_to_accept_connection_rx: Receiver<bool>,
    mut piece_completion_status_rx: Receiver<Vec<bool>>,
    new_peer_tx: Sender<TcpStream>,
) {
    let ok_to_accept_connection_for_rcv: Arc<Mutex<bool>> = Arc::new(Mutex::new(true)); // accept new connections at start
    let ok_to_accept_connection = ok_to_accept_connection_for_rcv.clone();
    tokio::spawn(async move {
        while let Some(msg) = ok_to_accept_connection_rx.recv().await {
            log::debug!(
                "got message to accept/refuse new incoming connections: {}",
                msg
            );
            *ok_to_accept_connection_for_rcv.lock().unwrap() = msg;
        }
    });

    let piece_completion_status_for_rcv: Arc<Mutex<Vec<bool>>> =
        Arc::new(Mutex::new(piece_completion_status));
    let piece_completion_status = piece_completion_status_for_rcv.clone();
    tokio::spawn(async move {
        while let Some(msg) = piece_completion_status_rx.recv().await {
            log::debug!("got message to update piece_completion_status");
            *piece_completion_status_for_rcv.lock().unwrap() = msg;
        }
    });

    let incoming_connection_listener = TcpListener::bind(format!("0.0.0.0:{}", listening_port))
        .await
        .unwrap();

    loop {
        log::debug!("waiting for incoming peer connections...");
        let (mut stream, _) = incoming_connection_listener.accept().await.unwrap(); // never timeout here, wait forever if needed
        if !*ok_to_accept_connection.lock().unwrap() {
            log::debug!(
                "reached limit of incoming connections, shutting down new connection from: {}",
                stream.peer_addr().unwrap()
            );
            _ = stream.shutdown().await;
            continue;
        }

        let piece_completion_status_for_spawn = piece_completion_status.clone();
        let own_peer_id_for_spawn = own_peer_id.clone();
        let new_peer_tx_for_spawn = new_peer_tx.clone();
        tokio::spawn(async move {
            let pcs = piece_completion_status_for_spawn.lock().unwrap().clone();
            let remote_addr = stream.peer_addr().unwrap();
            match timeout(
                DEFAULT_TIMEOUT,
                handshake(stream, info_hash, own_peer_id_for_spawn, pcs),
            )
            .await
            {
                Err(_elapsed) => {
                    log::debug!("handshake timeout with peer {}", remote_addr);
                }
                Ok(Err(e)) => {
                    log::debug!("handshake failed with peer {}: {}", remote_addr, e);
                }
                Ok(Ok(tcp_stream)) => {
                    new_peer_tx_for_spawn.send(tcp_stream).await.unwrap();
                }
            }
        });
    }
}

async fn connect_to_new_peer(
    host: String,
    port: u32,
    info_hash: [u8; 20],
    own_peer_id: String,
    piece_completion_status: Vec<bool>,
    new_peer_tx: Sender<TcpStream>,
) {
    let dest = format!("{}:{}", host, port);
    log::debug!("initiating connection to peer: {}", dest);
    match timeout(DEFAULT_TIMEOUT, TcpStream::connect(dest.clone())).await {
        Err(_elapsed) => {
            log::debug!("timed out connecting to peer {}", dest);
        }
        Ok(Err(e)) => {
            log::debug!("error initiating connection to peer {}: {}", dest, e);
        }
        Ok(Ok(tcp_stream)) => {
            match timeout(
                DEFAULT_TIMEOUT,
                handshake(
                    tcp_stream,
                    info_hash.clone(),
                    own_peer_id.clone(),
                    piece_completion_status.clone(),
                ),
            )
            .await
            {
                Err(_elapsed) => {
                    log::debug!("timed out completing handshake with peer {}", dest);
                }
                Ok(Err(e)) => {
                    log::debug!("error out completing handshake with peer {}", e);
                }
                Ok(Ok(tcp_stream)) => {
                    new_peer_tx.send(tcp_stream).await.unwrap();
                }
            }
        }
    }
}

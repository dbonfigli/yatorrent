use core::str;
use std::sync::Arc;
use std::time::Duration;
use std::{error::Error, iter, path::Path};

use rand::Rng;
use tokio::join;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::timeout;

use crate::{
    file_manager::FileManager,
    metainfo::{pretty_info_hash, Metainfo},
    tracker::{Event, Response, TrackerClient},
    wire_protocol::{Message, Protocol},
};

struct Peer {
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    network_client: TcpStream,
    haves: Vec<bool>,
}

static DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

impl Peer {
    pub fn new(network_client: TcpStream, num_pieces: usize) -> Self {
        return Peer {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            network_client,
            haves: vec![false; num_pieces],
        };
    }
}

pub struct TorrentManager {
    file_manager: FileManager,
    tracker_client: TrackerClient,
    info_hash: [u8; 20],
    own_peer_id: String, //our own peer id
    peers: Arc<Mutex<Vec<Peer>>>,
    incoming_connection_listener: TcpListener,
}

// start accepting peer connections in a loop
// start loop to send tracker request in a loop - and update info like possible peer list
// start management of connections we initiate:
// - manage choke and interest
// - keep track of send/recv info to be used to be sent to tracker
// - send / recv data (16KiB max each req)

// we initiate new connections if they are < 30
// we refuse new connections if we have > 55

impl TorrentManager {
    pub async fn new(
        base_path: &Path,
        listening_port: i32,
        metainfo: Metainfo,
    ) -> Result<Self, Box<dyn Error>> {
        let incoming_connection_listener =
            TcpListener::bind(format!("0.0.0.0:{}", listening_port)).await?;
        let own_peer_id = generate_peer_id();
        Ok(TorrentManager {
            file_manager: FileManager::new(
                base_path,
                metainfo.get_files(),
                metainfo.piece_length,
                metainfo.pieces,
            ),
            tracker_client: TrackerClient::new(
                own_peer_id.clone(),
                metainfo.announce,
                listening_port,
            ),
            info_hash: metainfo.info_hash,
            own_peer_id,
            peers: Arc::new(Mutex::new(Vec::new())),
            incoming_connection_listener,
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
                log::error!("could not read first request from tracker: {}", e);
            }
            Ok(Response::Failure(msg)) => {
                log::error!("tracker responded with failure: {}", msg);
            }
            Ok(Response::Ok(ok_response)) => {
                if let Some(msg) = ok_response.warning_message.clone() {
                    log::warn!("tracker send a warning: {}", msg);
                }
                // todo: read interval and start tracker requests in loop
                // read peers and connect to peers
                // let mut futures = Vec::new();
                // for i in 0..std::cmp::min(10, ok_response.peers.len()) {
                //     futures.push(
                //         self.initiate_peer(
                //             ok_response.peers[i].ip.clone(),
                //             ok_response.peers[i].port,
                //         ),
                //     );
                // }
                // join!(futures[0], futures[1]);

                // let f1 = self
                //     .initiate_peer(ok_response.peers[0].ip.clone(), ok_response.peers[0].port)
                //     .await;
                // let f2 = self
                //     .initiate_peer(ok_response.peers[1].ip.clone(), ok_response.peers[1].port)
                //     .await;
                // println!("{:?} {:?}", f1, f2);

                log::info!(
                    "tracker request succeeded, tracker response:\n{:?}",
                    ok_response
                );

                join!(
                    self.initiate_peer(ok_response.peers[0].ip.clone(), ok_response.peers[0].port),
                    self.initiate_peer(ok_response.peers[1].ip.clone(), ok_response.peers[1].port),
                    self.initiate_peer(ok_response.peers[2].ip.clone(), ok_response.peers[2].port),
                    self.initiate_peer(ok_response.peers[3].ip.clone(), ok_response.peers[3].port)
                );
            }
        }
        // start accepting connections from peers
    }

    pub async fn initiate_peer(&self, host: String, port: u32) -> Result<(), Box<dyn Error>> {
        let dest = format!("{}:{}", host, port);
        log::debug!("connecting to peer: {}", dest);
        let stream = timeout(DEFAULT_TIMEOUT, TcpStream::connect(dest)).await??;
        timeout(DEFAULT_TIMEOUT, self.handshake(stream)).await?
    }

    pub async fn accept_peer(&self) -> Result<(), Box<dyn Error>> {
        log::debug!("waiting for incoming peer connections...");
        let (stream, _) = self.incoming_connection_listener.accept().await?; // never timeout here, wait forever if needed
        timeout(DEFAULT_TIMEOUT, self.handshake(stream)).await?
    }

    async fn handshake(&self, mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
        let (peer_protocol, _reserved, peer_info_hash, peer_id) = stream
            .handshake(self.info_hash, self.own_peer_id.as_bytes().try_into()?)
            .await?;
        log::debug!(
            "received handshake info from {}: peer protocol: {}, info_hash: {}, peer_id: {}",
            stream.peer_addr().unwrap(),
            peer_protocol,
            pretty_info_hash(peer_info_hash),
            str::from_utf8(&peer_id)?,
        );
        if peer_info_hash != self.info_hash {
            log::warn!("info hash received during handshake does not match to the one we want (own: {}, theirs: {}), aborting connection", pretty_info_hash(self.info_hash), pretty_info_hash(peer_info_hash));
            return Err(Box::from("own and their infohash did not match"));
        }

        // send bitfield
        stream
            .send(Message::Bitfield(
                self.file_manager.piece_completion_status.clone(),
            ))
            .await?;
        log::debug!("bitfield sent to peer {}", stream.peer_addr().unwrap());

        // handshake completed successfully
        self.peers
            .lock()
            .await
            .push(Peer::new(stream, self.file_manager.num_pieces()));
        Ok(())
    }
}

pub fn generate_peer_id() -> String {
    const CHARSET: &[u8] = b"0123456789";
    let mut rng = rand::thread_rng();
    let one_char = || CHARSET[rng.gen_range(0..CHARSET.len())] as char;
    let random_string: String = iter::repeat_with(one_char).take(12).collect();
    format!("-YT0001-{random_string}")
}

use core::str;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::time::timeout;

use crate::wire_protocol::{ProtocolReadHalf, ProtocolWriteHalf};
use crate::{
    metainfo::pretty_info_hash,
    wire_protocol::{Message, Protocol},
};

static DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

pub enum ToPeerMsg {
    Send(Message),
}

pub type PeerAddr = String;

pub enum ToManagerMsg {
    Error(PeerAddr),
    Receive(PeerAddr, Message),
    Tick,
    NewPeer(TcpStream),
}

pub async fn connect_to_new_peer(
    host: String,
    port: u32,
    info_hash: [u8; 20],
    own_peer_id: String,
    piece_completion_status: Vec<bool>,
    to_manager_tx: Sender<ToManagerMsg>,
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
                    to_manager_tx
                        .send(ToManagerMsg::NewPeer(tcp_stream))
                        .await
                        .unwrap();
                }
            }
        }
    }
}

pub async fn run_new_incoming_peers_handler(
    listening_port: i32,
    info_hash: [u8; 20],
    own_peer_id: String,
    piece_completion_status: Vec<bool>,
    mut ok_to_accept_connection_rx: Receiver<bool>,
    mut piece_completion_status_rx: Receiver<Vec<bool>>,
    to_manager_tx: Sender<ToManagerMsg>,
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

    tokio::spawn(async move {
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
            let new_peer_tx_for_spawn = to_manager_tx.clone();
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
                        new_peer_tx_for_spawn
                            .send(ToManagerMsg::NewPeer(tcp_stream))
                            .await
                            .unwrap();
                    }
                }
            });
        }
    });
}

pub async fn start_peer_msg_handlers(
    tcp_stream: TcpStream,
    to_manager_tx: Sender<ToManagerMsg>,
    to_peer_rx: Receiver<ToPeerMsg>,
) {
    let peer_addr = tcp_stream.peer_addr().unwrap().to_string();
    let to_manager_tx_for_snd_message_handler = to_manager_tx.clone();
    let (read, write) = tokio::io::split(tcp_stream);
    tokio::spawn(rcv_message_handler(peer_addr.clone(), to_manager_tx, read));
    tokio::spawn(snd_message_handler(
        peer_addr.clone(),
        to_peer_rx,
        to_manager_tx_for_snd_message_handler,
        write,
    ));
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

async fn rcv_message_handler<T: ProtocolReadHalf + 'static>(
    peer_addr: String,
    to_manager_tx: Sender<ToManagerMsg>,
    mut wire_proto: T,
) {
    loop {
        match timeout(Duration::from_secs(180), wire_proto.receive()).await {
            Err(_elapsed) => {
                log::debug!(
                "did not receive anything (not even keep-alive messages) from peer in 3 minutes {}",
                peer_addr
            );
                to_manager_tx
                    .send(ToManagerMsg::Error(peer_addr))
                    .await
                    .unwrap();
                break;
            }
            Ok(Err(e)) => {
                log::debug!("receive failed with peer {}: {}", peer_addr, e);
                to_manager_tx
                    .send(ToManagerMsg::Error(peer_addr))
                    .await
                    .unwrap();
                break;
            }
            Ok(Ok(proto_msg)) => {
                log::debug!("received from {}: {}", peer_addr, proto_msg);
                to_manager_tx
                    .send(ToManagerMsg::Receive(peer_addr.clone(), proto_msg))
                    .await
                    .unwrap();
            }
        }
    }
}

async fn snd_message_handler<T: ProtocolWriteHalf + 'static>(
    peer_addr: String,
    mut to_peer_rx: Receiver<ToPeerMsg>,
    to_manager_tx: Sender<ToManagerMsg>,
    mut wire_proto: T,
) {
    while let Some(manager_msg) = to_peer_rx.recv().await {
        match manager_msg {
            ToPeerMsg::Send(proto_msg) => {
                log::debug!("sending message {} to peer {}", proto_msg, peer_addr);
                match timeout(DEFAULT_TIMEOUT, wire_proto.send(proto_msg)).await {
                    Err(_elapsed) => {
                        log::debug!("timeout sending message to peer {}", peer_addr);
                        to_manager_tx
                            .send(ToManagerMsg::Error(peer_addr))
                            .await
                            .unwrap();
                        break;
                    }
                    Ok(Err(e)) => {
                        log::debug!("sending failed with peer {}: {}", peer_addr, e);
                        to_manager_tx
                            .send(ToManagerMsg::Error(peer_addr))
                            .await
                            .unwrap();
                        break;
                    }
                    Ok(Ok(_)) => {}
                }
            }
        }
    }
}

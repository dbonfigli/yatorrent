use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Result};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::time::timeout;

use crate::bencoding::Value;
use crate::torrent_protocol::wire_protocol::{
    Message, Protocol, ProtocolReadHalf, ProtocolWriteHalf,
};
use crate::util::{force_string, pretty_info_hash};

static DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
static CANCELLATION_DURATION: Duration = Duration::from_secs(120);

pub enum ToPeerMsg {
    Send(Message),
}

pub type PeerAddr = String;

pub enum PeersToManagerMsg {
    Error(PeerAddr, PeerError),
    Receive(PeerAddr, Message),
    NewPeer(TcpStream),
}

#[derive(PartialEq)]
pub enum PeerError {
    HandshakeError,
    Timeout,
    Others,
}

pub type ToPeerCancelMsg = (u32, u32, u32, SystemTime); // piece_idx, begin, lenght, cancel time

pub async fn connect_to_new_peer(
    host: String,
    port: u16,
    info_hash: [u8; 20],
    own_peer_id: String,
    tcp_wire_protocol_listening_port: u16,
    piece_completion_status: Vec<bool>,
    peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
) {
    let dest = format!("{}:{}", host, port);
    log::trace!("initiating connection to peer: {}", dest);
    match timeout(DEFAULT_TIMEOUT, TcpStream::connect(dest.clone())).await {
        Err(_elapsed) => {
            log::trace!("timed out connecting to peer {}", dest);
            send_to_torrent_manager(
                &peers_to_torrent_manager_tx,
                PeersToManagerMsg::Error(format!("{}:{}", host, port), PeerError::HandshakeError),
            )
            .await;
        }
        Ok(Err(e)) => {
            log::trace!("error initiating connection to peer {}: {}", dest, e);
            send_to_torrent_manager(
                &peers_to_torrent_manager_tx,
                PeersToManagerMsg::Error(format!("{}:{}", host, port), PeerError::HandshakeError),
            )
            .await;
        }
        Ok(Ok(tcp_stream)) => {
            let peer_addr = match tcp_stream.peer_addr() {
                Ok(s) => s.to_string(),
                Err(e) => {
                    log::trace!(
                        "connecting to new peer failed because we could not get peer addr: {}",
                        e
                    );
                    send_to_torrent_manager(
                        &peers_to_torrent_manager_tx,
                        PeersToManagerMsg::Error(
                            format!("{}:{}", host, port),
                            PeerError::HandshakeError,
                        ),
                    )
                    .await;
                    return;
                }
            };
            match timeout(
                DEFAULT_TIMEOUT,
                handshake(
                    tcp_stream,
                    info_hash.clone(),
                    own_peer_id.clone(),
                    tcp_wire_protocol_listening_port,
                    Box::from(piece_completion_status),
                ),
            )
            .await
            {
                Err(_elapsed) => {
                    log::trace!("timed out completing handshake with peer {}", dest);
                    send_to_torrent_manager(
                        &peers_to_torrent_manager_tx,
                        PeersToManagerMsg::Error(peer_addr, PeerError::HandshakeError),
                    )
                    .await;
                }
                Ok(Err(e)) => {
                    log::trace!("error out completing handshake with peer {}", e);
                    send_to_torrent_manager(
                        &peers_to_torrent_manager_tx,
                        PeersToManagerMsg::Error(peer_addr, PeerError::HandshakeError),
                    )
                    .await;
                }
                Ok(Ok(tcp_stream)) => {
                    send_to_torrent_manager(
                        &peers_to_torrent_manager_tx,
                        PeersToManagerMsg::NewPeer(tcp_stream),
                    )
                    .await;
                }
            }
        }
    }
}

pub async fn run_new_incoming_peers_handler(
    info_hash: [u8; 20],
    own_peer_id: String,
    tcp_wire_protocol_listening_port: u16,
    piece_completion_status: Vec<bool>,
    mut ok_to_accept_connection_rx: Receiver<bool>,
    mut piece_completion_status_rx: Receiver<Vec<bool>>,
    peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
) {
    let ok_to_accept_connection_for_rcv: Arc<Mutex<bool>> = Arc::new(Mutex::new(true)); // accept new connections at start
    let ok_to_accept_connection = ok_to_accept_connection_for_rcv.clone();
    tokio::spawn(async move {
        while let Some(msg) = ok_to_accept_connection_rx.recv().await {
            log::trace!(
                "got message to accept/refuse new incoming connections: {}",
                msg
            );
            let mut ok_to_accept_connection_for_rcv_lock =
                ok_to_accept_connection_for_rcv.lock().await;
            *ok_to_accept_connection_for_rcv_lock = msg;
            drop(ok_to_accept_connection_for_rcv_lock);
        }
    });

    let piece_completion_status_for_rcv: Arc<Mutex<Vec<bool>>> =
        Arc::new(Mutex::new(piece_completion_status));
    let piece_completion_status = piece_completion_status_for_rcv.clone();
    tokio::spawn(async move {
        while let Some(msg) = piece_completion_status_rx.recv().await {
            log::trace!("got message to update piece_completion_status");
            let mut piece_completion_status_for_rcv_lock =
                piece_completion_status_for_rcv.lock().await;
            *piece_completion_status_for_rcv_lock = msg;
            drop(piece_completion_status_for_rcv_lock);
        }
    });

    let incoming_connection_listener =
        TcpListener::bind(format!("0.0.0.0:{}", tcp_wire_protocol_listening_port))
            .await
            .unwrap();

    tokio::spawn(async move {
        loop {
            log::debug!("waiting for incoming peer connections...");
            let (mut stream, _) = incoming_connection_listener.accept().await.unwrap(); // never timeout here, wait forever if needed
            let ok_to_accept_connection_lock = ok_to_accept_connection.lock().await;
            let ok_to_accept_connection = ok_to_accept_connection_lock.clone();
            drop(ok_to_accept_connection_lock);
            if !ok_to_accept_connection {
                log::trace!(
                    "reached limit of incoming connections, shutting down new connection from: {}",
                    addr_or_unknown(&stream)
                );
                _ = stream.shutdown().await;
                continue;
            }

            let piece_completion_status_for_spawn = piece_completion_status.clone();
            let own_peer_id_for_spawn = own_peer_id.clone();
            let peers_to_torrent_manager_tx_for_spawn = peers_to_torrent_manager_tx.clone();
            tokio::spawn(async move {
                let pcs_lock = piece_completion_status_for_spawn.lock().await;
                let pcs = pcs_lock.clone();
                drop(pcs_lock);
                let remote_addr = addr_or_unknown(&stream);
                match timeout(
                    DEFAULT_TIMEOUT,
                    handshake(
                        stream,
                        info_hash,
                        own_peer_id_for_spawn,
                        tcp_wire_protocol_listening_port,
                        Box::from(pcs),
                    ),
                )
                .await
                {
                    Err(_elapsed) => {
                        log::trace!("handshake timeout with peer {}", remote_addr);
                    }
                    Ok(Err(e)) => {
                        log::trace!("handshake failed with peer {}: {}", remote_addr, e);
                    }
                    Ok(Ok(tcp_stream)) => {
                        send_to_torrent_manager(
                            &peers_to_torrent_manager_tx_for_spawn,
                            PeersToManagerMsg::NewPeer(tcp_stream),
                        )
                        .await;
                    }
                }
            });
        }
    });
}

fn addr_or_unknown(stream: &TcpStream) -> String {
    match stream.peer_addr() {
        Ok(s) => s.to_string(),
        Err(_) => "<unknown>".to_string(),
    }
}

pub fn start_peer_msg_handlers(
    peer_addr: String,
    tcp_stream: TcpStream,
    peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
    to_peer_rx: Receiver<ToPeerMsg>,
    to_peer_cancel_rx: Receiver<ToPeerCancelMsg>,
) {
    let peers_to_torrent_manager_tx_for_snd_message_handler = peers_to_torrent_manager_tx.clone();
    let (read, write) = tokio::io::split(tcp_stream);
    tokio::spawn(rcv_message_handler(
        peer_addr.clone(),
        peers_to_torrent_manager_tx,
        read,
    ));
    tokio::spawn(snd_message_handler(
        peer_addr.clone(),
        to_peer_rx,
        peers_to_torrent_manager_tx_for_snd_message_handler,
        write,
        to_peer_cancel_rx,
    ));
}

async fn handshake(
    mut stream: TcpStream,
    info_hash: [u8; 20],
    own_peer_id: String,
    tcp_wire_protocol_listening_port: u16,
    piece_completion_status: Box<Vec<bool>>,
) -> Result<TcpStream> {
    let (peer_protocol, reserved, peer_info_hash, peer_id) = stream
        .handshake(info_hash, own_peer_id.as_bytes().try_into()?)
        .await?;
    log::trace!(
        "received handshake info from {}: peer protocol: {}, info_hash: {}, peer_id: {}, reserved: {:?}",
        addr_or_unknown(&stream),
        peer_protocol,
        pretty_info_hash(peer_info_hash),
        force_string(&peer_id.to_vec()),
        reserved,
    );
    if peer_info_hash != info_hash {
        log::debug!("handshake errored: info hash received during handshake does not match to the one we want (own: {}, theirs: {})", pretty_info_hash(info_hash), pretty_info_hash(peer_info_hash));
        bail!("own and their infohash did not match");
    }

    // send bitfield
    let peer_addr = addr_or_unknown(&stream);
    let (read, mut write) = tokio::io::split(stream);
    write
        .send(Message::Bitfield(piece_completion_status))
        .await?;
    log::trace!("bitfield sent to peer {}", peer_addr);

    // if peer supports DHT, send port
    if reserved[7] & 1u8 != 0 {
        write
            .send(Message::Port(tcp_wire_protocol_listening_port))
            .await?;
        log::trace!("port sent to peer {}", peer_addr);
    }

    // if peer supports extensions, send PEX support
    if reserved[5] & 0x10 != 0 {
        let extension_handshake = Value::Dict(
            HashMap::from([(
                b"m".to_vec(),
                Value::Dict(
                    HashMap::from([(
                        b"ut_pex".to_vec(),
                        Value::Int(1), // we always register 1 as ut_pex extension since we don't support others ATM
                    )]),
                    0,
                    0,
                ),
            )]),
            0,
            0,
        );
        write
            .send(Message::Extended(0, extension_handshake))
            .await?;
        log::trace!("extension handshake sent to peer {}", peer_addr);
    }

    let stream = read.unsplit(write);

    // handshake completed successfully
    Ok(stream)
}

async fn rcv_message_handler<T: ProtocolReadHalf + 'static>(
    peer_addr: String,
    peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
    mut wire_proto: T,
) {
    loop {
        match timeout(Duration::from_secs(180), wire_proto.receive()).await {
            Err(_elapsed) => {
                log::trace!("did not receive anything (not even keep-alive messages) from peer in 3 minutes {}", peer_addr);
                send_to_torrent_manager(
                    &peers_to_torrent_manager_tx,
                    PeersToManagerMsg::Error(peer_addr, PeerError::Timeout),
                )
                .await;
                break;
            }
            Ok(Err(e)) => {
                log::trace!("receive failed with peer {}: {}", peer_addr, e);
                send_to_torrent_manager(
                    &peers_to_torrent_manager_tx,
                    PeersToManagerMsg::Error(peer_addr, PeerError::Others),
                )
                .await;
                break;
            }
            Ok(Ok(proto_msg)) => {
                log::trace!("received from {}: {}", peer_addr, proto_msg);
                send_to_torrent_manager(
                    &peers_to_torrent_manager_tx,
                    PeersToManagerMsg::Receive(peer_addr.clone(), proto_msg),
                )
                .await;
            }
        }
    }
}

async fn snd_message_handler<T: ProtocolWriteHalf + 'static>(
    peer_addr: String,
    mut to_peer_rx: Receiver<ToPeerMsg>,
    peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
    mut wire_proto: T,
    mut to_peer_cancel_rx: Receiver<ToPeerCancelMsg>,
) {
    let mut cancellations = HashMap::<(u32, u32, u32), SystemTime>::new();
    while let Some(manager_msg) = to_peer_rx.recv().await {
        match manager_msg {
            ToPeerMsg::Send(proto_msg) => {
                // avoid sending data if the request has already been canceled by the peer
                if let Message::Piece(piece_idx, begin, data) = &proto_msg {
                    // receive pending cancellations
                    while let Ok((piece_idx, begin, lenght, cancel_time)) =
                        to_peer_cancel_rx.try_recv()
                    {
                        cancellations.insert((piece_idx, begin, lenght), cancel_time);
                    }
                    // remove expired cancellations
                    let cancellations_keys: Vec<(u32, u32, u32)> =
                        cancellations.keys().map(|k| k.clone()).collect();
                    for k in cancellations_keys {
                        let expired_at = *cancellations.get(&k).expect("not possible");
                        if let Ok(elapsed) = SystemTime::now().duration_since(expired_at) {
                            if elapsed > CANCELLATION_DURATION {
                                cancellations.remove(&k);
                            }
                        }
                    }
                    // avoid sending if there is a cancellation
                    let piece_request = (*piece_idx, *begin, data.len() as u32);
                    if cancellations.contains_key(&piece_request) {
                        cancellations.remove(&piece_request);
                        log::trace!("avoided sending canceled request to peer {} (block_idx: {} begin: {}, end: {})", peer_addr, piece_idx, begin, data.len());
                        continue;
                    }
                }

                log::trace!("sending message {} to peer {}", proto_msg, peer_addr);
                match timeout(DEFAULT_TIMEOUT, wire_proto.send(proto_msg)).await {
                    Err(_elapsed) => {
                        log::trace!("timeout sending message to peer {}", peer_addr);
                        send_to_torrent_manager(
                            &peers_to_torrent_manager_tx,
                            PeersToManagerMsg::Error(peer_addr, PeerError::Others),
                        )
                        .await;
                        break;
                    }
                    Ok(Err(e)) => {
                        log::trace!("sending failed with peer {}: {}", peer_addr, e);
                        send_to_torrent_manager(
                            &peers_to_torrent_manager_tx,
                            PeersToManagerMsg::Error(peer_addr, PeerError::Others),
                        )
                        .await;
                        break;
                    }
                    Ok(Ok(_)) => {}
                }
            }
        }
    }
}

async fn send_to_torrent_manager(
    peers_to_torrent_manager_tx: &Sender<PeersToManagerMsg>,
    msg: PeersToManagerMsg,
) {
    if peers_to_torrent_manager_tx.capacity() <= 5 {
        log::warn!(
            "low peers_to_torrent_manager_tx capacity: {}",
            peers_to_torrent_manager_tx.capacity()
        );
    }
    peers_to_torrent_manager_tx.send(msg).await.unwrap();
}

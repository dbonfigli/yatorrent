use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Result, bail};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::time::timeout;

use crate::bencoding::Value;
use crate::torrent_protocol::wire_protocol::{
    BlockRequest, Message, Protocol, ProtocolReadHalf, ProtocolWriteHalf,
};
use crate::util::{force_string, pretty_info_hash, version_string};

pub const MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER: i64 = 500;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const CANCELLATION_DURATION: Duration = Duration::from_secs(120);
const PEER_NO_INBOUND_TRAFFIC_FAILURE_TIMEOUT: Duration = Duration::from_secs(180);

const UT_PEX_EXTENSION_ID: i64 = 1;
const UT_METADATA_EXTENSION_ID: i64 = 2;
pub enum ToPeerMsg {
    Send(Message),
    Disconnect(),
}

pub type PeerAddr = String;
pub type FastExtensionSupport = bool;

pub enum PeersToManagerMsg {
    Error(PeerAddr, PeerError),
    Receive(PeerAddr, Message),
    NewPeer(TcpStream, FastExtensionSupport),
    PieceBlockRequestFulfilled(PeerAddr),
}

#[derive(PartialEq)]
pub enum PeerError {
    HandshakeError,
    Timeout,
    Others,
}

pub type ToPeerCancelMsg = (BlockRequest, SystemTime); // block request, cancel time

pub async fn connect_to_new_peer(
    host: String,
    port: u16,
    info_hash: [u8; 20],
    own_peer_id: String,
    tcp_wire_protocol_listening_port: u16,
    piece_completion_status: Option<Vec<bool>>,
    metadata_size: Option<i64>,
    peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
) {
    let dest = format!("{host}:{port}");
    log::trace!("initiating connection to peer: {dest}");
    match timeout(DEFAULT_TIMEOUT, TcpStream::connect(dest.clone())).await {
        Err(_elapsed) => {
            log::trace!("timed out connecting to peer {dest}");
            send_to_torrent_manager(
                &peers_to_torrent_manager_tx,
                PeersToManagerMsg::Error(format!("{host}:{port}"), PeerError::HandshakeError),
            )
            .await;
        }
        Ok(Err(e)) => {
            log::trace!("error initiating connection to peer {dest}: {e}");
            send_to_torrent_manager(
                &peers_to_torrent_manager_tx,
                PeersToManagerMsg::Error(format!("{host}:{port}"), PeerError::HandshakeError),
            )
            .await;
        }
        Ok(Ok(tcp_stream)) => {
            let peer_addr = match tcp_stream.peer_addr() {
                Ok(s) => s.to_string(),
                Err(e) => {
                    log::trace!(
                        "connecting to new peer failed because we could not get peer addr: {e}"
                    );
                    send_to_torrent_manager(
                        &peers_to_torrent_manager_tx,
                        PeersToManagerMsg::Error(
                            format!("{host}:{port}"),
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
                    piece_completion_status,
                    metadata_size,
                ),
            )
            .await
            {
                Err(_elapsed) => {
                    log::trace!("timed out completing handshake with peer {dest}");
                    send_to_torrent_manager(
                        &peers_to_torrent_manager_tx,
                        PeersToManagerMsg::Error(peer_addr, PeerError::HandshakeError),
                    )
                    .await;
                }
                Ok(Err(e)) => {
                    log::trace!("error completing handshake with peer {peer_addr}: {e}");
                    send_to_torrent_manager(
                        &peers_to_torrent_manager_tx,
                        PeersToManagerMsg::Error(peer_addr, PeerError::HandshakeError),
                    )
                    .await;
                }
                Ok(Ok((tcp_stream, supports_fast_extension))) => {
                    send_to_torrent_manager(
                        &peers_to_torrent_manager_tx,
                        PeersToManagerMsg::NewPeer(tcp_stream, supports_fast_extension),
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
    piece_completion_status: Option<Vec<bool>>,
    mut ok_to_accept_connection_rx: Receiver<bool>,
    mut metadata_size_rx: Receiver<i64>,
    mut piece_completion_status_rx: Receiver<Vec<bool>>,
    peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
    raw_metadata_size: Option<i64>,
) {
    let ok_to_accept_connection_for_rcv: Arc<Mutex<bool>> = Arc::new(Mutex::new(true)); // accept new connections at start
    let ok_to_accept_connection = ok_to_accept_connection_for_rcv.clone();
    tokio::spawn(async move {
        while let Some(msg) = ok_to_accept_connection_rx.recv().await {
            log::trace!("got message to accept/refuse new incoming connections: {msg}");
            let mut ok_to_accept_connection_for_rcv_lock =
                ok_to_accept_connection_for_rcv.lock().await;
            *ok_to_accept_connection_for_rcv_lock = msg;
            drop(ok_to_accept_connection_for_rcv_lock);
        }
    });

    let metadata_size_for_rcv: Arc<Mutex<Option<i64>>> = Arc::new(Mutex::new(raw_metadata_size));
    let metadata_size = metadata_size_for_rcv.clone();
    tokio::spawn(async move {
        while let Some(msg) = metadata_size_rx.recv().await {
            log::trace!("got message for newly known metadata size: {msg}");
            let mut metadata_size_for_rcv_lock = metadata_size_for_rcv.lock().await;
            *metadata_size_for_rcv_lock = Some(msg);
            drop(metadata_size_for_rcv_lock);
        }
    });

    let piece_completion_status_for_rcv: Arc<Mutex<Option<Vec<bool>>>> =
        Arc::new(Mutex::new(piece_completion_status));
    let piece_completion_status = piece_completion_status_for_rcv.clone();
    tokio::spawn(async move {
        while let Some(msg) = piece_completion_status_rx.recv().await {
            log::trace!("got message to update piece_completion_status");
            let mut piece_completion_status_for_rcv_lock =
                piece_completion_status_for_rcv.lock().await;
            *piece_completion_status_for_rcv_lock = Some(msg);
            drop(piece_completion_status_for_rcv_lock);
        }
    });

    let incoming_connection_listener =
        TcpListener::bind(format!("0.0.0.0:{tcp_wire_protocol_listening_port}"))
            .await
            .expect("failed binding to torrent protocol tcp port");

    tokio::spawn(async move {
        loop {
            log::debug!("waiting for incoming peer connections...");
            // never timeout on accept, wait forever if needed
            let mut stream = match incoming_connection_listener.accept().await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    log::warn!("accept failed: {e}");
                    continue;
                }
            };
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
            let metadata_size_for_spawn = metadata_size.clone();
            let own_peer_id_for_spawn = own_peer_id.clone();
            let peers_to_torrent_manager_tx_for_spawn = peers_to_torrent_manager_tx.clone();
            tokio::spawn(async move {
                let pcs_lock = piece_completion_status_for_spawn.lock().await;
                let pcs = pcs_lock.clone();
                drop(pcs_lock);
                let metadata_size_lock = metadata_size_for_spawn.lock().await;
                let metadata_size = metadata_size_lock.clone();
                drop(metadata_size_lock);
                let remote_addr = addr_or_unknown(&stream);
                match timeout(
                    DEFAULT_TIMEOUT,
                    handshake(
                        stream,
                        info_hash,
                        own_peer_id_for_spawn,
                        tcp_wire_protocol_listening_port,
                        pcs,
                        metadata_size,
                    ),
                )
                .await
                {
                    Err(_elapsed) => {
                        log::trace!("handshake timeout with peer {remote_addr}");
                    }
                    Ok(Err(e)) => {
                        log::trace!("handshake failed with peer {remote_addr}: {e}");
                    }
                    Ok(Ok((tcp_stream, supports_fast_extension))) => {
                        send_to_torrent_manager(
                            &peers_to_torrent_manager_tx_for_spawn,
                            PeersToManagerMsg::NewPeer(tcp_stream, supports_fast_extension),
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
    let mut rcv = tokio::spawn(rcv_message_handler(
        peer_addr.clone(),
        peers_to_torrent_manager_tx,
        read,
    ));
    let mut snd = tokio::spawn(snd_message_handler(
        peer_addr.clone(),
        to_peer_rx,
        peers_to_torrent_manager_tx_for_snd_message_handler,
        write,
        to_peer_cancel_rx,
    ));
    tokio::spawn(async move {
        tokio::select! {
            _ = &mut rcv => snd.abort(), // we dropped the read half, let's drop the write half, avoiding connection leaks
            _ = &mut snd => rcv.abort(), // vice-versa
        }
    });
}

async fn handshake(
    mut stream: TcpStream,
    info_hash: [u8; 20],
    own_peer_id: String,
    tcp_wire_protocol_listening_port: u16,
    piece_completion_status: Option<Vec<bool>>,
    metadata_size: Option<i64>,
) -> Result<(TcpStream, FastExtensionSupport)> {
    let (peer_protocol, reserved, peer_info_hash, peer_id) = stream
        .handshake(info_hash, own_peer_id.as_bytes().try_into()?)
        .await?;
    log::trace!(
        "received handshake info from {}: peer protocol: {peer_protocol}, info_hash: {}, peer_id: {}, reserved: {reserved:?}",
        addr_or_unknown(&stream),
        pretty_info_hash(peer_info_hash),
        force_string(&peer_id.to_vec()),
    );
    if peer_info_hash != info_hash {
        log::debug!(
            "handshake errored: info hash received during handshake does not match to the one we want (own: {}, theirs: {})",
            pretty_info_hash(info_hash),
            pretty_info_hash(peer_info_hash)
        );
        bail!("own and their infohash did not match");
    }

    let peer_addr = addr_or_unknown(&stream);
    let (read, mut write) = tokio::io::split(stream);

    let supports_fast_extension = if reserved[7] & 4u8 != 0 { true } else { false };

    if let Some(pcs) = piece_completion_status {
        let have_count = pcs.iter().filter(|status| **status).count();
        if supports_fast_extension && have_count == pcs.len() {
            write.send(Message::HaveAll).await?;
            log::trace!("have all sent to peer {peer_addr}");
        } else if supports_fast_extension && have_count == 0 {
            log::trace!("have none sent to peer {peer_addr}");
        } else {
            write.send(Message::Bitfield(pcs)).await?;
            log::trace!("bitfield sent to peer {peer_addr}");
        }
    } else {
        write.send(Message::HaveNone).await?;
        log::trace!("have none sent to peer {peer_addr}");
    }

    // if peer supports DHT, send port
    if reserved[7] & 1u8 != 0 {
        write
            .send(Message::Port(tcp_wire_protocol_listening_port))
            .await?;
        log::trace!("port sent to peer {peer_addr}");
    }

    // if peer supports extensions, send PEX and metadata extension support
    if reserved[5] & 0x10 != 0 {
        let mut handshake_dict = HashMap::from([
            (
                b"m".to_vec(),
                Value::Dict(
                    HashMap::from([
                        (b"ut_pex".to_vec(), Value::Int(UT_PEX_EXTENSION_ID)),
                        (
                            b"ut_metadata".to_vec(),
                            Value::Int(UT_METADATA_EXTENSION_ID),
                        ),
                    ]),
                    0,
                    0,
                ),
            ),
            (
                b"reqq".to_vec(),
                Value::Int(MAX_OUTSTANDING_INCOMING_PIECE_BLOCK_REQUESTS_PER_PEER),
            ),
            (b"v".to_vec(), Value::Str(version_string().into_bytes())),
        ]);
        if let Some(metadata_size) = metadata_size {
            handshake_dict.insert(b"metadata_size".to_vec(), Value::Int(metadata_size));
        }
        let extension_handshake = Value::Dict(handshake_dict, 0, 0);
        write
            .send(Message::Extended(0, extension_handshake, Vec::new()))
            .await?;
        log::trace!("extension handshake sent to peer {peer_addr}");
    }

    let stream = read.unsplit(write);

    // handshake completed successfully
    Ok((stream, supports_fast_extension))
}

async fn rcv_message_handler<T: ProtocolReadHalf + 'static>(
    peer_addr: String,
    peers_to_torrent_manager_tx: Sender<PeersToManagerMsg>,
    mut wire_proto: T,
) {
    loop {
        match timeout(
            PEER_NO_INBOUND_TRAFFIC_FAILURE_TIMEOUT,
            wire_proto.receive(),
        )
        .await
        {
            Err(_elapsed) => {
                log::trace!(
                    "did not receive anything (not even keep-alive messages) from peer {peer_addr} in {PEER_NO_INBOUND_TRAFFIC_FAILURE_TIMEOUT:#?}"
                );
                send_to_torrent_manager(
                    &peers_to_torrent_manager_tx,
                    PeersToManagerMsg::Error(peer_addr, PeerError::Timeout),
                )
                .await;
                break;
            }
            Ok(Err(e)) => {
                log::trace!("receive failed with peer {peer_addr}: {e}");
                send_to_torrent_manager(
                    &peers_to_torrent_manager_tx,
                    PeersToManagerMsg::Error(peer_addr, PeerError::Others),
                )
                .await;
                break;
            }
            Ok(Ok(proto_msg)) => {
                log::trace!("received from {peer_addr}: {proto_msg}");
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
    let mut cancellations = HashMap::<BlockRequest, SystemTime>::new();
    while let Some(manager_msg) = to_peer_rx.recv().await {
        match manager_msg {
            ToPeerMsg::Send(proto_msg) => {
                let mut is_sending_piece = false;

                // avoid sending data if the request has already been canceled by the peer
                if let Message::Piece(piece_idx, begin, data) = &proto_msg {
                    is_sending_piece = true;

                    // receive pending cancellations
                    while let Ok((block_request, cancel_time)) = to_peer_cancel_rx.try_recv() {
                        cancellations.insert(block_request, cancel_time);
                    }
                    // remove cancellations requested more than CANCELLATION_DURATION in the past
                    cancellations.retain(|_, cancel_time| {
                        SystemTime::now()
                            .duration_since(*cancel_time)
                            .unwrap_or_default()
                            < CANCELLATION_DURATION
                    });
                    // avoid sending if there is a cancellation
                    let block_request = BlockRequest {
                        piece_idx: *piece_idx,
                        block_begin: *begin,
                        data_len: data.len() as u32,
                    };
                    if cancellations.contains_key(&block_request) {
                        cancellations.remove(&block_request);
                        log::trace!(
                            "avoided sending canceled request to peer {peer_addr} (block_idx: {piece_idx} begin: {begin}, end: {})",
                            data.len()
                        );
                        send_to_torrent_manager(
                            &peers_to_torrent_manager_tx,
                            PeersToManagerMsg::PieceBlockRequestFulfilled(peer_addr.clone()),
                        )
                        .await;

                        continue;
                    }
                }

                log::trace!("sending message {proto_msg} to peer {peer_addr}");
                match timeout(DEFAULT_TIMEOUT, wire_proto.send(proto_msg)).await {
                    Err(_elapsed) => {
                        log::trace!("timeout sending message to peer {peer_addr}");
                        send_to_torrent_manager(
                            &peers_to_torrent_manager_tx,
                            PeersToManagerMsg::Error(peer_addr.clone(), PeerError::Others),
                        )
                        .await;
                        break;
                    }
                    Ok(Err(e)) => {
                        log::trace!("sending failed to peer {peer_addr}: {e}");
                        send_to_torrent_manager(
                            &peers_to_torrent_manager_tx,
                            PeersToManagerMsg::Error(peer_addr.clone(), PeerError::Others),
                        )
                        .await;
                        break;
                    }
                    Ok(Ok(_)) => {
                        if is_sending_piece {
                            send_to_torrent_manager(
                                &peers_to_torrent_manager_tx,
                                PeersToManagerMsg::PieceBlockRequestFulfilled(peer_addr.clone()),
                            )
                            .await;
                        }
                    }
                }
            }
            ToPeerMsg::Disconnect() => {
                break;
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
    // ignore error here: torrent manger can drop this peer due to errors in rcv_message_handler
    // while snd_message_handler is about to send messages to manager or vice versa
    _ = peers_to_torrent_manager_tx.send(msg).await;
}

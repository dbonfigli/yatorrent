use std::{
    collections::{HashMap, VecDeque},
    net::Ipv4Addr,
    time::{Duration, SystemTime},
};

use crate::{
    bencoding::Value::{Dict, Int, Str},
    manager::{
        bandwidth_tracker::BandwidthTracker,
        peer_handler::{FastExtensionSupport, ToPeerCancelMsg, ToPeerMsg},
        pex_handler::{AddedDroppedEvent, PexEvent},
    },
    torrent_protocol::wire_protocol::{BlockRequest, Message},
    util::HostAndPort,
};
use tokio::sync::mpsc::{
    Sender,
    error::TrySendError::{Closed, Full},
};

pub const METADATA_MESSAGE_REQUEST: i64 = 0;
pub const METADATA_MESSAGE_DATA: i64 = 1;
pub const METADATA_MESSAGE_REJECT: i64 = 2;

// can be retrieved per peer if it supports extensions, dict key "reqq",
// seen: deluge: 2000, qbittorrent: 500, transmission: 500, utorrent: 255, freebox bittorrent 2: 768, maybe variable.
// This parameter is extremelly important: a too low value will waste bandwidth in case a peer is really fast,
// a too high value will make the peer choke the connection and also saturate the channel capacity (see TO_PEER_CHANNEL_CAPACITY)
// 250 is the default in libtorrent as per https://bittorrent.org/beps/bep_0010.html
const DEFAULT_MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER: usize = 2000;
const RTT_SAMPLES_COUNT: usize = 20;
const KEEP_ALIVE_FREQ: Duration = Duration::from_secs(90);
const PEX_MESSAGE_COOL_OFF_PERIOD: Duration = Duration::from_secs(60);

pub enum MetadataMessage {
    Request(u64), // piece idx
    Data {
        piece_idx: u64,
        metadata_total_size: u64,
        data: Vec<u8>, // 16 kb or less if last piece
    },
    Reject(u64), // piece idx
}

pub struct Peer {
    peer_addr: HostAndPort,
    am_choking: bool,
    am_choking_since: SystemTime,
    am_interested: bool,
    peer_choking: bool,
    peer_choking_since: SystemTime,
    peer_interested: bool, // we are not really considering this now, should we use this as pre filter for incoming requests?
    haves: Option<Vec<bool>>, // this will be initialized after we have the metadata
    to_peer_tx: Sender<ToPeerMsg>,
    last_sent: SystemTime, // to understand when to send keepalived messages
    to_peer_cancel_tx: Sender<ToPeerCancelMsg>,
    outstanding_incoming_piece_block_requests: usize,
    ut_pex_id: u8,
    last_pex_message_sent: SystemTime,
    ut_metadata_id: u8,
    last_metadata_request_rejection: SystemTime,
    corruption_errors: u32,
    reqq: usize, // reqq received from peer
    bandwidth_tracker: BandwidthTracker,
    client_version: Option<String>,
    rtt: Option<Duration>,
    rtt_samples: VecDeque<Duration>,
    supports_fast_extension: bool,
}

impl Peer {
    pub fn new(
        peer_addr: HostAndPort,
        num_pieces: Option<usize>,
        to_peer_tx: Sender<ToPeerMsg>,
        to_peer_cancel_tx: Sender<ToPeerCancelMsg>,
        supports_fast_extension: FastExtensionSupport,
    ) -> Self {
        Peer {
            peer_addr,
            am_choking: true,
            am_choking_since: SystemTime::UNIX_EPOCH,
            am_interested: false,
            peer_choking: true,
            peer_choking_since: SystemTime::UNIX_EPOCH,
            peer_interested: false,
            haves: num_pieces.map(|n| vec![false; n]),
            to_peer_tx,
            last_sent: SystemTime::now(), // initally set it to now as there is no need to send them after the handshake
            to_peer_cancel_tx,
            outstanding_incoming_piece_block_requests: 0,
            ut_pex_id: 0, // i.e. no support for pex on this peer, initially
            last_pex_message_sent: SystemTime::UNIX_EPOCH,
            ut_metadata_id: 0, // i.e. no support for metadata on this peer, initially
            last_metadata_request_rejection: SystemTime::UNIX_EPOCH,
            corruption_errors: 0, // number of corrupted block received by this peer
            reqq: DEFAULT_MAX_OUTSTANDING_PIECE_BLOCK_REQUESTS_PER_PEER, // the number of outstanding request messages this client supports without dropping any
            bandwidth_tracker: BandwidthTracker::new(),
            client_version: Option::None,
            supports_fast_extension,
            rtt: None,
            rtt_samples: VecDeque::new(),
        }
    }

    pub fn get_peer_addr(&self) -> HostAndPort {
        self.peer_addr.clone()
    }

    pub fn get_am_choking(&self) -> bool {
        self.am_choking
    }

    pub fn set_am_choking(&mut self, chocking: bool) {
        self.am_choking = chocking;
        if chocking {
            self.am_choking_since = SystemTime::now();
        }
    }

    pub fn get_am_choking_since(&self) -> SystemTime {
        self.am_choking_since
    }

    pub fn get_am_interested(&self) -> bool {
        self.am_interested
    }

    pub fn set_am_interested(&mut self, interested: bool) {
        self.am_interested = interested;
    }

    pub fn is_peer_choking(&self) -> bool {
        self.peer_choking
    }

    pub fn set_peer_choking(&mut self, choking: bool) {
        self.peer_choking = choking;
        self.peer_choking_since = SystemTime::now();
    }

    pub fn peer_choking_since(&self) -> SystemTime {
        self.peer_choking_since
    }

    pub fn set_peer_interested(&mut self, interested: bool) {
        self.peer_interested = interested;
    }

    pub fn get_haves(&self) -> Option<&Vec<bool>> {
        self.haves.as_ref()
    }

    pub fn have_piece(&self, piece_idx: usize) -> bool {
        self.haves.as_ref().is_some_and(|haves| haves[piece_idx])
    }

    pub fn set_haves(&mut self, haves: Option<Vec<bool>>) {
        self.haves = haves
    }

    pub fn set_have(&mut self, piece_idx: usize) {
        if let Some(haves) = &mut self.haves
            && piece_idx < haves.len()
        {
            haves[piece_idx] = true;
        }
    }

    pub fn have_count(&self) -> usize {
        self.haves
            .as_ref()
            .map_or(0, |v| v.iter().filter(|x| **x).count())
    }

    pub async fn send(&mut self, msg: ToPeerMsg) {
        if self.to_peer_tx.capacity() <= 5 {
            log::warn!(
                "low to_peer_tx capacity to {}: {}",
                self.peer_addr,
                self.to_peer_tx.capacity()
            );
        }
        self.last_sent = SystemTime::now();
        let _ = self.to_peer_tx.send(msg).await;
        // ignore errors: it can happen that the channel is closed on the other side if the rx handler loop exited due to network errors,
        // and the peer is still lingering in self.peers because the control message about the error is not yet handled
    }

    pub fn try_send(&mut self, msg: ToPeerMsg) {
        match self.to_peer_tx.try_send(msg) {
            Ok(_) => {
                self.last_sent = SystemTime::now();
            }
            Err(Full(o)) => {
                log::debug!(
                    "no to_peer_tx capacity to {} on try_send, discarding {}",
                    self.peer_addr,
                    o
                );
            }
            Err(Closed(_)) => {
                // ignore errors: it can happen that the channel is closed on the other side if the rx handler loop exited due to network errors,
                // and the peer is still lingering in self.peers because the control message about the error is not yet handled
            }
        }
    }

    pub async fn send_keepalive(&mut self) {
        if let Ok(elapsed) = SystemTime::now().duration_since(self.last_sent)
            && elapsed > KEEP_ALIVE_FREQ
        {
            self.send(ToPeerMsg::Send(Message::KeepAlive)).await;
        }
    }

    pub fn send_cancel(&mut self, block_request: BlockRequest) {
        // we try to let the peer message handler know about the cancellation,
        // but if the buffer is full, we don't care, it means there were no outstanding messages to be sent
        // and so the cancellation would have no effect
        let _ = self
            .to_peer_cancel_tx
            .try_send((block_request, SystemTime::now()));
    }

    pub fn get_outstanding_incoming_piece_block_requests(&self) -> usize {
        self.outstanding_incoming_piece_block_requests
    }

    pub fn decrease_outstanding_incoming_piece_block_requests(&mut self) {
        self.outstanding_incoming_piece_block_requests = self
            .outstanding_incoming_piece_block_requests
            .saturating_sub(1);
    }

    pub fn increase_outstanding_incoming_piece_block_requests(&mut self) {
        self.outstanding_incoming_piece_block_requests += 1;
    }

    pub fn support_pex_extension(&self) -> bool {
        self.ut_pex_id != 0
    }

    pub fn get_ut_pex_id(&self) -> u8 {
        self.ut_pex_id
    }

    pub fn set_ut_pex_id(&mut self, ut_pex_id: u8) {
        self.ut_pex_id = ut_pex_id;
    }

    pub fn get_last_pex_message_sent(&self) -> SystemTime {
        self.last_pex_message_sent
    }

    pub async fn send_pex_extension_message(
        &mut self,
        added: Vec<HostAndPort>,
        dropped: Vec<HostAndPort>,
    ) {
        let mut h = HashMap::new();
        if !added.is_empty() {
            h.insert(
                b"added".to_vec(),
                Str(ip_port_list_to_compact_format(added)),
            );
        }
        if !dropped.is_empty() {
            h.insert(
                b"dropped".to_vec(),
                Str(ip_port_list_to_compact_format(dropped)),
            );
        }
        self.last_pex_message_sent = SystemTime::now();
        if !h.is_empty() {
            let pex_msg = Message::Extended {
                extension_protocol_id: self.ut_pex_id,
                bencoded_message: Dict {
                    dict: h,
                    start: 0,
                    end: 0,
                },
                additional_raw_data: Vec::new(),
            };
            log::trace!("sending pex message to peer {}: {pex_msg}", self.peer_addr);
            self.send(ToPeerMsg::Send(pex_msg)).await;
        }
    }

    pub fn get_ut_metadata_id(&self) -> u8 {
        self.ut_metadata_id
    }

    pub fn set_ut_metadata_id(&mut self, ut_metadata_id: u8) {
        self.ut_metadata_id = ut_metadata_id;
    }

    pub fn support_metadata_extension(&self) -> bool {
        self.ut_metadata_id != 0
    }

    pub fn get_last_metadata_request_rejection(&self) -> SystemTime {
        self.last_metadata_request_rejection
    }

    pub fn set_last_metadata_request_rejection(&mut self, last_rejection_tike: SystemTime) {
        self.last_metadata_request_rejection = last_rejection_tike
    }

    pub async fn send_metadata_extension_message(&mut self, metadata_message: MetadataMessage) {
        match metadata_message {
            MetadataMessage::Request(piece) => {
                let h = HashMap::from([
                    (b"msg_type".to_vec(), Int(METADATA_MESSAGE_REQUEST)),
                    (b"piece".to_vec(), Int(piece as i64)),
                ]);
                let metadata_msg = Message::Extended {
                    extension_protocol_id: self.ut_metadata_id,
                    bencoded_message: Dict {
                        dict: h,
                        start: 0,
                        end: 0,
                    },
                    additional_raw_data: Vec::new(),
                };
                log::trace!(
                    "sending metadata request message to peer {}: {metadata_msg}",
                    self.peer_addr
                );
                self.send(ToPeerMsg::Send(metadata_msg)).await;
            }
            MetadataMessage::Data {
                piece_idx,
                metadata_total_size,
                data,
            } => {
                let h = HashMap::from([
                    (b"msg_type".to_vec(), Int(METADATA_MESSAGE_DATA)),
                    (b"piece".to_vec(), Int(piece_idx as i64)),
                    (b"total_size".to_vec(), Int(metadata_total_size as i64)),
                ]);
                let metadata_msg = Message::Extended {
                    extension_protocol_id: self.ut_metadata_id,
                    bencoded_message: Dict {
                        dict: h,
                        start: 0,
                        end: 0,
                    },
                    additional_raw_data: data,
                };
                log::trace!(
                    "sending metadata data message to peer {}: {metadata_msg}",
                    self.peer_addr
                );
                self.send(ToPeerMsg::Send(metadata_msg)).await;
            }
            MetadataMessage::Reject(piece) => {
                let h = HashMap::from([
                    (b"msg_type".to_vec(), Int(METADATA_MESSAGE_REJECT)),
                    (b"piece".to_vec(), Int(piece as i64)),
                ]);
                let metadata_msg = Message::Extended {
                    extension_protocol_id: self.ut_metadata_id,
                    bencoded_message: Dict {
                        dict: h,
                        start: 0,
                        end: 0,
                    },
                    additional_raw_data: Vec::new(),
                };
                log::trace!(
                    "sending metadata reject message to peer {}: {metadata_msg}",
                    self.peer_addr
                );
                self.send(ToPeerMsg::Send(metadata_msg)).await;
            }
        }
    }

    pub fn increase_corruption_errors(&mut self) {
        self.corruption_errors += 1
    }

    pub fn get_corruption_errors(&self) -> u32 {
        self.corruption_errors
    }

    pub fn set_reqq(&mut self, reqq: usize) {
        self.reqq = reqq
    }

    pub fn get_reqq(&self) -> usize {
        self.reqq
    }

    pub fn get_bandwidth_tracker(&self) -> &BandwidthTracker {
        &self.bandwidth_tracker
    }

    pub fn get_bandwidth_tracker_mut(&mut self) -> &mut BandwidthTracker {
        &mut self.bandwidth_tracker
    }

    pub fn get_client_version(&self) -> String {
        self.client_version
            .as_deref()
            .unwrap_or("unknown")
            .to_string()
    }

    pub fn set_client_version(&mut self, client_version: String) {
        if self.client_version.is_none() {
            self.client_version = Some(client_version);
        }
    }

    pub fn supports_fast_extension(&self) -> bool {
        self.supports_fast_extension
    }

    pub fn get_rtt(&self) -> Option<Duration> {
        self.rtt
    }

    pub fn update_rtt(&mut self, rtt_sample: Duration) {
        self.rtt_samples.push_front(rtt_sample);
        if self.rtt_samples.len() > RTT_SAMPLES_COUNT {
            // todo: should we remove old samples only based on number or also based on oldness?
            // i.e. if we get 20 messages al at the same time now, we lose the "history",
            // should we keep data up to some 3s instead for example?
            self.rtt_samples.pop_back();
        }
        let mut latencies = Duration::ZERO;
        for s in self.rtt_samples.iter() {
            latencies += *s;
        }
        let rtt = latencies.div_f64(self.rtt_samples.len() as f64);
        self.rtt = Some(rtt);
    }

    pub async fn send_pex_extension_message_for_latest_peer_events(
        &mut self,
        latest_pex_update: SystemTime,
        added_dropped_peer_events: &Vec<AddedDroppedEvent>,
    ) {
        if !self.support_pex_extension() {
            return;
        }
        if latest_pex_update
            .duration_since(self.get_last_pex_message_sent())
            .unwrap_or_default()
            <= PEX_MESSAGE_COOL_OFF_PERIOD
        {
            return;
        }

        // only send events we have not yet sent
        let elided_events = added_dropped_peer_events
            .iter()
            .filter(
                |AddedDroppedEvent {
                     event_timestamp, ..
                 }| *event_timestamp > self.get_last_pex_message_sent(),
            )
            .fold(
                HashMap::new(),
                |mut map,
                 AddedDroppedEvent {
                     peer, event_type, ..
                 }| {
                    map.insert(peer.clone(), *event_type);
                    map
                },
            );
        let added = elided_events
            .iter()
            .filter(|(_, event_type)| **event_type == PexEvent::Added)
            .map(|(p, _)| (*p).clone())
            .collect();
        let dropped = elided_events
            .iter()
            .filter(|(_, event_type)| **event_type == PexEvent::Dropped)
            .map(|(p, _)| (*p).clone())
            .collect();
        self.send_pex_extension_message(added, dropped).await;
    }
}

fn ip_port_list_to_compact_format(addrs: Vec<HostAndPort>) -> Vec<u8> {
    let mut compact_format: Vec<u8> = Vec::new();
    for addr in addrs {
        let ip_port: Vec<_> = addr.split(':').collect();
        if ip_port.len() != 2 {
            panic!("addr string was not of the format ip:port");
        }
        let ipv4_addr: Ipv4Addr = ip_port[0].parse().expect("addr was not an ipv4");
        compact_format.append(&mut ipv4_addr.octets().to_vec());
        let port: u16 = ip_port[1].parse().expect("port was not a u16");
        compact_format.append(&mut port.to_be_bytes().to_vec());
    }
    compact_format
}

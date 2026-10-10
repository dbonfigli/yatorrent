use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::{Duration, Instant},
};

use crate::{
    bencoding::Value::{self, Dict, Str},
    manager::peer::Peer,
};

const ADDED_DROPPED_PEER_EVENTS_RETENTION: Duration = Duration::from_secs(90); // should be bigger than PEX_MESSAGE_COOL_OFF_PERIOD

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum PexEvent {
    Added,
    Dropped,
}

pub struct AddedDroppedEvent {
    pub event_timestamp: Instant,
    pub peer: SocketAddr,
    pub event_type: PexEvent,
}

pub struct PexHandler {
    added_dropped_peer_events: Vec<AddedDroppedEvent>,
}

impl PexHandler {
    pub fn new() -> Self {
        PexHandler {
            added_dropped_peer_events: Vec::new(),
        }
    }

    pub async fn send_pex_messages(&mut self, peers: &mut HashMap<SocketAddr, Peer>) {
        // remove old added / dropped events
        let now = Instant::now();
        self.added_dropped_peer_events.retain(
            |AddedDroppedEvent {
                 event_timestamp, ..
             }| {
                now.duration_since(*event_timestamp) < ADDED_DROPPED_PEER_EVENTS_RETENTION
            },
        );

        for peer in peers.values_mut() {
            peer.send_pex_extension_message_for_latest_peer_events(
                now,
                &self.added_dropped_peer_events,
            )
            .await;
        }
    }

    pub fn new_pex_event(&mut self, peer_addr: SocketAddr, pex_event: PexEvent) {
        self.added_dropped_peer_events
            .retain(|e| e.peer != peer_addr);
        self.added_dropped_peer_events.push(AddedDroppedEvent {
            event_timestamp: Instant::now(),
            peer: peer_addr,
            event_type: pex_event,
        });
    }
}

pub fn parse_extended_message_ut_pex(
    extended_message: Value,
    peer_addr: SocketAddr,
) -> Vec<SocketAddr> {
    let mut peers = Vec::new();

    let d = match extended_message {
        Dict { dict: d, .. } => d,
        _ => {
            log::debug!(
                "got a PEX message from {peer_addr}, it was a bencoded value but not a dict, ignoring it"
            );
            return peers;
        }
    };
    // todo: should we also use the dropped list? atm we are eager to hoard all possible peers so we ignore it
    if let Some(Str(compact_contacts_info)) = d.get(b"added".as_slice()) {
        // we don't support flags, dropped or ipv6 fields ATM
        if compact_contacts_info.len() % 6 != 0 {
            log::debug!(
                "got a PEX message from {peer_addr} with an \"added\" field len that is not divisible by 6, ignoring it"
            );
        } else {
            for i in (0..compact_contacts_info.len()).step_by(6) {
                let ip = Ipv4Addr::new(
                    compact_contacts_info[i],
                    compact_contacts_info[i + 1],
                    compact_contacts_info[i + 2],
                    compact_contacts_info[i + 3],
                );
                let port = u16::from_be_bytes([
                    compact_contacts_info[i + 4],
                    compact_contacts_info[i + 5],
                ]);
                peers.push(SocketAddr::new(IpAddr::V4(ip), port));
            }
        }
    }

    if let Some(Str(compact_contacts_info)) = d.get(b"added6".as_slice()) {
        if compact_contacts_info.len() % 18 != 0 {
            log::debug!(
                "got a PEX message from {peer_addr} with an \"added6\" field len that is not divisible by 18, ignoring it"
            );
        } else {
            for i in (0..compact_contacts_info.len()).step_by(18) {
                let ip = Ipv6Addr::new(
                    u16::from_be_bytes([compact_contacts_info[i], compact_contacts_info[i + 1]]),
                    u16::from_be_bytes([
                        compact_contacts_info[i + 2],
                        compact_contacts_info[i + 3],
                    ]),
                    u16::from_be_bytes([
                        compact_contacts_info[i + 4],
                        compact_contacts_info[i + 5],
                    ]),
                    u16::from_be_bytes([
                        compact_contacts_info[i + 6],
                        compact_contacts_info[i + 7],
                    ]),
                    u16::from_be_bytes([
                        compact_contacts_info[i + 8],
                        compact_contacts_info[i + 9],
                    ]),
                    u16::from_be_bytes([
                        compact_contacts_info[i + 10],
                        compact_contacts_info[i + 11],
                    ]),
                    u16::from_be_bytes([
                        compact_contacts_info[i + 12],
                        compact_contacts_info[i + 13],
                    ]),
                    u16::from_be_bytes([
                        compact_contacts_info[i + 14],
                        compact_contacts_info[i + 15],
                    ]),
                );
                let port = u16::from_be_bytes([
                    compact_contacts_info[i + 16],
                    compact_contacts_info[i + 17],
                ]);
                peers.push(SocketAddr::new(IpAddr::V6(ip), port));
            }
        }
    }

    peers
}

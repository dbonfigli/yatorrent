use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use crate::{
    bencoding::Value::{self, Dict, Str},
    manager::peer::Peer,
    tracker,
    util::HostAndPort,
};

const ADDED_DROPPED_PEER_EVENTS_RETENTION: Duration = Duration::from_secs(90); // should be bigger than PEX_MESSAGE_COOL_OFF_PERIOD

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum PexEvent {
    Added,
    Dropped,
}

pub struct AddedDroppedEvent {
    pub event_timestamp: SystemTime,
    pub peer: HostAndPort,
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

    pub async fn send_pex_messages(&mut self, peers: &mut HashMap<HostAndPort, Peer>) {
        // remove old added / dropped events
        let now = SystemTime::now();
        self.added_dropped_peer_events.retain(
            |AddedDroppedEvent {
                 event_timestamp, ..
             }| {
                now.duration_since(*event_timestamp).unwrap_or_default()
                    < ADDED_DROPPED_PEER_EVENTS_RETENTION
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

    pub fn new_pex_event(&mut self, peer_addr: HostAndPort, pex_event: PexEvent) {
        self.added_dropped_peer_events
            .retain(|e| e.peer != peer_addr);
        self.added_dropped_peer_events.push(AddedDroppedEvent {
            event_timestamp: SystemTime::now(),
            peer: peer_addr,
            event_type: pex_event,
        });
    }
}

pub fn parse_extended_message_ut_pex(
    extended_message: Value,
    peer_addr: HostAndPort,
) -> Vec<tracker::Peer> {
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
                "got a PEX message from {peer_addr} with an \"added\" field that is not divisible by 6, ignoring it"
            );
            return peers;
        }
        for i in (0..compact_contacts_info.len()).step_by(6) {
            let mut peer_ip_buf: [u8; 4] = [0; 4];
            peer_ip_buf.copy_from_slice(&compact_contacts_info[i..i + 4]);
            let ip = [
                peer_ip_buf[0].to_string(),
                peer_ip_buf[1].to_string(),
                peer_ip_buf[2].to_string(),
                peer_ip_buf[3].to_string(),
            ]
            .join(".");
            let mut peer_port_buf: [u8; 2] = [0; 2];
            peer_port_buf.copy_from_slice(&compact_contacts_info[i + 4..i + 6]);
            let port = u16::from_be_bytes(peer_port_buf);
            let p = tracker::Peer {
                peer_id: None,
                ip: ip.clone(),
                port,
            };
            peers.push(p);
        }
    }

    peers
}

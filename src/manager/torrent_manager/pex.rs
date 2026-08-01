use std::time::{Duration, SystemTime};

use crate::{
    bencoding::Value::{self, Dict, Str},
    manager::torrent_manager::TorrentManager,
    tracker,
};

const ADDED_DROPPED_PEER_EVENTS_RETENTION: Duration = Duration::from_secs(90); // should be bigger than PEX_MESSAGE_COOL_OFF_PERIOD

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum PexEvent {
    Added,
    Dropped,
}

impl TorrentManager {
    pub(super) async fn send_pex_messages(&mut self) {
        // remove old added / dropped events
        let now = SystemTime::now();
        self.added_dropped_peer_events
            .retain(|(event_timestamp, _, _)| {
                now.duration_since(*event_timestamp).unwrap_or_default()
                    < ADDED_DROPPED_PEER_EVENTS_RETENTION
            });

        for peer in self.peers_state.peers.values_mut() {
            peer.send_pex_extension_message_for_latest_peer_events(
                now,
                &self.added_dropped_peer_events,
            )
            .await;
        }
    }

    pub(super) fn handle_receive_extended_message_ut_pex(
        &mut self,
        extended_message: Value,
        peer_addr: String,
    ) {
        let d = match extended_message {
            Dict(d, _, _) => d,
            _ => {
                log::debug!(
                    "got a PEX message from {peer_addr}, it was a bencoded value but not a dict, ignoring it"
                );
                return;
            }
        };
        // todo: should we also use the dropped list? atm we are eager to hoard all possible peers so we ignore it
        if let Some(Str(compact_contacts_info)) = d.get(&b"added".to_vec()) {
            // we don't support flags, dropped or ipv6 fields ATM
            if compact_contacts_info.len() % 6 != 0 {
                log::debug!(
                    "got a PEX message from {peer_addr} with an \"added\" field that is not divisible by 6, ignoring it"
                );
                return;
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
                log::debug!("adding peer advertised by {peer_addr} from PEX: {ip}:{port}");
                let p = tracker::Peer {
                    peer_id: None,
                    ip: ip.clone(),
                    port,
                };
                let mut advertised_peers_mg = self
                    .peers_state
                    .advertised_peers
                    .lock()
                    .expect("another user panicked while holding the lock");
                advertised_peers_mg.insert(format!("{ip}:{port}"), (p, SystemTime::UNIX_EPOCH));
                drop(advertised_peers_mg);
            }
        }
    }

    pub(super) fn new_pex_event(&mut self, peer_addr: String, pex_event: PexEvent) {
        self.added_dropped_peer_events
            .push((SystemTime::now(), peer_addr, pex_event));
    }
}

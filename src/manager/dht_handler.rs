use std::{
    net::Ipv4Addr,
    time::{Duration, SystemTime},
};

use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::{
    dht::dht_manager::{DhtManager, DhtToTorrentManagerMsg, ToDhtManagerMsg},
    util::HostAndPort,
};

const DHT_BOOTSTRAP_TIME: Duration = Duration::from_secs(5);
const DHT_NEW_PEER_COOL_OFF_PERIOD: Duration = Duration::from_secs(15);

pub struct DhtHandler {
    torrent_info_hash: [u8; 20],
    dht_nodes: Vec<HostAndPort>,
    // internal channels, we store them here to avoid passing them around in nested calls
    to_dht_manager_tx: UnboundedSender<ToDhtManagerMsg>,
    to_dht_manager_rx: Option<UnboundedReceiver<ToDhtManagerMsg>>, // optional bc we will move it to the dht manager at start, todo: should we move creation of this channel there?
    last_get_peers_requested_time: SystemTime,
}

impl DhtHandler {
    pub fn new(dht_nodes: Vec<HostAndPort>, torrent_info_hash: [u8; 20]) -> Self {
        let (to_dht_manager_tx, to_dht_manager_rx) = mpsc::unbounded_channel();

        DhtHandler {
            torrent_info_hash,
            dht_nodes,
            to_dht_manager_tx,
            to_dht_manager_rx: Some(to_dht_manager_rx),
            last_get_peers_requested_time: SystemTime::now() - DHT_NEW_PEER_COOL_OFF_PERIOD
                + DHT_BOOTSTRAP_TIME, // try to wait a bit before the first request, in hope that the dht has been bootstrapped, so that we don't waste time for the first request with an empty routing table
        }
    }

    pub fn start_dht_manager(
        &mut self,
        listening_torrent_wire_protocol_port: u16,
        listening_dht_port: u16,
    ) -> UnboundedReceiver<DhtToTorrentManagerMsg> {
        let (dht_to_torrent_manager_tx, dht_to_torrent_manager_rx) = mpsc::unbounded_channel();

        let mut dht_manager = DhtManager::new(
            listening_torrent_wire_protocol_port,
            listening_dht_port,
            self.dht_nodes.clone(),
        );
        let to_dht_manager_rx = self
            .to_dht_manager_rx
            .take()
            .expect("no to_dht_manager_rx, has start been called twice?");

        tokio::spawn(async move {
            dht_manager
                .start(to_dht_manager_rx, dht_to_torrent_manager_tx)
                .await;
        });

        dht_to_torrent_manager_rx
    }

    pub fn request_new_peers_to_dht_manager(&mut self) {
        let now = SystemTime::now();
        if now
            .duration_since(self.last_get_peers_requested_time)
            .unwrap_or_default()
            > DHT_NEW_PEER_COOL_OFF_PERIOD
        {
            self.last_get_peers_requested_time = now;
            self.to_dht_manager_tx
                .send(ToDhtManagerMsg::GetNewPeers(self.torrent_info_hash))
                .expect("to_dht_manager_tx receiver half closed");
        }
    }

    pub fn new_node_discovered(&mut self, peer_ip_addr: &str, peer_port: u16) {
        self.to_dht_manager_tx
            .send(ToDhtManagerMsg::NewNode(format!(
                "{peer_ip_addr}:{peer_port}"
            )))
            .expect("to_dht_manager_tx receiver half closed");
    }

    pub fn new_peer_connected(&mut self, peer_ip_addr: Ipv4Addr, peer_port: u16) {
        self.to_dht_manager_tx
            .send(ToDhtManagerMsg::ConnectedToNewPeer(
                self.torrent_info_hash,
                peer_ip_addr,
                peer_port,
            ))
            .expect("to_dht_manager_tx receiver half closed");
    }
}

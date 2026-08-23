use std::{
    net::Ipv4Addr,
    time::{Duration, Instant},
};

use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::{
    dht::dht_manager::{DhtManager, DhtToTorrentManagerMsg, ToDhtManagerMsg},
    util::HostAndPort,
};

const DHT_BOOTSTRAP_TIME: Duration = Duration::from_secs(5);
const DHT_NEW_PEER_COOL_OFF_PERIOD: Duration = Duration::from_secs(15);

pub struct DhtHandler {
    dht_enabled: bool,
    torrent_info_hash: [u8; 20],
    dht_nodes: Vec<HostAndPort>,
    // internal channels, we store them here to avoid passing them around in nested calls
    to_dht_manager_tx: UnboundedSender<ToDhtManagerMsg>,
    to_dht_manager_rx: Option<UnboundedReceiver<ToDhtManagerMsg>>, // optional bc we will move it to the dht manager at start, todo: should we move creation of this channel there?
    dht_to_torrent_manager_rx: UnboundedReceiver<DhtToTorrentManagerMsg>,
    dht_to_torrent_manager_tx: UnboundedSender<DhtToTorrentManagerMsg>,
    last_get_peers_requested_time: Instant,
}

impl DhtHandler {
    pub fn new(
        dht_enabled: bool,
        dht_nodes: Vec<HostAndPort>,
        torrent_info_hash: [u8; 20],
    ) -> Self {
        let (to_dht_manager_tx, to_dht_manager_rx) = mpsc::unbounded_channel();
        let (dht_to_torrent_manager_tx, dht_to_torrent_manager_rx) = mpsc::unbounded_channel();

        DhtHandler {
            dht_enabled,
            torrent_info_hash,
            dht_nodes,
            to_dht_manager_tx,
            to_dht_manager_rx: Some(to_dht_manager_rx),
            last_get_peers_requested_time: Instant::now() - DHT_NEW_PEER_COOL_OFF_PERIOD
                + DHT_BOOTSTRAP_TIME, // try to wait a bit before the first request, in hope that the dht has been bootstrapped, so that we don't waste time for the first request with an empty routing table
            dht_to_torrent_manager_rx,
            dht_to_torrent_manager_tx,
        }
    }

    pub fn start_dht_manager(
        &mut self,
        listening_torrent_wire_protocol_port: u16,
        listening_dht_port: u16,
    ) {
        if !self.dht_enabled {
            return;
        }

        let mut dht_manager = DhtManager::new(
            listening_torrent_wire_protocol_port,
            listening_dht_port,
            self.dht_nodes.clone(),
        );
        let to_dht_manager_rx = self
            .to_dht_manager_rx
            .take()
            .expect("no to_dht_manager_rx, has start been called twice?");
        let dht_to_torrent_manager_tx = self.dht_to_torrent_manager_tx.clone();
        tokio::spawn(async move {
            dht_manager
                .start(to_dht_manager_rx, dht_to_torrent_manager_tx)
                .await;
        });
    }

    pub fn request_new_peers_to_dht_manager(&mut self) {
        if !self.dht_enabled {
            return;
        }

        let now = Instant::now();
        if now.duration_since(self.last_get_peers_requested_time) > DHT_NEW_PEER_COOL_OFF_PERIOD {
            self.last_get_peers_requested_time = now;
            _ = self
                .to_dht_manager_tx
                .send(ToDhtManagerMsg::GetNewPeers(self.torrent_info_hash));
        }
    }

    pub fn new_node_discovered(&mut self, peer_ip_addr: Ipv4Addr, peer_port: u16) {
        if !self.dht_enabled {
            return;
        }

        _ = self
            .to_dht_manager_tx
            .send(ToDhtManagerMsg::NewNode(format!(
                "{peer_ip_addr}:{peer_port}"
            )));
    }

    pub fn new_peer_connected(&mut self, peer_ip_addr: Ipv4Addr, peer_port: u16) {
        _ = self
            .to_dht_manager_tx
            .send(ToDhtManagerMsg::ConnectedToNewPeer(
                self.torrent_info_hash,
                peer_ip_addr,
                peer_port,
            ));
    }

    pub async fn recv_new_peer(&mut self) -> Option<DhtToTorrentManagerMsg> {
        if !self.dht_enabled {
            return None;
        }

        self.dht_to_torrent_manager_rx.recv().await
    }
}

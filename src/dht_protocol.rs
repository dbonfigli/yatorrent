use std::{
    collections::HashMap,
    net::Ipv4Addr,
    sync::{Arc, Mutex},
};

use tokio::{
    net::UdpSocket,
    sync::mpsc::{Receiver, Sender},
};

use rand::Rng;

use crate::{dht_messages::decode_krpc_message, util::pretty_info_hash};

// NOTE! we are only supporting IPv4 DHT, i.e. BEP 32 (https://www.bittorrent.org/beps/bep_0032.html) is not implemented

static WELL_KNOWN_BOOTSTRAP_NODES: &[&str] = &[
    "dht.libtorrent.org:25401",
    "router.utorrent.com:6881",
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.aelitis.com:6881",
];

#[derive(Debug)]
struct Node {} // todo: fill up with useful info

pub struct DhtManager {
    listening_dht_port: u16,
    listening_torrent_wire_protocol_port: u16,
    own_node_id: String,
    nodes: Arc<Mutex<HashMap<String, Node>>>,
}

fn generate_transaction_id() -> [u8; 2] {
    let mut rng = rand::thread_rng();
    [rng.gen(), rng.gen()]
}

pub enum ToDhtManagerMsg {
    GetNewPeers([u8; 20]), // info hash
    NewNode(String),       // "host:port"
}

pub enum DhtToTorrentManagerMsg {
    NewPeer(Ipv4Addr, u16),
}

impl DhtManager {
    pub fn new(
        listening_torrent_wire_protocol_port: u16,
        listening_dht_port: u16,
        own_node_id: String,
        mut nodes: Vec<String>,
    ) -> DhtManager {
        let mut start_nodes = HashMap::new();
        for n in nodes {
            start_nodes.insert(n, Node {});
        }
        for n in WELL_KNOWN_BOOTSTRAP_NODES {
            start_nodes.insert(n.to_string(), Node {});
        }
        return DhtManager {
            listening_dht_port,
            listening_torrent_wire_protocol_port,
            own_node_id,
            nodes: Arc::new(Mutex::new(start_nodes)),
        };
    }

    pub async fn start(
        &mut self,
        to_dht_manager_rx: Receiver<ToDhtManagerMsg>,
        dht_to_torrent_manager_tx: Sender<DhtToTorrentManagerMsg>,
    ) {
        tokio::spawn(handle_torrent_manger_messages(
            to_dht_manager_rx,
            self.nodes.clone(),
        ));

        let socket = UdpSocket::bind(format!("0.0.0.0:{}", self.listening_dht_port))
            .await
            .unwrap();

        let mut msg_buf = [0u8; 65535]; // max udp datagram size
        while let Ok((msg_size, addr)) = socket.recv_from(&mut msg_buf).await {
            match decode_krpc_message(msg_buf[0..msg_size].to_vec()) {
                Err(e) => {
                    log::trace!("error decoding incoming dht message from {}: {}", addr, e);
                    continue;
                }
                Ok((transaction_id, msg)) => {
                    // do things
                }
            }
        }
    }
}

async fn handle_torrent_manger_messages(
    mut to_dht_manager_rx: Receiver<ToDhtManagerMsg>,
    nodes: Arc<Mutex<HashMap<String, Node>>>,
) {
    while let Some(msg) = to_dht_manager_rx.recv().await {
        match msg {
            ToDhtManagerMsg::GetNewPeers(info_hash) => {
                log::warn!("got GetNewPeers msg: {:?}", pretty_info_hash(info_hash));
            }
            ToDhtManagerMsg::NewNode(n) => {
                let mut nodes_mg = nodes.lock().unwrap();
                nodes_mg.insert(n, Node {});
                log::warn!("current nodes: {:?}", nodes_mg);
                drop(nodes_mg);
            }
        }
    }
}

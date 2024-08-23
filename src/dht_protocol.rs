use std::net::Ipv4Addr;

use tokio::{
    net::UdpSocket,
    sync::mpsc::{Receiver, Sender},
};

use rand::Rng;

use crate::dht_messages::decode_krpc_message;

// NOTE! we are only supporting IPv4 DHT, i.e. BEP 32 (https://www.bittorrent.org/beps/bep_0032.html) is not implemented

static WELL_KNOWN_BOOTSTRAP_NODES: &[&str] = &[
    "dht.libtorrent.org:25401",
    "router.utorrent.com:6881",
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.aelitis.com:6881",
];

pub struct DhtClient {
    listening_dht_port: u16,
    listening_torrent_wire_protocol_port: u16,
    own_node_id: String,
    nodes: Vec<String>,
}

fn generate_transaction_id() -> [u8; 2] {
    let mut rng = rand::thread_rng();
    [rng.gen(), rng.gen()]
}

pub enum ToDhtMsg {
    GetNewPeers,
    NewNode(String),
}

pub enum ToManagerMsg {
    NewPeer(Ipv4Addr, u16),
}

impl DhtClient {
    pub fn new(
        listening_torrent_wire_protocol_port: u16,
        listening_dht_port: u16,
        own_node_id: String,
        mut nodes: Vec<String>,
    ) -> DhtClient {
        let mut start_nodes = Vec::new();
        start_nodes.append(&mut nodes);
        for n in WELL_KNOWN_BOOTSTRAP_NODES {
            start_nodes.push(n.to_string());
        }
        return DhtClient {
            listening_dht_port,
            listening_torrent_wire_protocol_port,
            own_node_id,
            nodes: start_nodes,
        };
    }

    pub async fn start(
        &mut self,
        to_dht_rx: Receiver<ToDhtMsg>,
        to_manager_tx: Sender<ToManagerMsg>,
    ) {
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

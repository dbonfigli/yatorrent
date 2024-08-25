use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use tokio::{
    net::UdpSocket,
    sync::mpsc::{self, Receiver, Sender},
};

use rand::Rng;

use crate::{
    dht::messages::{decode_krpc_message, encode_krpc_message}, util::{force_string, pretty_info_hash, start_tick}
};

use super::messages::KRPCMessage;

// NOTE! we are only supporting IPv4 DHT, i.e. BEP 32 (https://www.bittorrent.org/beps/bep_0032.html) is not implemented

static WELL_KNOWN_BOOTSTRAP_NODES: &[&str] = &[
    "dht.libtorrent.org:25401",
    "router.utorrent.com:6881",
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.aelitis.com:6881",
];

fn generate_transaction_id() -> [u8; 2] {
    let mut rng = rand::thread_rng();
    [rng.gen(), rng.gen()]
}

pub enum ToDhtManagerMsg {
    GetNewPeers([u8; 20]), // info hash
    NewNode(String),       // addr of new node, i.e. "host:port" string
}

pub enum DhtToTorrentManagerMsg {
    NewPeer(Ipv4Addr, u16),
}

pub struct DhtManager {
    listening_dht_port: u16,
    listening_torrent_wire_protocol_port: u16,
    own_node_id: [u8; 20],
    bootstrap_nodes: Vec<String>,
    inflght_requests: HashMap<Vec<u8>, String>, // transaction id -> dest addr
}

impl DhtManager {
    pub fn new(
        listening_torrent_wire_protocol_port: u16,
        listening_dht_port: u16,
        nodes: Vec<String>,
    ) -> DhtManager {
        let mut bootstrap_nodes = nodes;
        for n in WELL_KNOWN_BOOTSTRAP_NODES {
            bootstrap_nodes.push(n.to_string());
        }
        let mut rng = rand::thread_rng();
        let mut own_node_id: [u8; 20] = [0u8; 20];
        for i in 0..20 {
            own_node_id[i] = rng.gen();
        }
        return DhtManager {
            listening_dht_port,
            listening_torrent_wire_protocol_port,
            own_node_id,
            bootstrap_nodes,
            inflght_requests: HashMap::new(),
        };
    }

    pub async fn do_req(&mut self, socket: &UdpSocket, dest: String, msg: KRPCMessage) {
        let tid = generate_transaction_id();
        log::warn!(
            "perform req to {}, tid: {}, msg: {:?}",
            dest.clone(),
            force_string(&tid.to_vec()),
            msg
        );
        let buf = encode_krpc_message(tid.to_vec(), msg);
        self.inflght_requests.insert(tid.to_vec(), dest.clone());
        let _ = socket.send_to(&buf, dest).await.unwrap();
    }

    pub async fn start(
        &mut self,
        mut to_dht_manager_rx: Receiver<ToDhtManagerMsg>,
        dht_to_torrent_manager_tx: Sender<DhtToTorrentManagerMsg>,
    ) {
        // start ticker
        let (tick_tx, mut tick_rx) = mpsc::channel(1);
        start_tick(tick_tx, Duration::from_secs(1)).await;

        // open socket
        let socket = UdpSocket::bind(format!("0.0.0.0:{}", self.listening_dht_port))
            .await
            .unwrap();

        // bootstrap known nodes by finding node closest to self, and by that updating the routing table
        for i in 0..self.bootstrap_nodes.len() {
            self.do_req(
                &socket,
                self.bootstrap_nodes[i].clone(),
                KRPCMessage::FindNodeReq(self.own_node_id, self.own_node_id),
            )
            .await;
        }

        let mut msg_buf: [u8; 65535] = [0u8; 65535]; // max udp datagram size
        loop {
            tokio::select! {
                Ok((msg_size, addr)) = socket.recv_from(&mut msg_buf) => {
                    match decode_krpc_message(msg_buf[0..msg_size].to_vec()) {
                        Err(e) => {
                            log::warn!("error decoding incoming dht message from {}: {}", addr, e);
                        }
                        Ok((transaction_id, msg)) => {
                            self.handle_incoming_message(addr, transaction_id, msg);
                        }
                    }
                }
                Some(()) = tick_rx.recv() => {
                    // todo do things - remove stale inflight requests, send pings, etc
                }
                Some(msg) = to_dht_manager_rx.recv() => {
                    match msg {
                        ToDhtManagerMsg::GetNewPeers(info_hash) => {
                            log::trace!("got GetNewPeers msg: {:?}", pretty_info_hash(info_hash));
                            // todo do stuff to gather new peers for this info hash
                        }
                        ToDhtManagerMsg::NewNode(addr) => {
                            // ping new dht node to get node id and eventually put it in routing table
                            let tid = generate_transaction_id().to_vec();
                            let buf = encode_krpc_message(tid, KRPCMessage::PingReq(self.own_node_id));
                            let _ = socket.send_to(&buf, addr).await.unwrap();
                        }
                    }
                }
            }
        }
    }

    fn handle_incoming_message(
        &mut self,
        addr: SocketAddr,
        transaction_id: Vec<u8>,
        msg: KRPCMessage,
    ) {
        log::warn!(
            "got message from {}: tid: {}, msg: {:?}",
            addr,
            force_string(&transaction_id),
            msg
        );
        match msg {
            KRPCMessage::PingReq(_) => {}
            KRPCMessage::PingOrAnnouncePeerResp(queried_node_id) => {
                if !self.inflght_requests.contains_key(&transaction_id) {
                    log::warn!("got a ping or announce peer resp from {} for an unknown transaction id we didn't perform, ignoring it", addr);
                    return;
                }
                self.inflght_requests.remove(&transaction_id);
                // todo refresh node in routing table
            }
            KRPCMessage::FindNodeReq(querying_node_it, target_node_id) => {
                if !self.inflght_requests.contains_key(&transaction_id) {
                    log::warn!(
                        "got a find_node resp for an unknown transaction id we didn't perform"
                    );
                } else {
                    self.inflght_requests.remove(&transaction_id);
                    // todo put queried_node_id in routing table
                    log::warn!(
                        "find node req: querying_node_it: {} target_node_id: {}",
                        force_string(&querying_node_it.to_vec()),
                        force_string(&target_node_id.to_vec())
                    );
                }
            }
            KRPCMessage::FindNodeResp(_, _) => {}
            KRPCMessage::GetPeersReq(_, _) => {}
            KRPCMessage::GetPeersResp(_, _, _) => {}
            KRPCMessage::AnnouncePeerReq(_, _, _, _, _) => {}
            KRPCMessage::Error(_, _) => {}
        }
    }
}

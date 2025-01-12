use std::{
    cmp,
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
    time::{Duration, SystemTime},
};

use num_bigint::BigUint;
use sha1::{Digest, Sha1};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{self, Receiver, Sender},
};

use rand::Rng;

use crate::{
    dht::{
        messages::{
            decode_krpc_message, encode_krpc_message, ErrorType, GetPeersOrFindNodeRespData,
        },
        routing_table::{biguint_to_u8_20, distance, Node, K_FACTOR},
    },
    util::{force_string, pretty_info_hash, start_tick},
};

use super::{messages::KRPCMessage, routing_table::Bucket};

// NOTE! we are only supporting IPv4 DHT, i.e. BEP 32 (https://www.bittorrent.org/beps/bep_0032.html) is not implemented

const INFLIGHT_FIND_NODE_TIMEOUT: Duration = Duration::from_secs(12);
const INFLIGHT_GET_PEERS_TIMEOUT: Duration = Duration::from_secs(12);
const INFLIGHT_REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const ROUTING_TABLE_REFRESH_TIME: Duration = Duration::from_secs(60);

const WELL_KNOWN_BOOTSTRAP_NODES: &[&str] = &[
    "dht.libtorrent.org:25401",
    "router.utorrent.com:6881",
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.aelitis.com:6881",
];

// todo: we should use only 2 chars and all the possible chars here, but this is much more useful during debugging
fn generate_transaction_id() -> [u8; 10] {
    const CHARSET: &[u8] = b"0123456789";
    let mut rng = rand::thread_rng();
    let mut one_char = || CHARSET[rng.gen_range(0..CHARSET.len())] as u8;
    let mut transaction_id: [u8; 10] = [0u8; 10];
    for i in 0..10 {
        transaction_id[i] = one_char();
    }
    transaction_id
}

pub enum ToDhtManagerMsg {
    GetNewPeers([u8; 20]), // info hash
    NewNode(String),       // addr of new node, i.e. "host:port" string
    ConnectedToNewPeer([u8; 20], Ipv4Addr, u16),
}

pub enum DhtToTorrentManagerMsg {
    NewPeer(Ipv4Addr, u16),
}

pub struct DhtManager {
    listening_dht_port: u16,
    listening_torrent_wire_protocol_port: u16,
    own_node_id: [u8; 20],
    bootstrap_nodes: Vec<String>,
    msg_sender: MessageSender,
    routing_table: Bucket,
    token_signing_secret: [u8; 10], // todo: we must rotate this once in a while
    known_peers: HashMap<[u8; 20], HashMap<(Ipv4Addr, u16), SystemTime>>, // info hash -> hashmap of (ip/port of peers that have it -> last announced); todo: should we expire keys here once in a while / if keys are too many
    inflight_get_peers_requests: HashMap<[u8; 20], GetPeersRequest>, // info hash -> GetPeersRequest
    inflight_find_node_requests: HashMap<[u8; 20], FindNodeRequest>, // random id -> FindNodeRequest
    last_routing_table_refresh: SystemTime,
}

struct GetPeersRequest {
    start_time: SystemTime,
    total_requests: usize,
    inflight_requests: usize,
    queried_nodes: HashSet<[u8; 20]>, // nodes for which we asked a get_peers request
    replying_nodes: Vec<([u8; 20], Ipv4Addr, u16, Option<Vec<u8>>)>, // list of nodes for which we got replies, that we can use in later announce_peer requests
    replying_nodes_with_peers: usize,
    discovered_peers: HashSet<(Ipv4Addr, u16)>, // lits of peers we discovered
}

struct FindNodeRequest {
    node_id_to_find: [u8; 20],
    start_time: SystemTime,
    total_requests: usize,
    inflight_requests: usize,
    total_discovered_nodes: usize,
    queried_nodes: HashSet<[u8; 20]>, // nodes for which we asked a find_node request
    closest_k_nodes: Vec<([u8; 20], Ipv4Addr, u16)>,
    probed_nodes: HashSet<[u8; 20]>, // nodes we sent a ping to eventually put then in the routing table
}

struct MessageSender {
    // transaction id -> , req time, message,,
    inflight_requests: HashMap<
        Vec<u8>,
        (
            String,           // dest addr
            SystemTime,       // req time
            KRPCMessage,      // message
            Option<[u8; 20]>, // optinal request id (info hash or random id the request related to) in case it was a get_peers or find_node request
            usize, // depth of the nested request, if it is recursive (e.g. in case of get_peers or find_node)
        ),
    >,
}

impl MessageSender {
    pub fn new() -> Self {
        MessageSender {
            inflight_requests: HashMap::new(),
        }
    }

    pub fn remove_expired(&mut self) {
        let now = SystemTime::now();
        self.inflight_requests
            .retain(|_, (_, t, _, _, _)| now.duration_since(*t).unwrap() < INFLIGHT_REQUEST_TIMEOUT)
    }

    pub async fn do_req(
        &mut self,
        socket: &UdpSocket,
        dest: String,
        msg: KRPCMessage,
        request_id: Option<[u8; 20]>,
        call_depth: usize,
    ) {
        let tid = generate_transaction_id();
        log::trace!(
            "perform req to {}, depth: {call_depth}, tid: {}, msg: {msg:?}",
            dest.clone(),
            force_string(&tid.to_vec()),
        );
        let buf = encode_krpc_message(tid.to_vec(), msg.clone());
        self.inflight_requests.insert(
            tid.to_vec(),
            (
                dest.clone(),
                SystemTime::now(),
                msg.clone(),
                request_id,
                call_depth,
            ),
        );
        if let Err(e) = socket.send_to(&buf, dest.clone()).await {
            // don't care if we cannot send it, due to routing issues or others (if it was for buffer full, send_to would have blocked)
            log::debug!("could not send dht message {msg:?} to {dest}: {e}");
        };
    }
}

impl DhtManager {
    pub fn new(
        listening_torrent_wire_protocol_port: u16,
        listening_dht_port: u16,
        mut bootstrap_nodes: Vec<String>,
    ) -> DhtManager {
        for n in WELL_KNOWN_BOOTSTRAP_NODES {
            bootstrap_nodes.push(n.to_string());
        }
        let mut own_node_id: [u8; 20] = [0u8; 20];
        for i in 0..20 {
            own_node_id[i] = rand::random();
        }
        let mut token_signing_secret: [u8; 10] = [0u8; 10];
        for i in 0..10 {
            token_signing_secret[i] = rand::random();
        }
        return DhtManager {
            listening_dht_port,
            listening_torrent_wire_protocol_port,
            own_node_id,
            bootstrap_nodes,
            msg_sender: MessageSender::new(),
            routing_table: Bucket::new(&own_node_id),
            token_signing_secret,
            known_peers: HashMap::new(),
            inflight_get_peers_requests: HashMap::new(),
            inflight_find_node_requests: HashMap::new(),
            last_routing_table_refresh: SystemTime::now(),
        };
    }

    pub async fn start(
        &mut self,
        mut to_dht_manager_rx: Receiver<ToDhtManagerMsg>,
        dht_to_torrent_manager_tx: Sender<DhtToTorrentManagerMsg>,
    ) {
        // start ticker
        let (tick_tx, mut tick_rx) = mpsc::channel(1);
        start_tick(tick_tx, Duration::from_secs(1)).await;

        // open ipv4 socket
        let socket = UdpSocket::bind(format!("0.0.0.0:{}", self.listening_dht_port))
            .await
            .unwrap();

        // bootstrap initial known nodes by finding nodes closest to self
        for i in 0..self.bootstrap_nodes.len() {
            self.find_node(self.bootstrap_nodes[i].clone(), &socket, self.own_node_id)
                .await;
        }

        let mut msg_buf: [u8; 65535] = [0u8; 65535]; // max udp datagram size
        loop {
            tokio::select! {
                Ok((msg_size, remote_addr)) = socket.recv_from(&mut msg_buf) => {
                    match decode_krpc_message(msg_buf[0..msg_size].to_vec()) {
                        Err(e) => {
                            log::trace!("error decoding incoming dht message from {remote_addr}: {e}, udp message was: {msg_buf:?}");
                        }
                        Ok((transaction_id, msg)) => {
                            self.handle_incoming_message(remote_addr, &socket, transaction_id, msg, &dht_to_torrent_manager_tx).await;
                        }
                    }
                }
                Some(()) = tick_rx.recv() => {
                    log::debug!("routing table size: {}, inflight requests: {}, inflight get_peers requests: {}, inflight find_nodes_requests: {}",
                    self.routing_table.as_mut_vec().len(), // todo: optimize this
                    self.msg_sender.inflight_requests.len(), self.inflight_get_peers_requests.len(), self.inflight_find_node_requests.len());
                    self.handle_ticker(&socket).await;
                }
                Some(msg) = to_dht_manager_rx.recv() => {
                    match msg {
                        ToDhtManagerMsg::GetNewPeers(info_hash) => {
                            log::trace!("got GetNewPeers msg from torrent manager: {}", pretty_info_hash(info_hash));
                            self.get_peers(&socket, info_hash).await;
                        }
                        ToDhtManagerMsg::NewNode(new_node_addr) => {
                            log::trace!("got NewNode msg from torrent manager: {new_node_addr}");
                            // ping new dht node as validation to eventually put it in routing table
                            self.msg_sender.do_req(
                                &socket,
                                new_node_addr,
                                KRPCMessage::PingReq(self.own_node_id),
                                None,
                                0
                            ).await;
                        }
                        ToDhtManagerMsg::ConnectedToNewPeer(info_hash, new_peer_addr, new_peer_port) => {
                            log::trace!("got ConnectedToNewPeer msg from torrent manager: {new_peer_addr}");
                            let peer_info = (new_peer_addr, new_peer_port);
                            match self.known_peers.get_mut(&info_hash) {
                                Some(peers_for_this_info_hash) => {
                                    peers_for_this_info_hash.insert(peer_info, SystemTime::now());
                                }
                                None => {
                                    let mut peers_for_this_info_hash = HashMap::new();
                                    peers_for_this_info_hash.insert(peer_info, SystemTime::now());
                                    self.known_peers.insert(info_hash, peers_for_this_info_hash);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn handle_ticker(&mut self, socket: &UdpSocket) {
        let now = SystemTime::now();

        // expire inflight requests
        self.msg_sender.remove_expired();

        // terminate expired get_peers requests
        let expired_get_peers_requests: Vec<[u8; 20]> = self
            .inflight_get_peers_requests
            .iter()
            .filter(|(_, v)| now.duration_since(v.start_time).unwrap() > INFLIGHT_GET_PEERS_TIMEOUT)
            .map(|(k, _)| *k)
            .collect();
        for info_hash in expired_get_peers_requests {
            self.end_get_peers_search(&info_hash, socket).await;
        }

        // terminate expired find_node requests
        let expired_find_nodes_requests: Vec<[u8; 20]> = self
            .inflight_find_node_requests
            .iter()
            .filter(|(_, v)| now.duration_since(v.start_time).unwrap() > INFLIGHT_FIND_NODE_TIMEOUT)
            .map(|(k, _)| *k)
            .collect();
        for req_id in expired_find_nodes_requests {
            self.end_find_node_search(&req_id).await;
        }

        // routing table maintenance
        let mut nodes_to_be_removed = Vec::new();

        for n in self.routing_table.as_mut_vec() {
            // ping nodes if not seen a reply in the last 10 minutes and last pinged less than 2 minutes ago
            if now.duration_since(n.last_replied).unwrap() > Duration::from_secs(600)
                && now.duration_since(n.last_pinged).unwrap() > Duration::from_secs(60)
            {
                n.last_pinged = now;
                self.msg_sender
                    .do_req(
                        socket,
                        to_addr_string(&n.addr, n.port),
                        KRPCMessage::PingReq(self.own_node_id),
                        None,
                        0,
                    )
                    .await;
            }

            // accumulate nodes to be removed if not active anymore in the last 15 mins
            if now.duration_since(n.last_replied).unwrap() > Duration::from_secs(900) {
                nodes_to_be_removed.push(n.clone());
            }
        }

        for n in nodes_to_be_removed {
            self.routing_table.remove(&n);
            // this is out of official bep05 specs but we do that to replace removed nodes: if we remove a node, we send a find_node with the removed id to the closest nodes
            // we do this instead of refreshing buckets if withing 15m the bucket was not changed. Todo: maybe change this
            let n_id = biguint_to_u8_20(&n.id);
            let closest = self.routing_table.closest_nodes(&n_id);
            for c in closest {
                self.find_node(to_addr_string(&c.addr, c.port), socket, n_id)
                    .await;
            }
        }

        // every 1m randomly find new nodes - this is out of official bep05 specs, we do this to have a bigger routing table - maybe remove this
        if now.duration_since(self.last_routing_table_refresh).unwrap() > ROUTING_TABLE_REFRESH_TIME
        {
            self.last_routing_table_refresh = now;
            let mut random_id: [u8; 20] = [0u8; 20];
            for i in 0..20 {
                random_id[i] = rand::random();
            }
            let closest = self.routing_table.closest_nodes(&random_id);
            for c in closest {
                self.find_node(to_addr_string(&c.addr, c.port), socket, random_id)
                    .await;
            }
        }
    }

    async fn find_node(&mut self, to_addr: String, socket: &UdpSocket, node_id: [u8; 20]) {
        let mut random_id: [u8; 20] = [0u8; 20];
        for i in 0..20 {
            random_id[i] = rand::random();
        }

        let get_node_request = FindNodeRequest {
            node_id_to_find: node_id,
            start_time: SystemTime::now(),
            total_requests: 1,
            inflight_requests: 1,
            total_discovered_nodes: 0,
            queried_nodes: HashSet::new(),
            closest_k_nodes: Vec::new(),
            probed_nodes: HashSet::new(),
        };
        self.inflight_find_node_requests
            .insert(random_id, get_node_request);

        self.msg_sender
            .do_req(
                &socket,
                to_addr,
                KRPCMessage::FindNodeReq(self.own_node_id, node_id),
                Some(random_id),
                0,
            )
            .await;
    }

    async fn get_peers(&mut self, socket: &UdpSocket, info_hash: [u8; 20]) {
        if self.inflight_get_peers_requests.contains_key(&info_hash) {
            log::debug!(
                "ignoring get_peers request for info hash {} since another one is still in flight",
                force_string(&info_hash.to_vec())
            );
            return;
        }

        // perform get_peers request to the closest known good nodes
        let closest_nodes = self.routing_table.closest_nodes(&info_hash);

        let get_peers_request = GetPeersRequest {
            start_time: SystemTime::now(),
            total_requests: closest_nodes.len(),
            inflight_requests: closest_nodes.len(),
            queried_nodes: closest_nodes
                .iter()
                .map(|n| biguint_to_u8_20(&n.id))
                .collect(),
            replying_nodes: Vec::new(),
            replying_nodes_with_peers: 0,
            discovered_peers: HashSet::new(),
        };
        self.inflight_get_peers_requests
            .insert(info_hash, get_peers_request);

        for n in closest_nodes.iter() {
            self.msg_sender
                .do_req(
                    socket,
                    to_addr_string(&n.addr, n.port),
                    KRPCMessage::GetPeersReq(self.own_node_id, info_hash),
                    Some(info_hash),
                    0,
                )
                .await;
        }
    }

    async fn handle_incoming_message(
        &mut self,
        remote_addr: SocketAddr,
        socket: &UdpSocket,
        transaction_id: Vec<u8>,
        msg: KRPCMessage,
        dht_to_torrent_manager_tx: &Sender<DhtToTorrentManagerMsg>,
    ) {
        log::trace!(
            "got message from {remote_addr}: tid: {}, msg: {msg:?}",
            force_string(&transaction_id),
        );
        let remote_port = remote_addr.port();
        let remote_ipv4addr = match remote_addr.ip() {
            IpAddr::V4(ipv4addr) => ipv4addr,
            IpAddr::V6(_) => {
                log::warn!("got ipv6 response from dht message, this should not be possible");
                return;
            }
        };

        match msg {
            KRPCMessage::PingReq(_querying_node_id) => {
                self.msg_sender
                    .do_req(
                        socket,
                        remote_addr.to_string(),
                        KRPCMessage::PingOrAnnouncePeerResp(self.own_node_id),
                        None,
                        0,
                    )
                    .await;
            }

            KRPCMessage::PingOrAnnouncePeerResp(queried_node_id) => {
                self.handle_ping_or_announce_peer_resp(
                    remote_ipv4addr,
                    remote_port,
                    socket,
                    transaction_id,
                    queried_node_id,
                )
                .await;
            }

            KRPCMessage::FindNodeReq(querying_node_id, target_node_id) => {
                self.handle_find_node_req(remote_addr, socket, querying_node_id, target_node_id)
                    .await;
            }

            KRPCMessage::GetPeersReq(_querying_node_id, info_hash) => {
                self.handle_get_peers_req(remote_ipv4addr, remote_port, socket, info_hash)
                    .await
            }

            KRPCMessage::GetPeersOrFindNodeResp(resp_data) => {
                let (original_request_id, call_depth) = match self
                    .msg_sender
                    .inflight_requests
                    .remove(&transaction_id)
                {
                    Some((_, _, _, Some(info_hash), depth)) => (info_hash, depth),
                    _ => {
                        log::trace!("got a get_peers or find_node resp from {remote_addr} for an expired or unknown transaction id ({}) we didn't perform, ignoring it",
                            force_string(&transaction_id.to_vec())
                        );
                        return;
                    }
                };
                // if it was a get_peers response...
                if let Some(mut original_request) = self
                    .inflight_get_peers_requests
                    .remove(&original_request_id)
                {
                    self.handle_get_peers_resp(
                        remote_ipv4addr,
                        remote_port,
                        socket,
                        original_request_id,
                        &mut original_request,
                        call_depth,
                        resp_data,
                        dht_to_torrent_manager_tx,
                    )
                    .await;
                    self.inflight_get_peers_requests
                        .insert(original_request_id, original_request);
                    return;
                }
                // if instead it was a find_node response...
                if let Some(mut original_request) = self
                    .inflight_find_node_requests
                    .remove(&original_request_id)
                {
                    self.handle_find_node_resp(
                        remote_ipv4addr,
                        remote_port,
                        socket,
                        original_request_id,
                        &mut original_request,
                        call_depth,
                        resp_data,
                    )
                    .await;
                    self.inflight_find_node_requests
                        .insert(original_request_id, original_request);
                    return;
                }
                log::trace!(
                    "got a get_peers or find_node resp from {remote_addr} for a request ({}) we do not have track of, maybe it expired, ignoring it",
                    force_string(&original_request_id.to_vec())
                );
            }

            KRPCMessage::AnnouncePeerReq(
                _querying_node_id,
                info_hash,
                announce_peer_port,
                token,
                imply_port,
            ) => {
                self.handle_announce_peer_req(
                    remote_ipv4addr,
                    remote_port,
                    socket,
                    info_hash,
                    token,
                    announce_peer_port,
                    imply_port,
                )
                .await;
                return;
            }

            KRPCMessage::Error(error_type, msg) => {
                if !self
                    .msg_sender
                    .inflight_requests
                    .contains_key(&transaction_id)
                {
                    log::trace!("got a error resp from {} for an unknown or expired transaction id ({}) we didn't perform, ignoring it",
                        remote_addr, force_string(&transaction_id.to_vec())
                    );
                    return;
                }
                let inflight_req = self
                    .msg_sender
                    .inflight_requests
                    .remove(&transaction_id)
                    .unwrap();
                log::trace!("got dht error respose for transaction id {} from {}; our message sent was: {:?}, error type: {:?}, error message: {}",
                    force_string(&transaction_id),remote_addr, inflight_req.2, error_type, msg
                );
            }
        }
    }

    async fn end_get_peers_search(&mut self, info_hash: &[u8; 20], socket: &UdpSocket) {
        if let Some(req) = self.inflight_get_peers_requests.remove(info_hash) {
            // send announce_peer to closest K nodes
            for i in 0..cmp::min(req.replying_nodes.len(), K_FACTOR) {
                let (_, ip, port, token) = &req.replying_nodes[i];
                if let Some(token_val) = token {
                    self.msg_sender
                        .do_req(
                            socket,
                            to_addr_string(&ip, *port),
                            KRPCMessage::AnnouncePeerReq(
                                self.own_node_id,
                                *info_hash,
                                self.listening_torrent_wire_protocol_port,
                                token_val.clone(),
                                false,
                            ),
                            None,
                            0,
                        )
                        .await;
                }
            }

            log::info!(
                "get_peers request terminated: routing table size: {}, total sent requests: {} not replied: {}, replied: {}, replied with peers: {}, discovered peers: {}",
                self.routing_table.as_mut_vec().len(), // todo optimize this
                req.total_requests,
                req.inflight_requests,
                req.replying_nodes.len(),
                req.replying_nodes_with_peers,
                req.discovered_peers.len(),
            );
        }
    }

    async fn end_find_node_search(&mut self, req_id: &[u8; 20]) {
        if let Some(req) = self.inflight_find_node_requests.remove(req_id) {
            log::debug!(
                "find_node request for {} terminated: total sent requests: {} not replied: {}, discovered nodes: {}, probed nodes for routing table addition: {}",
                force_string(&req.node_id_to_find.to_vec()),
                req.total_requests,
                req.inflight_requests,
                req.total_discovered_nodes,
                req.probed_nodes.len(),
            );
        }
    }

    async fn handle_get_peers_resp(
        &mut self,
        remote_ipv4addr: Ipv4Addr,
        remote_port: u16,
        socket: &UdpSocket,
        original_request_id: [u8; 20],
        original_request: &mut GetPeersRequest,
        call_depth: usize,
        resp_data: GetPeersOrFindNodeRespData,
        dht_to_torrent_manager_tx: &Sender<DhtToTorrentManagerMsg>,
    ) {
        // in case of get_peers request, we set the id of the request same as the infohash,
        // since we are not allowing multiple concurrent get_peers searches
        let info_hash = original_request_id;

        if original_request.inflight_requests > 0 {
            // avoid overflowing when by pure bad luck we get a late replay for a not yet expired msg request for
            // an expired get_peers request that has been recreated immediatelly after the previous has expired
            original_request.inflight_requests -= 1;
        }

        // update replying nodes and sort by distance to target
        original_request.replying_nodes.push((
            resp_data.target_id,
            remote_ipv4addr,
            remote_port,
            resp_data.token,
        ));
        original_request
            .replying_nodes
            .sort_by_key(|(node_id, _, _, _)| distance(&info_hash, node_id));

        // act on response: nodes
        if let Some(nodes) = resp_data.nodes {
            let mut max_distance_for_new_req =
                BigUint::from_str("2").unwrap().pow(160) - BigUint::from_str("1").unwrap();
            if original_request.replying_nodes.len() != 0 {
                max_distance_for_new_req = distance(
                    &info_hash,
                    &original_request.replying_nodes
                        [cmp::min(original_request.replying_nodes.len(), K_FACTOR) - 1]
                        .0,
                );
            }
            for (node_id, ip, port) in nodes {
                if
                // we don't have already performed a request to this node
                !original_request.queried_nodes.contains(&node_id) &&
            // if node is closer than the K_FACTORth closer node we queried, perform iterative request
            distance(&info_hash, &node_id) <= max_distance_for_new_req
                {
                    self.msg_sender
                        .do_req(
                            socket,
                            to_addr_string(&ip, port),
                            KRPCMessage::GetPeersReq(self.own_node_id, info_hash),
                            Some(info_hash),
                            call_depth + 1,
                        )
                        .await;
                    original_request.total_requests += 1;
                    original_request.inflight_requests += 1;
                    original_request.queried_nodes.insert(node_id);
                }
            }
        }

        // act on response: peers
        if let Some(peers) = resp_data.values {
            if peers.len() > 0 {
                original_request.replying_nodes_with_peers += 1;
            }

            for p in peers {
                if !original_request.discovered_peers.contains(&p) {
                    original_request.discovered_peers.insert(p);
                    // immediatelly send discovered peer to torrent manager
                    let _ = dht_to_torrent_manager_tx
                        .send(DhtToTorrentManagerMsg::NewPeer(p.0, p.1))
                        .await;
                }
            }
            if original_request.inflight_requests == 0 {
                // search is over
                self.end_get_peers_search(&original_request_id, socket)
                    .await;
            }
        }
    }

    async fn handle_find_node_resp(
        &mut self,
        remote_ipv4addr: Ipv4Addr,
        remote_port: u16,
        socket: &UdpSocket,
        original_request_id: [u8; 20],
        original_request: &mut FindNodeRequest,
        call_depth: usize,
        resp_data: GetPeersOrFindNodeRespData,
    ) {
        let nodes = match resp_data.nodes {
            Some(nodes) => nodes,
            None => {
                log::trace!("we correlated a GetPeersOrFindNodeResp to an existing find_node request but the response had no nodes, skipping it");
                return;
            }
        };

        if original_request.inflight_requests > 0 {
            // avoid overflowing in special cases similar to get_peer
            original_request.inflight_requests -= 1;
        }

        // add or refresh this replying node to the routing table
        self.routing_table
            .add(Node::new(resp_data.target_id, remote_ipv4addr, remote_port));

        for (node_id, addr, port) in nodes {
            if addr == Ipv4Addr::new(0, 0, 0, 0) || port == 0 {
                // sometimes it happens that replies contain invalid addresses
                continue;
            }
            original_request.total_discovered_nodes += 1;

            // ping newly discovered nodes to eventually put them in the routing table
            if let None = self.routing_table.get_mut(&node_id) {
                if !original_request.probed_nodes.contains(&node_id) {
                    original_request.probed_nodes.insert(node_id);
                    self.msg_sender
                        .do_req(
                            socket,
                            to_addr_string(&addr, port),
                            KRPCMessage::PingReq(self.own_node_id),
                            None,
                            0,
                        )
                        .await;
                }
            }

            // avoid sending find_node to a node we already asked
            if original_request.queried_nodes.contains(&node_id) {
                continue;
            }
            // add this node among the best ones, if it is
            original_request.closest_k_nodes.push((node_id, addr, port));
            original_request
                .closest_k_nodes
                .sort_by_key(|(n_id, _, _)| distance(&original_request.node_id_to_find, n_id));
            original_request.closest_k_nodes.truncate(K_FACTOR);

            // if it is one of the best, query it
            if distance(&original_request.node_id_to_find, &node_id)
                <= distance(
                    &original_request.node_id_to_find,
                    &original_request.closest_k_nodes.last().unwrap().0,
                )
            {
                self.msg_sender
                    .do_req(
                        socket,
                        to_addr_string(&addr, port),
                        KRPCMessage::FindNodeReq(
                            self.own_node_id,
                            original_request.node_id_to_find,
                        ),
                        Some(original_request_id),
                        call_depth + 1,
                    )
                    .await;
                original_request.total_requests += 1;
                original_request.inflight_requests += 1;
                original_request.queried_nodes.insert(node_id);
            }
        }

        if original_request.inflight_requests == 0 {
            // search is over, let's gather results
            self.end_find_node_search(&original_request_id).await;
        }
        return;
    }

    async fn handle_announce_peer_req(
        &mut self,
        remote_ipv4addr: Ipv4Addr,
        remote_port: u16,
        socket: &UdpSocket,
        info_hash: [u8; 20],
        token: Vec<u8>,
        announce_peer_port: u16,
        imply_port: bool,
    ) {
        let source_req_addr_port = to_addr_string(&remote_ipv4addr, remote_port);
        // verify token
        let token_u8_20: [u8; 20] = match token.try_into() {
            Ok(t) => t,
            Err(_) => {
                log::trace!("got an announce_peer from {source_req_addr_port} with a token that is not 20b as expected, refusing it");
                self.msg_sender
                    .do_req(
                        &socket,
                        source_req_addr_port,
                        KRPCMessage::Error(
                            ErrorType::GenericError,
                            "wrong secret, should be 20b long".to_string(),
                        ),
                        None,
                        0,
                    )
                    .await;
                return;
            }
        };
        let mut expected_token_plain = self.token_signing_secret.to_vec();
        expected_token_plain.append(&mut remote_ipv4addr.octets().to_vec());
        let expected_token: [u8; 20] = Sha1::digest(expected_token_plain)
            .as_slice()
            .try_into()
            .unwrap();
        if expected_token != token_u8_20 {
            log::trace!(
        "got an announce_peer from {source_req_addr_port} with a token ({}) that is not what we expected ({}), refusing it",
        force_string(&token_u8_20.to_vec()),
        force_string(&expected_token.to_vec())
    );
            self.msg_sender
                .do_req(
                    &socket,
                    source_req_addr_port,
                    KRPCMessage::Error(ErrorType::GenericError, "wrong secret content".to_string()),
                    None,
                    0,
                )
                .await;
            return;
        }
        // store / refresh peer info for this info hash
        let mut peer_port = announce_peer_port;
        if imply_port {
            peer_port = remote_port;
        }
        let peer_info = (remote_ipv4addr, peer_port);
        match self.known_peers.get_mut(&info_hash) {
            Some(s) => {
                s.insert(peer_info, SystemTime::now());
            }
            None => {
                let mut s = HashMap::new();
                s.insert(peer_info, SystemTime::now());
                self.known_peers.insert(info_hash, s);
            }
        }
        // send ok
        self.msg_sender
            .do_req(
                &socket,
                source_req_addr_port,
                KRPCMessage::PingOrAnnouncePeerResp(self.own_node_id),
                None,
                0,
            )
            .await;
    }

    async fn handle_ping_or_announce_peer_resp(
        &mut self,
        remote_ipv4addr: Ipv4Addr,
        remote_port: u16,
        socket: &UdpSocket,
        transaction_id: Vec<u8>,
        queried_node_id: [u8; 20],
    ) {
        if !self
            .msg_sender
            .inflight_requests
            .contains_key(&transaction_id)
        {
            log::trace!("got a ping or announce_peer resp from {remote_ipv4addr}:{remote_port} for an expired or unknown transaction id ({}) we didn't perform, ignoring it", force_string(&transaction_id.to_vec()));
            return;
        }
        self.msg_sender.inflight_requests.remove(&transaction_id);
        // the node is valid, refresh last replied / add node to the routing table
        let added =
            self.routing_table
                .add(Node::new(queried_node_id, remote_ipv4addr, remote_port));
        // this is out of official spec of bep05 but we do this to accumulate more nodes in the routing table, todo maybe remove this
        if added {
            // find_node for a random node near the new one
            let mut random_close_node_id = queried_node_id.clone();
            random_close_node_id[19] ^= rand::random::<u8>();
            self.find_node(
                to_addr_string(&remote_ipv4addr, remote_port),
                socket,
                queried_node_id,
            )
            .await;
        }
    }

    async fn handle_get_peers_req(
        &mut self,
        remote_ipv4addr: Ipv4Addr,
        remote_port: u16,
        socket: &UdpSocket,
        info_hash: [u8; 20],
    ) {
        let source_req_addr_port = to_addr_string(&remote_ipv4addr, remote_port);
        // generate token
        let mut token_plain = self.token_signing_secret.to_vec();
        token_plain.append(&mut remote_ipv4addr.octets().to_vec());
        let token: [u8; 20] = Sha1::digest(token_plain).as_slice().try_into().unwrap();
        // response
        match self.known_peers.get(&info_hash) {
            Some(peers) => {
                let mut resp_peers_info: Vec<(Ipv4Addr, u16)> =
                    peers.iter().map(|(k, _v)| *k).collect();
                resp_peers_info.truncate(8000); // do not overflow a single udp packet. todo: do a better calculation
                self.msg_sender
                    .do_req(
                        socket,
                        source_req_addr_port.to_string(),
                        KRPCMessage::GetPeersOrFindNodeResp(GetPeersOrFindNodeRespData {
                            target_id: self.own_node_id,
                            token: Some(token.to_vec()),
                            nodes: None, // todo: note that some clients also send closest nodes on get_peers requests even if they know some peers, should we do the same?
                            values: Some(resp_peers_info),
                        }),
                        None,
                        0,
                    )
                    .await;
            }
            None => {
                let closest_nodes_info: Vec<([u8; 20], Ipv4Addr, u16)> = self
                    .routing_table
                    .closest_nodes(&info_hash)
                    .iter()
                    .map(|n| (biguint_to_u8_20(&n.id), n.addr, n.port))
                    .collect();
                self.msg_sender
                    .do_req(
                        socket,
                        source_req_addr_port.to_string(),
                        KRPCMessage::GetPeersOrFindNodeResp(GetPeersOrFindNodeRespData {
                            target_id: self.own_node_id,
                            token: Some(token.to_vec()),
                            nodes: Some(closest_nodes_info),
                            values: None,
                        }),
                        None,
                        0,
                    )
                    .await;
            }
        }
    }

    async fn handle_find_node_req(
        &mut self,
        remote_addr: SocketAddr,
        socket: &UdpSocket,
        querying_node_id: [u8; 20],
        target_node_id: [u8; 20],
    ) {
        let closest_nodes = self.routing_table.closest_nodes(&target_node_id);
        self.msg_sender
            .do_req(
                socket,
                remote_addr.to_string(),
                KRPCMessage::GetPeersOrFindNodeResp(GetPeersOrFindNodeRespData {
                    target_id: self.own_node_id,
                    token: None,
                    nodes: Some(
                        closest_nodes
                            .iter()
                            .map(|n| (biguint_to_u8_20(&n.id), n.addr, n.port))
                            .collect(),
                    ),
                    values: None,
                }),
                None,
                0,
            )
            .await;

        // if we don't have it in the routing table, ping this node to eventually put it in the routing table:
        // it could be a new node bootsrapping that is trying to let himself know
        if let None = self.routing_table.get_mut(&querying_node_id) {
            self.msg_sender
                .do_req(
                    socket,
                    remote_addr.to_string(),
                    KRPCMessage::PingReq(self.own_node_id),
                    None,
                    0,
                )
                .await;
        }
    }
}

fn to_addr_string(addr: &Ipv4Addr, port: u16) -> String {
    format!("{}:{}", addr.to_string(), port)
}

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

use crate::{
    dht::{
        messages::{
            decode_krpc_message, encode_krpc_message, ErrorType, GetPeersRespValuesOrNodes,
        },
        routing_table::{biguint_to_u8_20, distance, Node},
    },
    util::{force_string, pretty_info_hash, start_tick},
};

use super::{messages::KRPCMessage, routing_table::Bucket};

// NOTE! we are only supporting IPv4 DHT, i.e. BEP 32 (https://www.bittorrent.org/beps/bep_0032.html) is not implemented

static K_FACTOR: usize = 8;

static INFLIGHT_FIND_NODE_TIMEOUT: Duration = Duration::from_secs(25);
static INFLIGHT_GET_PEERS_TIMEOUT: Duration = Duration::from_secs(25);
static INFLIGHT_REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
static ROUTING_TABLE_REFRESH_TIME: Duration = Duration::from_secs(60);

static WELL_KNOWN_BOOTSTRAP_NODES: &[&str] = &[
    "dht.libtorrent.org:25401",
    "router.utorrent.com:6881",
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.aelitis.com:6881",
];

fn generate_transaction_id() -> [u8; 2] {
    [rand::random(), rand::random()]
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
    sender: MessageSender,
    routing_table: Bucket,
    token_signing_secret: [u8; 10], // todo: we must rotate this once in a while
    known_peers: HashMap<[u8; 20], HashMap<(Ipv4Addr, u16), SystemTime>>, // info hash -> hasmap of (ip/port of peers that have it -> last announced); todo: should we expire keys here once in a while / if keys are too many
    inflight_get_peers_requests: HashMap<[u8; 20], GetPeersRequest>, // info hash -> GetPeersRequest
    inflight_find_node_requests: HashMap<[u8; 20], FindNodeRequest>, // node id -> FindNodeRequest
    last_routing_table_refresh: SystemTime,
}

struct GetPeersRequest {
    start_time: SystemTime,
    total_requests: usize,
    inflight_requests: usize,
    queried_nodes: HashSet<[u8; 20]>, // nodes for which we asked a get_peer request
    replying_nodes: Vec<([u8; 20], Ipv4Addr, u16, Vec<u8>)>, // list of nodes for which we got replies, that we can use in later announce_peer requests
    discovered_peers: HashSet<(Ipv4Addr, u16)>,              // lits of peers we discovered
}

struct FindNodeRequest {
    node_id_to_find: [u8; 20],
    start_time: SystemTime,
    total_requests: usize,
    inflight_requests: usize,
    total_discovered_nodes: usize,
    requested_to_nodes: HashSet<[u8; 20]>, // nodes for which we asked a find_node request
    closest_k_nodes: Vec<([u8; 20], Ipv4Addr, u16)>,
    probed_nodes: HashSet<[u8; 20]>, // nodes we sent a ping to eventually put then in the routing table
}

struct MessageSender {
    // transaction id -> dest addr, req time, message, optinal info hash or random id the request related to, in case it was a get_peer or find_node request
    inflight_requests: HashMap<Vec<u8>, (String, SystemTime, KRPCMessage, Option<[u8; 20]>)>,
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
            .retain(|_, (_, t, _, _)| now.duration_since(*t).unwrap() < INFLIGHT_REQUEST_TIMEOUT)
    }

    pub async fn do_req(
        &mut self,
        socket: &UdpSocket,
        dest: String,
        msg: KRPCMessage,
        related_info: Option<[u8; 20]>,
    ) {
        let tid = generate_transaction_id();
        log::trace!(
            "perform req to {}, tid: {}, msg: {:?}",
            dest.clone(),
            force_string(&tid.to_vec()),
            msg
        );
        let buf = encode_krpc_message(tid.to_vec(), msg.clone());
        self.inflight_requests.insert(
            tid.to_vec(),
            (dest.clone(), SystemTime::now(), msg, related_info),
        );
        if let Err(e) = socket.send_to(&buf, dest.clone()).await {
            // don't care if we cannot send it, due to roting issues or others (if it was for buffer full, send_to would block)
            log::debug!("could not send dht message to {}: {}", dest, e);
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
            sender: MessageSender::new(),
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
                Ok((msg_size, addr)) = socket.recv_from(&mut msg_buf) => {
                    match decode_krpc_message(msg_buf[0..msg_size].to_vec()) {
                        Err(e) => {
                            log::trace!("error decoding incoming dht message from {}: {}, udp message was: {:?}", addr, e, msg_buf);
                        }
                        Ok((transaction_id, msg)) => {
                            self.handle_incoming_message(addr, transaction_id, msg, &socket, &dht_to_torrent_manager_tx).await;
                        }
                    }
                }
                Some(()) = tick_rx.recv() => {
                    log::info!("routing table size: {}, inflight requests: {}, inflight get_peer requests: {}, inflight find_nodes_requests: {}",
                    self.routing_table.as_mut_vec().len(), // todo: optimize this
                    self.sender.inflight_requests.len(), self.inflight_get_peers_requests.len(), self.inflight_find_node_requests.len());
                    self.handle_ticker(&dht_to_torrent_manager_tx, &socket).await;
                }
                Some(msg) = to_dht_manager_rx.recv() => {
                    match msg {
                        ToDhtManagerMsg::GetNewPeers(info_hash) => {
                            log::trace!("got GetNewPeers msg from torrent manager: {}", pretty_info_hash(info_hash));
                            self.get_peers(&socket, info_hash).await;
                        }
                        ToDhtManagerMsg::NewNode(addr) => {
                            log::trace!("got NewNode msg from torrent manager: {}", addr);
                            // ping new dht node as validation to eventually put it in routing table
                            self.sender.do_req(
                                &socket,
                                addr,
                                KRPCMessage::PingReq(self.own_node_id),
                                None
                            ).await;
                        }
                    }
                }
            }
        }
    }

    async fn handle_ticker(
        &mut self,
        dht_to_torrent_manager_tx: &Sender<DhtToTorrentManagerMsg>,
        socket: &UdpSocket,
    ) {
        let now = SystemTime::now();

        // expire inflight requests
        self.sender.remove_expired();

        // end expired get_peers requests
        let expired_get_peers_requests: Vec<[u8; 20]> = self
            .inflight_get_peers_requests
            .iter()
            .filter(|(_, v)| now.duration_since(v.start_time).unwrap() > INFLIGHT_GET_PEERS_TIMEOUT)
            .map(|(k, _)| *k)
            .collect();
        for info_hash in expired_get_peers_requests {
            self.end_get_peer_search(&info_hash, dht_to_torrent_manager_tx, socket)
                .await;
        }

        // end expired find_node requests
        let expired_find_nodes_requests: Vec<[u8; 20]> = self
            .inflight_find_node_requests
            .iter()
            .filter(|(_, v)| now.duration_since(v.start_time).unwrap() > INFLIGHT_FIND_NODE_TIMEOUT)
            .map(|(k, _)| *k)
            .collect();
        for random_id in expired_find_nodes_requests {
            self.end_find_node_search(&random_id).await;
        }

        // routing table maintenance
        let mut nodes_to_be_removed = Vec::new();
        {
            for n in self.routing_table.as_mut_vec() {
                // ping nodes if not seen a reply in the last 10 minutes and last pinged less than 2 minutes ago
                if now.duration_since(n.last_replied).unwrap() > Duration::from_secs(600)
                    && now.duration_since(n.last_pinged).unwrap() > Duration::from_secs(60)
                {
                    n.last_pinged = now;
                    self.sender
                        .do_req(
                            socket,
                            to_addr_string(&n.addr, n.port),
                            KRPCMessage::PingReq(self.own_node_id),
                            None,
                        )
                        .await;
                }

                // accumulate nodes to be removed if not active anymore in the last 15 mins
                if now.duration_since(n.last_replied).unwrap() > Duration::from_secs(900) {
                    nodes_to_be_removed.push(n.clone());
                }
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
        self.sender
            .do_req(
                &socket,
                to_addr,
                KRPCMessage::FindNodeReq(self.own_node_id, node_id),
                Some(random_id),
            )
            .await;

        let get_node_request = FindNodeRequest {
            node_id_to_find: node_id,
            start_time: SystemTime::now(),
            total_requests: 1,
            inflight_requests: 1,
            total_discovered_nodes: 0,
            requested_to_nodes: HashSet::new(),
            closest_k_nodes: Vec::new(),
            probed_nodes: HashSet::new(),
        };
        self.inflight_find_node_requests
            .insert(random_id, get_node_request);
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
        for n in closest_nodes.iter() {
            self.sender
                .do_req(
                    socket,
                    to_addr_string(&n.addr, n.port),
                    KRPCMessage::GetPeersReq(self.own_node_id, info_hash),
                    Some(info_hash),
                )
                .await;
        }

        let get_peer_request = GetPeersRequest {
            start_time: SystemTime::now(),
            total_requests: closest_nodes.len(),
            inflight_requests: closest_nodes.len(),
            queried_nodes: closest_nodes
                .iter()
                .map(|n| biguint_to_u8_20(&n.id))
                .collect(),
            replying_nodes: Vec::new(),
            discovered_peers: HashSet::new(),
        };
        self.inflight_get_peers_requests
            .insert(info_hash, get_peer_request);
    }

    async fn handle_incoming_message(
        &mut self,
        addr: SocketAddr,
        transaction_id: Vec<u8>,
        msg: KRPCMessage,
        socket: &UdpSocket,
        dht_to_torrent_manager_tx: &Sender<DhtToTorrentManagerMsg>,
    ) {
        log::trace!(
            "got message from {}: tid: {}, msg: {:?}",
            addr,
            force_string(&transaction_id),
            msg
        );
        let port = addr.port();
        let ipv4addr = match addr.ip() {
            IpAddr::V4(ipv4addr) => ipv4addr,
            IpAddr::V6(_) => {
                log::warn!("got ipv6 response from dht message, this should not be possible");
                return;
            }
        };
        match msg {
            KRPCMessage::PingReq(_querying_node_id) => {
                self.sender
                    .do_req(
                        socket,
                        addr.to_string(),
                        KRPCMessage::PingOrAnnouncePeerResp(self.own_node_id),
                        None,
                    )
                    .await;
            }
            KRPCMessage::PingOrAnnouncePeerResp(queried_node_id) => {
                if !self.sender.inflight_requests.contains_key(&transaction_id) {
                    log::trace!("got a ping or announce_peer resp from {} for an expired or unknown transaction id we didn't perform, ignoring it", addr);
                    return;
                }
                self.sender.inflight_requests.remove(&transaction_id);
                // the node is valid, refresh last replied / add node to the routing table
                let added = self
                    .routing_table
                    .add(Node::new(queried_node_id, ipv4addr, port));
                // this is out of official spec of bep05 but we do this to accumulate more nodes in the routing table, todo maybe remove this
                if added {
                    // find_node for a random node near the new one
                    let mut random_close_node_id = queried_node_id.clone();
                    random_close_node_id[19] ^= rand::random::<u8>();
                    self.find_node(to_addr_string(&ipv4addr, port), socket, queried_node_id)
                        .await;
                }
            }
            KRPCMessage::FindNodeReq(querying_node_id, target_node_id) => {
                // reply to node
                let closest = self.routing_table.closest_nodes(&target_node_id);
                self.sender
                    .do_req(
                        socket,
                        addr.to_string(),
                        KRPCMessage::FindNodeResp(
                            self.own_node_id,
                            closest
                                .iter()
                                .map(|n| (biguint_to_u8_20(&n.id), n.addr, n.port))
                                .collect(),
                        ),
                        None,
                    )
                    .await;

                // if we don't have it in the routing table, ping this node to eventually put it in the routing table: it could be a new node bootsrapping that is trying to let himself know
                if let None = self.routing_table.get_mut(&querying_node_id) {
                    self.sender
                        .do_req(
                            socket,
                            addr.to_string(),
                            KRPCMessage::PingReq(querying_node_id),
                            None,
                        )
                        .await;
                }
            }
            KRPCMessage::FindNodeResp(queried_node_id, nodes) => {
                let random_id = match self.sender.inflight_requests.remove(&transaction_id) {
                    Some((_, _, _, Some(random_id))) => random_id,
                    _ => {
                        log::trace!("got a find_node resp from {} for an expired or unknown transaction id we didn't perform, ignoring it", addr);
                        return;
                    }
                };
                let find_node_req = match self.inflight_find_node_requests.get_mut(&random_id) {
                    Some(find_node_req) => find_node_req,
                    None => {
                        log::trace!("got a find_node resp from {} for an expired find_node request, ignoring it", addr);
                        return;
                    }
                };

                if find_node_req.inflight_requests > 0 {
                    // avoid overflowing in special cases similar to get_peer
                    find_node_req.inflight_requests -= 1;
                }

                // add or refresh this replying node to the routing table
                self.routing_table
                    .add(Node::new(queried_node_id, ipv4addr, port));

                for (node_id, addr, port) in nodes {
                    find_node_req.total_discovered_nodes += 1;

                    // ping newly discovered nodes to eventually put them in the routing table
                    if let None = self.routing_table.get_mut(&node_id) {
                        if !find_node_req.probed_nodes.contains(&node_id) {
                            find_node_req.probed_nodes.insert(node_id);
                            self.sender
                                .do_req(
                                    socket,
                                    to_addr_string(&addr, port),
                                    KRPCMessage::PingReq(self.own_node_id),
                                    None,
                                )
                                .await;
                        }
                    }

                    // avoid sending find_node to a node we already asked
                    if find_node_req.requested_to_nodes.contains(&node_id) {
                        continue;
                    }
                    // add this node among the best ones, if it is
                    find_node_req.closest_k_nodes.push((node_id, addr, port));
                    find_node_req
                        .closest_k_nodes
                        .sort_by_key(|(n_id, _, _)| distance(&find_node_req.node_id_to_find, n_id));
                    find_node_req.closest_k_nodes.truncate(K_FACTOR);

                    // if it is one of the best, query it
                    if distance(&find_node_req.node_id_to_find, &node_id)
                        <= distance(
                            &find_node_req.node_id_to_find,
                            &find_node_req.closest_k_nodes.last().unwrap().0,
                        )
                    {
                        self.sender
                            .do_req(
                                socket,
                                to_addr_string(&addr, port),
                                KRPCMessage::FindNodeReq(
                                    self.own_node_id,
                                    find_node_req.node_id_to_find,
                                ),
                                Some(random_id),
                            )
                            .await;
                        find_node_req.total_requests += 1;
                        find_node_req.inflight_requests += 1;
                        find_node_req.requested_to_nodes.insert(node_id);
                    }
                }

                if find_node_req.inflight_requests == 0 {
                    // search is over, let's gather results
                    self.end_find_node_search(&random_id).await;
                }
            }
            KRPCMessage::GetPeersReq(_querying_node_id, info_hash) => {
                // generate token
                let mut token_plain = self.token_signing_secret.to_vec();
                token_plain.append(&mut ipv4addr.octets().to_vec());
                let token: [u8; 20] = Sha1::digest(token_plain).as_slice().try_into().unwrap();
                // response
                match self.known_peers.get(&info_hash) {
                    Some(peers) => {
                        let resp_peers_info = peers.iter().map(|(k, _v)| *k).collect();
                        self.sender
                            .do_req(
                                socket,
                                addr.to_string(),
                                KRPCMessage::GetPeersResp(
                                    self.own_node_id,
                                    token.to_vec(),
                                    GetPeersRespValuesOrNodes::Values(resp_peers_info), // todo we need to also add our own peers we know outside of dht
                                ),
                                None,
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
                        self.sender
                            .do_req(
                                socket,
                                addr.to_string(),
                                KRPCMessage::GetPeersResp(
                                    self.own_node_id,
                                    token.to_vec(),
                                    GetPeersRespValuesOrNodes::Nodes(closest_nodes_info),
                                ),
                                None,
                            )
                            .await;
                    }
                }
            }
            KRPCMessage::GetPeersResp(queried_node_id, token, get_peers_resp_values_or_nodes) => {
                let info_hash = match self.sender.inflight_requests.remove(&transaction_id) {
                    Some((_, _, _, Some(info_hash))) => info_hash,
                    _ => {
                        log::trace!("got a get_peers resp from {} for an expired or unknown transaction id we didn't perform, ignoring it", addr);
                        return;
                    }
                };
                let get_peers_req = match self.inflight_get_peers_requests.get_mut(&info_hash) {
                    Some(get_peers_req) => get_peers_req,
                    None => {
                        log::trace!("got a get_peers resp from {} for an expired get_peers request, ignoring it", addr);
                        return;
                    }
                };
                if get_peers_req.inflight_requests > 0 {
                    // avoid overflowing when by pure bad luck we get a late replay for a not yet expired msg request for
                    // an expired get_peers request that has been recreated immediatelly after the previous has expired
                    get_peers_req.inflight_requests -= 1;
                }

                // update replying nodes and sort by distance to target
                get_peers_req
                    .replying_nodes
                    .push((queried_node_id, ipv4addr, port, token));
                get_peers_req
                    .replying_nodes
                    .sort_by_key(|(node_id, _, _, _)| distance(&info_hash, node_id));

                // act on response: peers or nodes
                match get_peers_resp_values_or_nodes {
                    GetPeersRespValuesOrNodes::Values(peers) => {
                        for p in peers {
                            get_peers_req.discovered_peers.insert(p);
                        }
                        if get_peers_req.inflight_requests == 0 {
                            // search is over, let's push results
                            self.end_get_peer_search(&info_hash, dht_to_torrent_manager_tx, socket)
                                .await;
                        }
                    }
                    GetPeersRespValuesOrNodes::Nodes(nodes) => {
                        let mut max_distance_for_new_req = BigUint::from_str("2").unwrap().pow(160)
                            - BigUint::from_str("1").unwrap();
                        if get_peers_req.replying_nodes.len() != 0 {
                            max_distance_for_new_req = distance(
                                &info_hash,
                                &get_peers_req.replying_nodes
                                    [cmp::min(get_peers_req.replying_nodes.len(), K_FACTOR) - 1]
                                    .0,
                            );
                        }
                        for (node_id, ip, port) in nodes {
                            if
                            // we don't have already performed a request to this node
                            !get_peers_req.queried_nodes.contains(&node_id) &&
                            // if node is closer than the K_FACTORth closer node we queried, perform iterative request
                            distance(&info_hash, &node_id) <= max_distance_for_new_req
                            {
                                self.sender
                                    .do_req(
                                        socket,
                                        to_addr_string(&ip, port),
                                        KRPCMessage::GetPeersReq(self.own_node_id, info_hash),
                                        Some(info_hash),
                                    )
                                    .await;
                                get_peers_req.total_requests += 1;
                                get_peers_req.inflight_requests += 1;
                                get_peers_req.queried_nodes.insert(node_id);
                            }
                        }
                    }
                }
            }
            KRPCMessage::AnnouncePeerReq(
                _querying_node_id,
                info_hash,
                announce_peer_port,
                token,
                imply_port,
            ) => {
                // verify token
                let token_u8_20: [u8; 20] = match token.try_into() {
                    Ok(t) => t,
                    Err(_) => {
                        log::trace!("got an announce_peer from {} with a token that is not 20b as expected, refusing it", addr);
                        self.sender
                            .do_req(
                                &socket,
                                addr.to_string(),
                                KRPCMessage::Error(
                                    ErrorType::GenericError,
                                    "wrong secret, should be 20b long".to_string(),
                                ),
                                None,
                            )
                            .await;
                        return;
                    }
                };
                let mut expected_token_plain = self.token_signing_secret.to_vec();
                expected_token_plain.append(&mut ipv4addr.octets().to_vec());
                let expected_token: [u8; 20] = Sha1::digest(expected_token_plain)
                    .as_slice()
                    .try_into()
                    .unwrap();
                if expected_token != token_u8_20 {
                    log::trace!("got an announce_peer from {} with a token that is not what we expected, refusing it", addr);
                    self.sender
                        .do_req(
                            &socket,
                            addr.to_string(),
                            KRPCMessage::Error(
                                ErrorType::GenericError,
                                "wrong secret content".to_string(),
                            ),
                            None,
                        )
                        .await;
                    return;
                }
                // store / refresh peer info for this info hash
                let mut peer_port = announce_peer_port;
                if imply_port {
                    peer_port = port;
                }
                let peer_info = (ipv4addr, peer_port);
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
                self.sender
                    .do_req(
                        &socket,
                        addr.to_string(),
                        KRPCMessage::PingOrAnnouncePeerResp(self.own_node_id),
                        None,
                    )
                    .await;
                return;
            }
            KRPCMessage::Error(error_type, msg) => {
                if !self.sender.inflight_requests.contains_key(&transaction_id) {
                    log::trace!(
                        "got a error resp from {} for an unknown or expired transaction id we didn't perform, ignoring it", addr
                    );
                    return;
                }
                let inflight_req = self
                    .sender
                    .inflight_requests
                    .remove(&transaction_id)
                    .unwrap();
                log::debug!("got dht error respose for transaction id {} from {}; our message was: {:?}, error type: {:?}, error message: {}", force_string(&transaction_id), addr, inflight_req.1, error_type, msg);
            }
        }
    }

    async fn end_get_peer_search(
        &mut self,
        info_hash: &[u8; 20],
        dht_to_torrent_manager_tx: &Sender<DhtToTorrentManagerMsg>,
        socket: &UdpSocket,
    ) {
        if let Some(req) = self.inflight_get_peers_requests.remove(info_hash) {
            // send announce_peer to closest K nodes
            for i in 0..cmp::min(req.replying_nodes.len(), K_FACTOR) {
                let (_, ip, port, token) = &req.replying_nodes[i];
                self.sender
                    .do_req(
                        socket,
                        to_addr_string(&ip, *port),
                        KRPCMessage::AnnouncePeerReq(
                            self.own_node_id,
                            *info_hash,
                            self.listening_torrent_wire_protocol_port,
                            token.clone(),
                            false,
                        ),
                        None,
                    )
                    .await;
            }

            log::error!(
                "get_peer request ended: total sent requests: {} not replied: {}, replied: {}, discovered peers: {}",
                req.total_requests,
                req.inflight_requests,
                req.replying_nodes.len(),
                req.discovered_peers.len()
            );

            // finally send discovered peers to torrent manager
            for (ip, port) in req.discovered_peers {
                let _ = dht_to_torrent_manager_tx
                    .send(DhtToTorrentManagerMsg::NewPeer(ip, port))
                    .await;
            }
        }
    }

    async fn end_find_node_search(&mut self, random_id: &[u8; 20]) {
        if let Some(req) = self.inflight_find_node_requests.remove(random_id) {
            log::error!(
                "find_node request for {} ended: total sent requests: {} not replied: {}, discovered nodes: {}, probed nodes for routing table addition: {}",
                force_string(&req.node_id_to_find.to_vec()),
                req.total_requests,
                req.inflight_requests,
                req.total_discovered_nodes,
                req.probed_nodes.len(),
            );
        }
    }
}

fn to_addr_string(addr: &Ipv4Addr, port: u16) -> String {
    format!("{}:{}", addr.to_string(), port)
}

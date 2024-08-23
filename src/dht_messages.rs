use crate::{bencoding::Value, util::force_string};
use std::{collections::HashMap, error::Error, net::Ipv4Addr};

pub enum KRPCMessage {
    PingReq(String),                                          // querying nodes id
    PingOrAnnouncePeerResp(Vec<u8>), // queried nodes id, resps to ping or announce_peer messages cannot be distinghised by themselves without original transaciton id
    FindNodeReq(Vec<u8>, Vec<u8>),   // querying node id, id of target node
    FindNodeResp(String, Vec<(String, Ipv4Addr, u16)>), // queried node id, list of (20-byte Node ID in network byte order, IP-address, port)
    GetPeersReq(Vec<u8>, Vec<u8>), // querying node id, 20-byte infohash of target torrent
    GetPeersResp(String, Vec<u8>, GetPeersRespValuesOrNodes), // queried nodes id, token for a future announce_peer query, either "values" (list of peers having the info hash) or "nodes" (K nodes in the queried nodes routing table closest to the infohash supplied in the query)
    AnnouncePeerReq(Vec<u8>, Vec<u8>, u16, Vec<u8>, bool), // querying node id, 20-byte infohash of target torrent, port where we are listeing for torrent wire protocol, token received in response to a previous get_peers query, wether to imply port
    Error(ErrorType, String),                              // error type, message
}

pub enum GetPeersRespValuesOrNodes {
    Nodes(Vec<(String, Ipv4Addr, u16)>), // 20-byte Node ID in network byte order, IP-address, port
    Values(Vec<(Ipv4Addr, u16)>),        // peer IP-address, peer port
}

pub enum ErrorType {
    GenericError,
    ServerError,
    ProtocolError,
    MethodUnknown,
}

impl ErrorType {
    fn to_code(&self) -> i64 {
        match self {
            ErrorType::GenericError => 201,
            ErrorType::ServerError => 202,
            ErrorType::ProtocolError => 203,
            ErrorType::MethodUnknown => 204,
        }
    }

    fn from_code(code: i64) -> Result<Self, Box<dyn Error>> {
        match code {
            201 => Ok(ErrorType::GenericError),
            202 => Ok(ErrorType::ServerError),
            203 => Ok(ErrorType::ProtocolError),
            204 => Ok(ErrorType::MethodUnknown),
            _ => Err(Box::from("code not valid")),
        }
    }
}

pub fn encode_krpc_message(transaction_id: Vec<u8>, msg: KRPCMessage) -> Vec<u8> {
    let mut h = HashMap::from([
        (b"t".to_vec(), Value::Str(transaction_id)),
        (b"v".to_vec(), Value::Str(b"YT00".to_vec())),
    ]);

    match msg {
        KRPCMessage::PingReq(id) => {
            h.insert(b"y".to_vec(), Value::Str(b"q".to_vec()));
            h.insert(b"q".to_vec(), Value::Str(b"ping".to_vec()));
            h.insert(
                b"a".to_vec(),
                Value::Dict(
                    HashMap::from([(b"id".to_vec(), Value::Str(id.into_bytes()))]),
                    0, // this is discarded when creating hashmap bencoded values
                    0, // this is discarded when creating hashmap bencoded values
                ),
            );
        }
        KRPCMessage::PingOrAnnouncePeerResp(id) => {
            h.insert(b"y".to_vec(), Value::Str(b"r".to_vec()));
            h.insert(
                b"r".to_vec(),
                Value::Dict(
                    HashMap::from([(b"id".to_vec(), Value::Str(id))]),
                    0, // this is discarded when creating hashmap bencoded values
                    0, // this is discarded when creating hashmap bencoded values
                ),
            );
        }
        KRPCMessage::FindNodeReq(querying_node_id, target_node_id) => {
            h.insert(b"y".to_vec(), Value::Str(b"q".to_vec()));
            h.insert(b"q".to_vec(), Value::Str(b"find_node".to_vec()));
            h.insert(
                b"a".to_vec(),
                Value::Dict(
                    HashMap::from([
                        (b"id".to_vec(), Value::Str(querying_node_id)),
                        (b"target".to_vec(), Value::Str(target_node_id)),
                    ]),
                    0, // this is discarded when creating hashmap bencoded values
                    0, // this is discarded when creating hashmap bencoded values
                ),
            );
        }
        KRPCMessage::FindNodeResp(queried_node_id, nodes) => {
            let mut compact_node_info_vec = Vec::new();
            for (node_id, ip, port) in nodes {
                let mut compact_node_info_buf = [0u8; 26];
                compact_node_info_buf[0..20].copy_from_slice(&node_id.into_bytes());
                compact_node_info_buf[20..24].copy_from_slice(&ip.octets());
                compact_node_info_buf[24..26].copy_from_slice(&port.to_be_bytes());
                compact_node_info_vec.push(Value::Str(compact_node_info_buf.to_vec()));
            }
            h.insert(b"y".to_vec(), Value::Str(b"r".to_vec()));
            h.insert(
                b"r".to_vec(),
                Value::Dict(
                    HashMap::from([
                        (b"id".to_vec(), Value::Str(queried_node_id.into_bytes())),
                        (b"nodes".to_vec(), Value::List(compact_node_info_vec)),
                    ]),
                    0, // this is discarded when creating hashmap bencoded values
                    0, // this is discarded when creating hashmap bencoded values
                ),
            );
        }
        KRPCMessage::GetPeersReq(querying_node_id, target_infohash) => {
            h.insert(b"y".to_vec(), Value::Str(b"q".to_vec()));
            h.insert(b"q".to_vec(), Value::Str(b"get_peers".to_vec()));
            h.insert(
                b"a".to_vec(),
                Value::Dict(
                    HashMap::from([
                        (b"id".to_vec(), Value::Str(querying_node_id)),
                        (b"info_hash".to_vec(), Value::Str(target_infohash)),
                    ]),
                    0, // this is discarded when creating hashmap bencoded values
                    0, // this is discarded when creating hashmap bencoded values
                ),
            );
        }
        KRPCMessage::GetPeersResp(queried_node_ip, token, values_or_nodes) => {
            let mut r = HashMap::from([
                (b"id".to_vec(), Value::Str(queried_node_ip.into_bytes())),
                (b"token".to_vec(), Value::Str(token)),
            ]);
            match values_or_nodes {
                GetPeersRespValuesOrNodes::Nodes(nodes) => {
                    let mut n_l = Vec::new();
                    for (node_id, ip_addr, port) in nodes {
                        let mut p_buf = [0u8, 6];
                        p_buf[0..20].copy_from_slice(&node_id.into_bytes());
                        p_buf[20..24].copy_from_slice(&ip_addr.octets());
                        p_buf[24..26].copy_from_slice(&port.to_be_bytes());
                        n_l.push(Value::Str(p_buf.to_vec()));
                    }
                    r.insert(b"nodes".to_vec(), Value::List(n_l));
                }
                GetPeersRespValuesOrNodes::Values(values) => {
                    let mut v_l = Vec::new();
                    for (ip_addr, port) in values {
                        let mut p_buf = [0u8, 6];
                        p_buf[0..4].copy_from_slice(&ip_addr.octets());
                        p_buf[4..2].copy_from_slice(&port.to_be_bytes());
                        v_l.push(Value::Str(p_buf.to_vec()));
                    }
                    r.insert(b"values".to_vec(), Value::List(v_l));
                }
            }
            h.insert(b"y".to_vec(), Value::Str(b"r".to_vec()));
            h.insert(b"r".to_vec(), Value::Dict(r, 0, 0));
        }
        KRPCMessage::AnnouncePeerReq(querying_node_id, info_hash, port, token, implied_port) => {
            h.insert(b"y".to_vec(), Value::Str(b"q".to_vec()));
            h.insert(b"q".to_vec(), Value::Str(b"announce_peer".to_vec()));
            h.insert(
                b"a".to_vec(),
                Value::Dict(
                    HashMap::from([
                        (b"id".to_vec(), Value::Str(querying_node_id)),
                        (
                            b"implied_port".to_vec(),
                            if implied_port {
                                Value::Int(1)
                            } else {
                                Value::Int(0)
                            },
                        ),
                        (b"info_hash".to_vec(), Value::Str(info_hash)),
                        (b"port".to_vec(), Value::Int(port as i64)),
                        (b"token".to_vec(), Value::Str(token)),
                    ]),
                    0, // this is discarded when creating hashmap bencoded values
                    0, // this is discarded when creating hashmap bencoded values
                ),
            );
        }
        KRPCMessage::Error(error_type, error_msg) => {
            h.insert(b"y".to_vec(), Value::Str(b"e".to_vec()));
            h.insert(
                b"e".to_vec(),
                Value::List(vec![
                    Value::Int(error_type.to_code()),
                    Value::Str(error_msg.into_bytes()),
                ]),
            );
        }
    }

    Value::Dict(h, 0, 0).encode()
}

pub fn decode_krpc_message(
    data: Vec<u8>,
) -> Result<(Vec<u8> /* transaction id */, KRPCMessage), Box<dyn Error>> {
    let transaction_id;
    let bencoded_data = Value::new(&data);
    match bencoded_data {
        Value::Error(e) => return Err(Box::from(e.to_string())),
        Value::Str(_) | Value::Int(_) | Value::List(_) => {
            return Err(Box::from("got krpc message that is not a dict"))
        }
        Value::Dict(h, _, _) => {
            // check transaction id
            if let Some(t_id) = h.get(&b"t".to_vec()) {
                if let Value::Str(t_id_str) = t_id {
                    transaction_id = t_id_str.clone();
                } else {
                    return Err(Box::from(
                        "got krpc message that contains a t key that is not a string",
                    ));
                }
            } else {
                return Err(Box::from("got krpc message that does not contain a t key"));
            }

            // check y key existence
            let y = match h.get(&b"y".to_vec()) {
                Some(y) => y,
                None => {
                    return Err(Box::from("got krpc message that does not contain a y key"));
                }
            };

            // check y key is a string
            let y_str = match y {
                Value::Str(y_str) => y_str,
                _ => {
                    return Err(Box::from(
                        "got krpc message that contains a y key that is not a bencoded string",
                    ));
                }
            };

            // check y key type
            if y_str == b"q" {
                let msg = parse_req_message(&h)?;
                return Ok((transaction_id, msg));
            } else if y_str == b"r" {
                let msg = parse_response_message(&h)?;
                return Ok((transaction_id, msg));
            } else if y_str == b"e" {
                let msg = parse_error_message(&h)?;
                return Ok((transaction_id, msg));
            } else {
                return Err(Box::from(
                    "got krpc message that contains a y key that is neither a \"q\" or a \"r\"",
                ));
            }
        }
    }
}

fn parse_req_message(h: &HashMap<Vec<u8>, Value>) -> Result<KRPCMessage, Box<dyn Error>> {
    // check a existance
    let a = match h.get(&b"a".to_vec()) {
        Some(a) => a,
        _ => {
            return Err(Box::from(
                "got krpc message that is a query (y=q) but with no a key",
            ));
        }
    };

    // check a is a dict
    let a_h = match a {
        Value::Dict(a_h, _, _) => a_h,
        _ => {
            return Err(Box::from(
                "got krpc message that is a query (y=q) with \"a\" key that is not a dict",
            ));
        }
    };

    // check a contains id
    let id = match a_h.get(&b"id".to_vec()) {
        Some(id) => id,
        _ => {
            return Err(Box::from(
                    "got krpc message that is a query (y=q) with \"a\" key that is a dict with no id key",
                ));
        }
    };

    // check id is a string
    let id_str = match id {
        Value::Str(id_str) => id_str,
        _ => {
            return Err(Box::from("got krpc message that is a query (y=q) with \"a\" key that is a dict with id key that is not a bencoded string"));
        }
    };

    // check q existance
    let q = match h.get(&b"q".to_vec()) {
        Some(q) => q,
        _ => {
            return Err(Box::from(
                "got krpc message that is a query (y=q) but with no q key",
            ));
        }
    };

    // check q is a string
    let q_str = match q {
        Value::Str(q_str) => q_str,
        _ => {
            return Err(Box::from(
                "got krpc message that is a query (y=q) but the q key is not a bencoded string",
            ));
        }
    };

    // check q type
    if q_str == b"ping" {
        return Ok(KRPCMessage::PingOrAnnouncePeerResp(id_str.clone()));
    } else if q_str == b"find_node" {
        // check a contains target
        let target = match a_h.get(&b"target".to_vec()) {
            Some(target) => target,
            _ => {
                return Err(Box::from(
                    "got krpc message that is a find_node query (y=q) with \"a\" key that is a dict with no target key",
                ));
            }
        };

        // check target is a string
        let target_str = match target {
            Value::Str(target_str) => target_str,
            _ => {
                return Err(Box::from("got krpc message that is a find_node query (y=q) with \"a\" key that is a dict with target key that is not a bencoded string"));
            }
        };

        return Ok(KRPCMessage::FindNodeReq(id_str.clone(), target_str.clone()));
    } else if q_str == b"get_peers" {
        // check a contains info_hash
        let info_hash = match a_h.get(&b"info_hash".to_vec()) {
            Some(info_hash) => info_hash,
            _ => {
                return Err(Box::from(
                    "got krpc message that is a get_peers query (y=q) with \"a\" key that is a dict with no info_hash key",
                ));
            }
        };

        // check info_hash is a string
        let info_hash_str = match info_hash {
            Value::Str(info_hash_str) => info_hash_str,
            _ => {
                return Err(Box::from("got krpc message that is a get_peers query (y=q) with \"a\" key that is a dict with info_hash key that is not a bencoded string"));
            }
        };

        return Ok(KRPCMessage::GetPeersReq(
            id_str.clone(),
            info_hash_str.clone(),
        ));
    } else if q_str == b"announce_peer" {
        // check a contains info_hash
        let info_hash = match a_h.get(&b"info_hash".to_vec()) {
            Some(info_hash) => info_hash,
            _ => {
                return Err(Box::from("got krpc message that is a announce_peer query (y=q) with \"a\" key that is a dict with no info_hash key"));
            }
        };

        // check info_hash is a string
        let info_hash_str = match info_hash {
            Value::Str(info_hash_str) => info_hash_str,
            _ => {
                return Err(Box::from("got krpc message that is a announce_peer query (y=q) with \"a\" key that is a dict with info_hash key that is not a bencoded string"));
            }
        };

        // check a contains token
        let token = match a_h.get(&b"token".to_vec()) {
            Some(token) => token,
            _ => {
                return Err(Box::from("got krpc message that is a announce_peer query (y=q) with \"a\" key that is a dict with no token key"));
            }
        };

        // check token is a string
        let token_str = match token {
            Value::Str(token_str) => token_str,
            _ => {
                return Err(Box::from("got krpc message that is a announce_peer query (y=q) with \"a\" key that is a dict with token key that is not a bencoded string"));
            }
        };

        // check a contains port
        let port = match a_h.get(&b"port".to_vec()) {
            Some(port) => port,
            _ => {
                return Err(Box::from("got krpc message that is a announce_peer query (y=q) with \"a\" key that is a dict with no port key"));
            }
        };

        // check port is a int
        let mut port_int: u16 = match port {
            Value::Int(port_int) => match (*port_int).try_into() {
                Ok(p) => p,
                Err(_) => {
                    return Err(Box::from("got krpc message that is a announce_peer query (y=q) with \"a\" key that is a dict with port key that is a number that does not fit a u16"));
                }
            },
            _ => {
                return Err(Box::from("got krpc message that is a announce_peer query (y=q) with \"a\" key that is a dict with with a port key that is not a number"));
            }
        };

        let mut implied_port = false;
        if let Some(Value::Int(implied_port_int)) = a_h.get(&b"implied_port".to_vec()) {
            if *implied_port_int != 0 {
                implied_port = true;
            }
        }

        return Ok(KRPCMessage::AnnouncePeerReq(
            id_str.clone(),
            info_hash_str.clone(),
            port_int,
            token_str.clone(),
            implied_port,
        ));
    } else {
        return Err(Box::from(
            "got krpc message that is a query (y=q) but but could not recognize query type",
        ));
    }
}

fn parse_response_message(h: &HashMap<Vec<u8>, Value>) -> Result<KRPCMessage, Box<dyn Error>> {
    // check r existance
    let r = match h.get(&b"r".to_vec()) {
        Some(r) => r,
        None => {
            return Err(Box::from(
                "got krpc message that is a response (y=r) but but with no r key",
            ));
        }
    };

    // check r is a doct
    let r_h = match r {
        Value::Dict(r_h, _, _) => r_h,
        _ => {
            return Err(Box::from(
                "got krpc message that is a response (y=r) but but r key is not a bencoded dict",
            ));
        }
    };

    // check id existance
    let id = match r_h.get(&b"id".to_vec()) {
        Some(id) => id,
        None => {
            return Err(Box::from(
                "got krpc message that is a response (y=r) but but id key not present in r map",
            ));
        }
    };

    // check id is a string
    let id_str = match id {
        Value::Str(id_str) => id_str,
        _ => {
            return Err(Box::from("got krpc message that is a response (y=r) but but id key in r map is not a bencoded string"));
        }
    };

    // check token existence
    if let Some(token) = r_h.get(&b"token".to_vec()) {
        // this is a get_peer response
        let token_str = match token {
            Value::Str(token_str) => token_str,
            _ => {
                return Err(Box::from("got krpc message that is a response (y=r) and has a token key in the r map that is not a bencoded string"));
            }
        };
        if let Some(nodes_v) = r_h.get(&b"nodes".to_vec()) {
            let nodes_l = match nodes_v {
                Value::List(nodes_l) => nodes_l,
                _ => {
                    return Err(Box::from("got krpc message that is a response (y=r) and has a nodes key in the r map that is not a list"));
                }
            };
            let mut nodes = Vec::new();
            for n in nodes_l {
                let n_str = match n {
                    Value::Str(n_str) => n_str,
                    _ => {
                        return Err(Box::from("got krpc message that is a response (y=r) and has a nodes key in the r map but contains an element that is not a bencoded string"));
                    }
                };
                if n_str.len() != 26 {
                    return Err(Box::from("got krpc message that is a response (y=r) and has a nodes key in the r map but contains an element that is not 26 chars long"));
                }
                let mut node_id_buf: [u8; 20] = [0; 20];
                node_id_buf.copy_from_slice(&n_str[0..20]); // todo optmize this
                let node_id = force_string(&node_id_buf.to_vec());
                let mut node_ip_buf: [u8; 4] = [0; 4];
                node_ip_buf.copy_from_slice(&n_str[2..24]); // todo optmize this
                let node_ip = Ipv4Addr::from(node_ip_buf);
                let mut node_port_buf: [u8; 2] = [0; 2];
                node_port_buf.copy_from_slice(&n_str[24..26]); // todo optmize this
                let node_port = u16::from_be_bytes(node_port_buf);
                nodes.push((node_id, node_ip, node_port));
            }
            return Ok(
                KRPCMessage::GetPeersResp(
                    force_string(id_str),
                    token_str.clone(),
                    GetPeersRespValuesOrNodes::Nodes(nodes),
                ), // todo really force string here?
            );
        } else if let Some(peers_v) = r_h.get(&b"values".to_vec()) {
            let peers_l = match peers_v {
                Value::List(peers_l) => peers_l,
                _ => {
                    return Err(Box::from("got krpc message that is a response (y=r) and has a values key in the r map that is not a list"));
                }
            };
            let mut peers = Vec::new();
            for n in peers_l {
                let n_str = match n {
                    Value::Str(n_str) => n_str,
                    _ => {
                        return Err(Box::from("got krpc message that is a response (y=r) and has a values key in the r map but contains an element that is not a bencoded string"));
                    }
                };
                if n_str.len() != 6 {
                    return Err(Box::from("got krpc message that is a response (y=r) and has a values key in the r map but contains an element that is not 6 chars long"));
                }
                let mut peer_ip_buf: [u8; 4] = [0; 4];
                peer_ip_buf.copy_from_slice(&n_str[0..4]); // todo optmize this
                let peer_ip = Ipv4Addr::from(peer_ip_buf);
                let mut peer_port_buf: [u8; 2] = [0; 2];
                peer_port_buf.copy_from_slice(&n_str[4..6]); // todo optmize this
                let peer_port = u16::from_be_bytes(peer_port_buf);
                peers.push((peer_ip, peer_port));
            }
            return Ok(
                KRPCMessage::GetPeersResp(
                    force_string(id_str),
                    token_str.clone(),
                    GetPeersRespValuesOrNodes::Values(peers),
                ), // todo really force string here?
            );
        } else {
            return Err(Box::from("got krpc message that is a response (y=r) and has a token key in the r map but no nodes or values key"));
        }
    } else if let Some(nodes_v) = r_h.get(&b"nodes".to_vec()) {
        // this is a find_node response
        let nodes_l = match nodes_v {
            Value::List(nodes_l) => nodes_l,
            _ => {
                return Err(Box::from("got krpc message that is a response (y=r) and has a nodes key in the r map that is not a list"));
            }
        };
        let mut nodes = Vec::new();
        for n in nodes_l {
            let n_str = match n {
                Value::Str(n_str) => n_str,
                _ => {
                    return Err(Box::from("got krpc message that is a response (y=r) and has a nodes key in the r map but contains an element that is not a bencoded string"));
                }
            };
            if n_str.len() != 26 {
                return Err(Box::from("got krpc message that is a response (y=r) and has a nodes key in the r map but contains an element that is not 26 chars long"));
            }
            let mut node_id_buf: [u8; 20] = [0; 20];
            node_id_buf.copy_from_slice(&n_str[0..20]); // todo optmize this
            let node_id = force_string(&node_id_buf.to_vec());
            let mut node_ip_buf: [u8; 4] = [0; 4];
            node_ip_buf.copy_from_slice(&n_str[2..24]); // todo optmize this
            let node_ip = Ipv4Addr::from(node_ip_buf);
            let mut node_port_buf: [u8; 2] = [0; 2];
            node_port_buf.copy_from_slice(&n_str[24..26]); // todo optmize this
            let node_port = u16::from_be_bytes(node_port_buf);
            nodes.push((node_id, node_ip, node_port));
        }
        return Ok(
            KRPCMessage::FindNodeResp(force_string(id_str), (nodes)), // todo really force string here?
        );
    } else {
        // this is a ping or announce_peer response
        return Ok(KRPCMessage::PingOrAnnouncePeerResp(id_str.clone()));
    }
}

fn parse_error_message(h: &HashMap<Vec<u8>, Value>) -> Result<KRPCMessage, Box<dyn Error>> {
    let e = match h.get(&b"e".to_vec()) {
        Some(e) => e,
        None => {
            return Err(Box::from(
                "got krpc message that is an error (y=e) but the e key is not present",
            ));
        }
    };
    let e_l = match e {
        Value::List(e_l) => e_l,
        _ => {
            return Err(Box::from(
                "got krpc message that is an error (y=e) but the e key is not a list",
            ));
        }
    };
    if e_l.len() != 2 {
        return Err(Box::from(
            "got krpc message that is an error (y=e) but the e key is not a 2-element list",
        ));
    }
    let error_type = match e_l[0].clone() {
        Value::Int(e_code) => ErrorType::from_code(e_code)?,
        _ => {
            return Err(Box::from("got krpc message that is an error (y=e) but the first element in the e key list is not a bencoded integer"));
        }
    };
    let error_message = match e_l[1].clone() {
        Value::Str(error_message) => error_message,
        _ => {
            return Err(Box::from("got krpc message that is an error (y=e) but the second element in the e key list is not a bencoded string"));
        }
    };
    return Ok(KRPCMessage::Error(error_type, force_string(&error_message)));
}
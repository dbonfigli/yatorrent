use reqwest::ClientBuilder;
use tokio::{
    net::UdpSocket,
    time::{sleep, timeout},
};

use crate::bencoding::Value;
use anyhow::{Result, bail};
use rand::seq::IndexedRandom;
use std::fmt::Display;
use std::{fmt, io::Read, str, time::Duration};
use thiserror::Error;

const UDP_TIMEOUT: Duration = Duration::from_secs(15);
const UDP_RETRY_COOL_OFF_SEC: u64 = 15;
const UDP_MAX_RETRIES: u32 = 3; // according to https://www.bittorrent.org/beps/bep_0015.html it should be 8, but it is way too much

#[derive(Debug, Error)]
#[error("no trackers in list")]
pub(crate) struct NoTrackerError;

#[derive(PartialEq, Debug, Clone)]
pub struct Peer {
    pub peer_id: Option<String>, // peer's self-selected ID, as described above for the tracker request (string)
    pub ip: String, // peer's IP address either IPv6 (hexed) or IPv4 (dotted quad) or DNS name (string)
    pub port: u16,  // peer's port number (integer)
}

#[derive(PartialEq, Debug)]
pub struct OkResponse {
    pub warning_message: Option<String>, // Similar to failure reason, but the response still gets processed normally. The warning message is shown just like an error.
    pub interval: i64, // Interval in seconds that the client should wait between sending regular requests to the tracker
    pub min_interval: Option<i64>, // (optional) Minimum announce interval. If present clients must not re-announce more frequently than this.
    pub tracker_id: Option<String>, // A string that the client should send back on its next announcements. If absent and a previous announce sent a tracker id, do not discard the old value; keep using it.
    pub complete: i64,              // number of peers with the entire file, i.e. seeders (integer)
    pub incomplete: i64,            // number of non-seeder peers, aka "leechers" (integer)
    pub peers: Vec<Peer>,           // dictionary or model for peers
}

#[derive(PartialEq, Debug)]
pub enum Response {
    Ok(OkResponse),
    Failure(String), // failure_reason; if present, then no other keys may be present. The value is a human-readable error message as to why the request failed (string).
}

impl Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Response::Ok(ok_response) => {
                if let Some(warning_message) = &ok_response.warning_message {
                    write!(f, "WARNING {warning_message}")?;
                }
                let peers = ok_response
                    .peers
                    .iter()
                    .map(|p| format!("  - {}:{} (id: {:#?})", p.ip, p.port, p.peer_id))
                    .collect::<Vec<String>>()
                    .join("\n");
                write!(
                    f,
                    "interval: {}\nmin_interval: {:#?}\ntracker_id: {:#?}\nn. peers completed: {}\nn. peers incomplete: {}\npeers:\n{peers}",
                    ok_response.interval,
                    ok_response.min_interval,
                    ok_response.tracker_id,
                    ok_response.complete,
                    ok_response.incomplete
                )
            }
            Response::Failure(failure_message) => {
                write!(f, "FAILURE: {failure_message}")
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum Event {
    None,
    Started,
    #[allow(dead_code)]
    Stopped,
    Completed,
}

impl Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Event::None => "",
            Event::Started => "started",
            Event::Stopped => "stopped",
            Event::Completed => "completed",
        }
        .to_string();
        write!(f, "{str}")
    }
}

const COMPACT: i32 = 1;

#[derive(Clone)]
pub struct TrackerClient {
    peer_id: String,
    tracker_id: Option<String>,
    listening_port: u16,
    trackers_url: Vec<Vec<String>>,
    pub tracker_request_interval: Duration,
}

impl TrackerClient {
    pub fn new(peer_id: String, trackers_url: Vec<Vec<String>>, listening_port: u16) -> Self {
        let mut randomized_tiers: Vec<Vec<String>> = Vec::new();
        for tier in trackers_url {
            let randomized_tier = tier
                .choose_multiple(&mut rand::rng(), tier.len())
                .map(|e| e.clone())
                .collect();
            randomized_tiers.push(randomized_tier);
        }
        TrackerClient {
            peer_id,
            tracker_id: None,
            listening_port,
            trackers_url: randomized_tiers,
            tracker_request_interval: Duration::from_secs(600), // high interval by default to avoid bombarding tracker before we get the proper interval from it
        }
    }

    pub async fn request(
        &mut self,
        info_hash: [u8; 20],
        uploaded: u64,
        downloaded: u64,
        left: Option<u64>,
        event: Event,
    ) -> Result<Response> {
        let mut error_message = Vec::new();
        for tier_idx in 0..self.trackers_url.len() {
            for tracker_idx in 0..self.trackers_url[tier_idx].len() {
                let url = self.trackers_url[tier_idx][tracker_idx].clone();
                match self
                    .request_to_tracker(
                        url.clone(),
                        info_hash,
                        uploaded,
                        downloaded,
                        left.clone(),
                        event.clone(),
                    )
                    .await
                {
                    Ok(Response::Failure(msg)) => {
                        error_message.push(format!(
                            "tracker {} errored: \"{}\"",
                            url.clone(),
                            msg.clone()
                        ));
                        log::debug!("tracker {} responded with failure: {msg}", url.clone());
                        log::debug!("will try next tracker if it exists...");
                    }
                    Ok(Response::Ok(response)) => {
                        // update tracker id
                        if let None = self.tracker_id {
                            if let Some(id) = response.tracker_id.clone() {
                                self.tracker_id = Some(id);
                            }
                        }
                        // update order of trackers with the good one first
                        if tracker_idx != 0 {
                            let good_tracker = self.trackers_url[tier_idx].remove(tracker_idx);
                            self.trackers_url[tier_idx].insert(0, good_tracker);
                        }
                        // update tracker request interval
                        self.tracker_request_interval =
                            Duration::from_secs(response.interval as u64);
                        return Ok(Response::Ok(response));
                    }
                    Err(e) => {
                        log::debug!("error from tracker {}: {e}", url.clone());
                        log::debug!("will try next tracker if it exists...");
                        error_message.push(format!("tracker {} errored: \"{e}\"", url.clone()));
                    }
                }
            }
        }
        if error_message.len() == 0 {
            bail!(NoTrackerError);
        }
        bail!(error_message.join("; "));
    }

    async fn request_to_tracker(
        &self,
        url: String,
        info_hash: [u8; 20],
        uploaded: u64,
        downloaded: u64,
        left: Option<u64>,
        event: Event,
    ) -> Result<Response> {
        if url.starts_with("http") {
            log::debug!("trying reaching http tracker {url}...");
            self.request_to_http_tracker(url, info_hash, uploaded, downloaded, left, event)
                .await
        } else if url.starts_with("udp") {
            let mut attempts = 0;
            loop {
                log::debug!("trying reaching udp tracker {url}...");
                match self
                    .request_to_udp_tracker(
                        url.clone(),
                        info_hash,
                        uploaded,
                        downloaded,
                        left,
                        event.clone(),
                    )
                    .await
                {
                    Ok(r) => return Ok(r),
                    Err(e) => {
                        if attempts > UDP_MAX_RETRIES {
                            return Err(e);
                        } else {
                            sleep(Duration::from_secs(
                                UDP_RETRY_COOL_OFF_SEC * 2u64.pow(attempts),
                            ))
                            .await;
                            attempts += 1;
                        }
                    }
                }
            }
        } else {
            // some torrents are announcing webtorrent websockets with scheme wss://
            bail!("scheme of url not supported: {url}");
        }
    }

    async fn request_to_http_tracker(
        &self,
        url: String,
        info_hash: [u8; 20],
        uploaded: u64,
        downloaded: u64,
        left: Option<u64>,
        event: Event,
    ) -> Result<Response> {
        let mut url = reqwest::Url::parse_with_params(
            url.as_str(),
            &[
                ("key", self.peer_id.clone()), // some trackers want this otherwise they will reply "This tracker requires support for the "key" announce parameter. Please update or change your client.". todo: still i could not find the related BEP
                ("peer_id", self.peer_id.clone()),
                ("port", self.listening_port.to_string()),
                ("uploaded", uploaded.to_string()),
                ("downloaded", downloaded.to_string()),
                ("compact", COMPACT.to_string()),
                ("event", event.to_string()),
                ("numwant", "50".to_string()),
            ],
        )?;

        if let Some(left) = left {
            url.query_pairs_mut().append_pair("left", &left.to_string());
        }

        // we need this so to avoid reqwest to urlencode again info_hash - binary array cannot be natively url encoded by it
        if let Some(query) = url.query() {
            url.set_query(Some(
                &("info_hash=".to_string() + &url_encode_info_hash(info_hash) + "&" + query),
            ))
        }

        log::debug!("requesting url: {url}");

        let body: Vec<u8> = match ClientBuilder::new()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()?
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?
            .bytes()
            .collect()
        {
            Ok(b) => b,
            Err(e) => bail!("error on http request: {e}"),
        };

        let response_map = match Value::new(&body) {
            Value::Dict(m, _, _) => m,
            _ => bail!("The server response was not a valid bencoded map"),
        };

        if let Some(Value::Str(failure_reason_vec)) = response_map.get(&b"failure reason".to_vec())
        {
            if let Ok(f) = str::from_utf8(&failure_reason_vec) {
                return Ok(Response::Failure(f.to_string()));
            } else {
                bail!(
                    "Failure reason key provided in bencoded dict response but it is not an UTF8 string"
                );
            }
        }

        // warning message
        let warning_message = match response_map.get(&b"warning message".to_vec()) {
            Some(Value::Str(warning_message_vec)) => match str::from_utf8(&warning_message_vec) {
                Ok(w) => Some(w.to_string()),
                _ => bail!(
                    "Warning message key provided in bencoded dict response but it is not an UTF8 string"
                ),
            },
            _ => None,
        };

        // interval
        let interval = match response_map.get(&b"interval".to_vec()) {
            Some(Value::Int(i)) => *i,
            _ => bail!(
                "Interval key not provided in bencoded dict response or provided but it is not a number"
            ),
        };

        // min interval
        let min_interval = match response_map.get(&b"min interval".to_vec()) {
            Some(Value::Int(i)) => Some(*i),
            _ => None,
        };

        // tracker id
        let tracker_id = match response_map.get(&b"tracker id".to_vec()) {
            Some(Value::Str(tracker_id_vec)) => match str::from_utf8(&tracker_id_vec) {
                Ok(w) => Some(w.to_string()),
                _ => bail!(
                    "Tracker id key provided in bencoded dict response but it is not an UTF8 string"
                ),
            },
            _ => None,
        };

        // complete
        let complete = match response_map.get(&b"complete".to_vec()) {
            Some(Value::Int(i)) => *i,
            _ => bail!(
                "Complete key not provided in bencoded dict response or provided but it is not a number"
            ),
        };

        // incomplete
        let incomplete = match response_map.get(&b"incomplete".to_vec()) {
            Some(Value::Int(i)) => *i,
            _ => bail!(
                "Incomplete key not provided in bencoded dict response or provided but it is not a number"
            ),
        };

        // peers
        let peers = match response_map.get(&b"peers".to_vec()) {
            Some(Value::List(peers_list)) => get_peers_with_dict_model(peers_list)?,
            Some(Value::Str(peers_bytes)) => get_peers_with_binary_model(peers_bytes)?,
            _ => bail!(
                "Peers key not provided in bencoded dict or provided but it was not a list or string"
            ),
        };

        Ok(Response::Ok(OkResponse {
            warning_message,
            interval,
            min_interval,
            tracker_id,
            complete,
            incomplete,
            peers,
        }))
    }

    async fn request_to_udp_tracker(
        &self,
        url: String,
        info_hash: [u8; 20],
        uploaded: u64,
        downloaded: u64,
        left: Option<u64>,
        event: Event,
    ) -> Result<Response> {
        let url = reqwest::Url::parse(&url)?;
        let host = match url.host() {
            Some(h) => h,
            None => bail!("udp tracker url did not contain host: {url}"),
        };
        let port = match url.port() {
            Some(p) => p,
            None => bail!("udp tracker url did not contain port: {url}"),
        };

        let dest_addr = format!("{host}:{port}");
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        if let Err(e) = socket.connect(dest_addr).await {
            bail!(e);
        }

        // send connect
        let transaction_id: u32 = rand::random::<u32>();
        let mut send_connect_buf = [0u8; 16];
        send_connect_buf[0..8].copy_from_slice(&(0x41727101980u64.to_be_bytes()));
        send_connect_buf[8..12].copy_from_slice(&(0u32.to_be_bytes()));
        send_connect_buf[12..16].copy_from_slice(&(transaction_id.to_be_bytes()));
        if let Err(e) = socket.send(&send_connect_buf).await {
            bail!(e);
        }

        // receive connect response
        let mut recv_connect_buf = [0u8; 16];
        match timeout(UDP_TIMEOUT, socket.recv(&mut recv_connect_buf)).await {
            Err(_elapsed) => bail!("timed out receiving connect response from udp tracker"),
            Ok(Err(e)) => bail!(e),
            Ok(Ok(bytes_recv)) => {
                if bytes_recv != recv_connect_buf.len() {
                    bail!("received less than 16 bytes on recv connect from udp tracker");
                }
            }
        }

        let mut action_buf = [0u8; 4];
        action_buf.copy_from_slice(&recv_connect_buf[0..4]);
        let action = u32::from_be_bytes(action_buf);
        if action != 0 {
            bail!(
                "got connect response from udp tracker but received action was not 0 (i.e.: connect): {action}"
            );
        }

        let mut transaction_id_buf = [0u8; 4];
        transaction_id_buf.copy_from_slice(&recv_connect_buf[4..8]);
        let recv_transaction_id = u32::from_be_bytes(transaction_id_buf);
        if recv_transaction_id != transaction_id {
            bail!(
                "got connect response from udp tracker but received transaction_id {recv_transaction_id} was different from the request ({transaction_id})"
            );
        }

        let mut connection_id_buf = [0u8; 8];
        connection_id_buf.copy_from_slice(&recv_connect_buf[8..16]);
        let connection_id = u64::from_be_bytes(connection_id_buf);

        // send announce
        let mut announce_buf = [0u8; 98];
        announce_buf[0..8].copy_from_slice(&connection_id.to_be_bytes());
        announce_buf[8..12].copy_from_slice(&1u32.to_be_bytes()); // action: announce
        let transaction_id: u32 = rand::random::<u32>();
        announce_buf[12..16].copy_from_slice(&transaction_id.to_be_bytes());
        announce_buf[16..36].copy_from_slice(&info_hash);
        announce_buf[36..56].copy_from_slice(self.peer_id.as_bytes());
        announce_buf[56..64].copy_from_slice(&downloaded.to_be_bytes());
        announce_buf[64..72].copy_from_slice(&left.unwrap_or_default().to_be_bytes()); // BEP 0015 does not specify what to do if left is not provided, so we use 0
        announce_buf[72..80].copy_from_slice(&uploaded.to_be_bytes());
        let event_id: u32 = match event {
            Event::None => 0,
            Event::Completed => 1,
            Event::Started => 2,
            Event::Stopped => 3,
        };
        announce_buf[80..84].copy_from_slice(&event_id.to_be_bytes());
        announce_buf[92..96].copy_from_slice(&(-1i32).to_be_bytes());
        announce_buf[96..98].copy_from_slice(&self.listening_port.to_be_bytes());
        if let Err(e) = socket.send(&announce_buf).await {
            bail!(e);
        }

        // receive announce response
        let mut recv_announce_buf = [0u8; 65535]; // max udp datagram size
        match timeout(UDP_TIMEOUT, socket.recv(&mut recv_announce_buf)).await {
            Err(_elapsed) => bail!("timed out receiving announce response from udp tracker"),
            Ok(Err(e)) => bail!(e),
            Ok(Ok(bytes_recv)) => {
                if bytes_recv < 16 {
                    bail!("received less than 16 bytes on recv announce from udp tracker");
                }

                let mut action_buf = [0u8; 4];
                action_buf.copy_from_slice(&recv_announce_buf[0..4]);
                let action = u32::from_be_bytes(action_buf);
                if action != 1 {
                    bail!(
                        "got announce response from udp tracker but received action was not 1 (i.e.: announce): {action}"
                    );
                }

                let mut transaction_id_buf = [0u8; 4];
                transaction_id_buf.copy_from_slice(&recv_announce_buf[4..8]);
                let recv_transaction_id = u32::from_be_bytes(transaction_id_buf);
                if recv_transaction_id != transaction_id {
                    bail!(
                        "got announce response from udp tracker but received transaction_id {recv_transaction_id} was different from the request ({transaction_id})"
                    );
                }

                let mut interval_buf = [0u8; 4];
                interval_buf.copy_from_slice(&recv_announce_buf[8..12]);
                let interval = u32::from_be_bytes(interval_buf);

                let mut leechers_buf = [0u8; 4];
                leechers_buf.copy_from_slice(&recv_announce_buf[12..16]);
                let leechers = u32::from_be_bytes(leechers_buf);

                let mut seeders_buf = [0u8; 4];
                seeders_buf.copy_from_slice(&recv_announce_buf[16..20]);
                let seeders = u32::from_be_bytes(seeders_buf);

                let mut peers = Vec::new();
                let address_len = 4; // todo check if we are using ipv6
                if (bytes_recv - 20) % 6 != 0 {
                    bail!(
                        "got announce response but size is not valid: addresses field is not divisible by 6: {}",
                        bytes_recv - 20
                    );
                }
                for i in (20..bytes_recv).step_by(address_len + 2) {
                    let mut address_buf = [0u8; 4];
                    address_buf.copy_from_slice(&recv_announce_buf[i..i + 4]);
                    let ip = [
                        address_buf[0].to_string(),
                        address_buf[1].to_string(),
                        address_buf[2].to_string(),
                        address_buf[3].to_string(),
                    ]
                    .join(".");
                    let peer_port_bytes = &recv_announce_buf[i + 4..i + 6];
                    let port = peer_port_bytes[0] as u16 * 256 + peer_port_bytes[1] as u16;
                    peers.push(Peer {
                        peer_id: None,
                        ip,
                        port,
                    });
                }

                Ok(Response::Ok(OkResponse {
                    warning_message: None,
                    interval: interval as i64,
                    min_interval: None,
                    tracker_id: None,
                    complete: seeders as i64,
                    incomplete: leechers as i64,
                    peers,
                }))
            }
        }
    }
}

fn get_peers_with_dict_model(peers_values: &Vec<Value>) -> Result<Vec<Peer>> {
    let mut peers_list: Vec<Peer> = Vec::new();
    for v in peers_values {
        match v {
            Value::Dict(peer_dic, _, _) => {
                // peer id
                let peer_id = match peer_dic.get(&b"peer id".to_vec()) {
                    Some(Value::Str(peer_id_vec)) => match str::from_utf8(&peer_id_vec) {
                        Ok(w) => Some(w.to_string()),
                        _ => bail!(
                            "Peer id key provided in list of peers in bencoded dict response but it is not an UTF8 string"
                        ),
                    },
                    _ => None,
                };

                // ip
                let ip = match peer_dic.get(&b"ip".to_vec()) {
                    Some(Value::Str(ip_vec)) => match str::from_utf8(&ip_vec) {
                        Ok(i) => i.to_string(),
                        _ => bail!(
                            "Ip key provided in list of peers in bencoded dict response but it is not an UTF8 string"
                        ),
                    },
                    _ => bail!(
                        "Ip key not provided in list of peers in bencoded dict response or provided but it is not a string"
                    ),
                };

                // port
                let port = match peer_dic.get(&b"port".to_vec()) {
                    Some(Value::Int(port_vec)) => u16::try_from(*port_vec)?,
                    _ => bail!(
                        "Port key not provided in list of peers in bencoded dict response or provided but it is not a valid number"
                    ),
                };

                peers_list.push(Peer { peer_id, ip, port });
            }
            _ => bail!("Peers list contains a value that is not a dic"),
        };
    }
    Ok(peers_list)
}

fn get_peers_with_binary_model(peers_bytes: &Vec<u8>) -> Result<Vec<Peer>> {
    if peers_bytes.len() % 6 != 0 {
        bail!("Peers list is provided in binary model but it is not aligned to 6 bytes");
    }
    let mut peers_list: Vec<Peer> = Vec::new();
    for i in (0..peers_bytes.len()).step_by(6) {
        let peer_ip_bytes = &peers_bytes[i..i + 4];
        let ip = [
            peer_ip_bytes[0].to_string(),
            peer_ip_bytes[1].to_string(),
            peer_ip_bytes[2].to_string(),
            peer_ip_bytes[3].to_string(),
        ]
        .join(".");
        let peer_port_bytes = &peers_bytes[i + 4..i + 6];
        let port = peer_port_bytes[0] as u16 * 256 + peer_port_bytes[1] as u16;

        peers_list.push(Peer {
            peer_id: None,
            ip,
            port,
        });
    }
    Ok(peers_list)
}

fn url_encode_info_hash(binary_array: [u8; 20]) -> String {
    let mut url_encoded = "".to_string();
    for v in binary_array {
        url_encoded = url_encoded + &format!("%{:02X}", v)
    }
    url_encoded
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn binary_peer_conversion() {
        let byte_peers = [
            0x1, 0x2, 0x3, 0x4, /* ip 1.2.3.4 */ 0x00, 0x50, /* port 80 */
            0x5, 0x6, 0x7, 0x8, /* ip 5.6.7.8 */ 0x04, 0xbd, /* port 1213 */
        ]
        .to_vec();
        let peers_result = get_peers_with_binary_model(&byte_peers);
        let expected = [
            Peer {
                peer_id: None,
                ip: "1.2.3.4".to_string(),
                port: 80,
            },
            Peer {
                peer_id: None,
                ip: "5.6.7.8".to_string(),
                port: 1213,
            },
        ]
        .to_vec();

        assert_matches!(peers_result, Ok(peers) => {
            assert_eq!(peers, expected)
        });
    }
}

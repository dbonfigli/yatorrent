use reqwest::ClientBuilder;

use crate::bencoding::Value;
use std::{error::Error, fmt, io::Read, str, time::Duration};

#[derive(PartialEq, Debug, Clone)]
pub struct Peer {
    pub peer_id: Option<String>, // peer's self-selected ID, as described above for the tracker request (string)
    pub ip: String, // peer's IP address either IPv6 (hexed) or IPv4 (dotted quad) or DNS name (string)
    pub port: u32,  // peer's port number (integer)
}

#[derive(PartialEq, Debug)]
pub struct OkResponse {
    pub warning_message: Option<String>, // Similar to failure reason, but the response still gets processed normally. The warning message is shown just like an error.
    pub interval: i64, // Interval in seconds that the client should wait between sending regular requests to the tracker
    pub min_interval: Option<i64>, // (optional) Minimum announce interval. If present clients must not reannounce more frequently than this.
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

impl fmt::Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Response::Ok(ok_response) => {
                if let Some(warning_message) = &ok_response.warning_message {
                    write!(f, "WARNING {}", warning_message)?;
                }
                let peers = ok_response
                    .peers
                    .iter()
                    .map(|p| format!("  - {}:{} (id: {:#?})", p.ip, p.port, p.peer_id))
                    .collect::<Vec<String>>()
                    .join("\n");
                write!(f, "interval: {}\nmin_interval: {:#?}\ntracker_id: {:#?}\nn. peers completed: {}\nn. peers incomplete: {}\npeers:\n{}", ok_response.interval, ok_response.min_interval, ok_response.tracker_id, ok_response.complete, ok_response.incomplete, peers)
            }
            Response::Failure(failure_message) => {
                write!(f, "FAILURE: {}", failure_message)
            }
        }
    }
}

pub enum Event {
    None,
    Started,
    Stopped,
    Completed,
}

impl ToString for Event {
    fn to_string(&self) -> String {
        match self {
            Event::None => "",
            Event::Started => "started",
            Event::Stopped => "stopped",
            Event::Completed => "completed",
        }
        .to_string()
    }
}

const COMPACT: i32 = 1;

#[derive(Clone)]
pub struct TrackerClient {
    peer_id: String,
    pub tracker_id: Option<String>,
    listening_port: i32,
    tracker_url: String,
}

impl TrackerClient {
    pub fn new(peer_id: String, tracker_url: String, listening_port: i32) -> Self {
        TrackerClient {
            peer_id: peer_id,
            tracker_id: Option::None,
            listening_port,
            tracker_url,
        }
    }

    pub async fn request(
        &self,
        info_hash: [u8; 20],
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: Event,
    ) -> Result<Response, Box<dyn Error>> {
        let mut url = reqwest::Url::parse_with_params(
            self.tracker_url.as_str(),
            &[
                ("peer_id", self.peer_id.clone()),
                ("port", self.listening_port.to_string()),
                ("uploaded", uploaded.to_string()),
                ("downloaded", downloaded.to_string()),
                ("left", left.to_string()),
                ("compact", COMPACT.to_string()),
                ("event", event.to_string()),
                ("numwant", "50".to_string()),
            ],
        )?;

        // we need this so to avoid reqwest to urlencode again info_hash - binary array cannot be natively url encoded by it
        if let Some(query) = url.query() {
            url.set_query(Some(
                &("info_hash=".to_string() + &url_encode_info_hash(info_hash) + "&" + query),
            ))
        }

        log::trace!("requesting url: {}", url);

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
            Err(e) => return Err(Box::new(e)),
        };

        let response_map = match Value::new(&body) {
            Value::Dict(m, _, _) => m,
            _ => return Err("The server response was not a valid bencoded map".into()),
        };

        if let Some(Value::Str(failure_reason_vec)) = response_map.get(&b"failure reason".to_vec())
        {
            if let Ok(f) = str::from_utf8(&failure_reason_vec) {
                return Ok(Response::Failure(f.to_string()));
            } else {
                return Err("Failure reason key provided in bencoded dict response but it is not an UTF8 string".into());
            }
        }

        // warning message
        let warning_message = match response_map.get(&b"warning message".to_vec()) {
            Some(Value::Str(warning_message_vec)) => match str::from_utf8(&warning_message_vec) {
                Ok(w) => Option::Some(w.to_string()),
                _ => return Err("Warining message key provided in bencoded dict response but it is not an UTF8 string".into()),
            }
            _ => Option::None
        };

        // interval
        let interval = match response_map.get(&b"interval".to_vec()) {
            Some(Value::Int(i)) => *i,
            _ => return Err("Interval key not provided in bencoded dict response or provided but it is not a number".into()),
        };

        // min interval
        let min_interval = match response_map.get(&b"min interval".to_vec()) {
            Some(Value::Int(i)) => Option::Some(*i),
            _ => Option::None,
        };

        // tracker id
        let tracker_id = match response_map.get(&b"tracker id".to_vec()) {
            Some(Value::Str(tracker_id_vec)) => match str::from_utf8(&tracker_id_vec) {
                Ok(w) => Option::Some(w.to_string()),
                _ => return Err(
                    "Tracker id key provided in bencoded dict response but it is not an UTF8 string"
                        .into(),
                ),
            },
            _ => Option::None,
        };

        // complete
        let complete = match response_map.get(&b"complete".to_vec()) {
            Some(Value::Int(i)) => *i,
            _ => return Err("Complete key not provided in bencoded dict response or provided but it is not a number".into()),
        };

        // incomplete
        let incomplete = match response_map.get(&b"incomplete".to_vec()) {
            Some(Value::Int(i)) => *i,
            _ => return Err("Incomplete key not provided in bencoded dict response or provided but it is not a number".into()),
        };

        // peers
        let peers = match response_map.get(&b"peers".to_vec()) {
            Some(Value::List(peers_list)) => get_peers_wiht_dict_model(peers_list)?,
            Some(Value::Str(peers_bytes)) => get_peers_wiht_binary_model(peers_bytes)?,
            _ => return Err(
                "Peers key not provided in bencoded dict or provided but it was not a list or string"
                    .into(),
            ),
        };

        Ok(Response::Ok(OkResponse {
            warning_message: warning_message,
            interval: interval,
            min_interval: min_interval,
            tracker_id: tracker_id,
            complete: complete,
            incomplete: incomplete,
            peers: peers,
        }))
    }
}

fn get_peers_wiht_dict_model(peers_values: &Vec<Value>) -> Result<Vec<Peer>, Box<dyn Error>> {
    let mut peers_list: Vec<Peer> = Vec::new();
    for v in peers_values {
        match v {
            Value::Dict(peer_dic, _, _) => {
                // peer id
                let peer_id = match peer_dic.get(&b"peer id".to_vec()) {
                    Some(Value::Str(peer_id_vec)) => match str::from_utf8(&peer_id_vec) {
                        Ok(w) => Option::Some(w.to_string()),
                        _ => return Err("Peer id key provided in list of peers in bencoded dict response but it is not an UTF8 string".into()),
                    }
                    _ => Option::None
                };

                // ip
                let ip = match peer_dic.get(&b"ip".to_vec()) {
                    Some(Value::Str(ip_vec)) => match str::from_utf8(&ip_vec) {
                        Ok(i) => i.to_string(),
                        _ => return Err("Ip key provided in list of peers in bencoded dict response but it is not an UTF8 string".into()),
                    }
                    _ => return Err("Ip key not provided in list of peers in bencoded dict response or provided but it is not a string".into()),
                };

                // port
                let port = match peer_dic.get(&b"port".to_vec()) {
                    Some(Value::Int(port_vec)) => u32::try_from(*port_vec)?,
                    _ => return Err("Port key not provided in list of peers in bencoded dict response or provided but it is not a valid number".into()),
                };

                peers_list.push(Peer { peer_id, ip, port });
            }
            _ => return Err("Peers list contains a value that is not a dic".into()),
        };
    }
    Ok(peers_list)
}

fn get_peers_wiht_binary_model(peers_bytes: &Vec<u8>) -> Result<Vec<Peer>, Box<dyn Error>> {
    if peers_bytes.len() % 6 != 0 {
        return Err(
            "Peers list is provided in binary model but it is not aligned to 6 bytes".into(),
        );
    }
    let mut peers_list: Vec<Peer> = Vec::new();
    for i in (0..=(peers_bytes.len() - 1)).step_by(6) {
        let peer_ip_bytes = &peers_bytes[i..i + 4];
        let ip = [
            (peer_ip_bytes[0]).to_string(),
            (peer_ip_bytes[1]).to_string(),
            (peer_ip_bytes[2]).to_string(),
            peer_ip_bytes[3].to_string(),
        ]
        .join(".");
        let peer_port_bytes = &peers_bytes[i + 4..i + 6];
        let port = peer_port_bytes[0] as u32 * 256 + peer_port_bytes[1] as u32;

        peers_list.push(Peer {
            peer_id: Option::None,
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
        let peers_result = get_peers_wiht_binary_model(&byte_peers);
        let expected = [
            Peer {
                peer_id: Option::None,
                ip: "1.2.3.4".to_string(),
                port: 80,
            },
            Peer {
                peer_id: Option::None,
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

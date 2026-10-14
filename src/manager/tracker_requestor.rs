use anyhow::{Result, bail};
use rand::RngExt;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::net::lookup_host;
use tokio::task::JoinHandle;

use crate::manager::peer::PeerDiscoverySource;
use crate::manager::torrent_manager::peer_context::{AdvertisedPeers, PeerAddrAndId};
use crate::tracker::{Event, NoTrackerError, Response, TrackerClient};

struct TrackerClientEntry {
    bind_ip: IpAddr,
    tracker_client: Arc<Mutex<TrackerClient>>,
    last_tracker_request_time: Option<Instant>,
}

impl TrackerClientEntry {
    async fn async_request_to_tracker(
        &mut self,
        event: Event,
        advertised_peers: &AdvertisedPeers,
        bytes_left: Option<u64>,
        uploaded_downloaded_bytes: (u64, u64),
        info_hash: [u8; 20],
    ) -> JoinHandle<()> {
        self.last_tracker_request_time = Some(Instant::now());

        let mut cloned_tracker_client = {
            self.tracker_client
                .lock()
                .expect("another user panicked while holding the lock")
                .clone()
        };

        let mut advertised_peers = advertised_peers.clone();
        let bind_ip = self.bind_ip;
        let tracker_client_arc = self.tracker_client.clone();
        tokio::spawn(async move {
            if let Ok(latest_advertised_peers) = request_to_tracker(
                &mut cloned_tracker_client,
                bind_ip,
                event,
                bytes_left,
                info_hash,
                uploaded_downloaded_bytes.0,
                uploaded_downloaded_bytes.1,
            )
            .await
            {
                advertised_peers.insert(latest_advertised_peers, PeerDiscoverySource::Tracker);
                *tracker_client_arc
                    .lock()
                    .expect("another user panicked while holding the lock") = cloned_tracker_client;
            }
        })
    }
}

pub struct TrackerRequestor {
    tracker_clients: Vec<TrackerClientEntry>,
    completed_sent_to_tracker: bool,
    info_hash: [u8; 20],
}

impl TrackerRequestor {
    pub fn new(
        bind_ips: Vec<IpAddr>,
        peer_id: String,
        trackers_url: Vec<Vec<String>>,
        listening_torrent_wire_protocol_port: u16,
        info_hash: [u8; 20],
    ) -> Self {
        let announce_key: u32 = rand::rng().random();
        let mut tracker_clients = Vec::new();
        for bind_ip in bind_ips {
            tracker_clients.push(TrackerClientEntry {
                bind_ip,
                tracker_client: Arc::new(Mutex::new(TrackerClient::new(
                    peer_id.clone(),
                    announce_key,
                    trackers_url.clone(),
                    listening_torrent_wire_protocol_port,
                ))),
                last_tracker_request_time: None,
            });
        }

        TrackerRequestor {
            tracker_clients,
            completed_sent_to_tracker: false,
            info_hash,
        }
    }

    pub async fn async_request_to_tracker(
        &mut self,
        event: Event,
        advertised_peers: &AdvertisedPeers,
        bytes_left: Option<u64>,
        uploaded_downloaded_bytes: (u64, u64),
    ) -> Option<Vec<JoinHandle<()>>> {
        if event == Event::Completed {
            if self.completed_sent_to_tracker {
                return None;
            }
            self.completed_sent_to_tracker = true;
        }

        let mut join_handles = Vec::new();
        for t in self.tracker_clients.iter_mut() {
            let event = if event == Event::None && t.last_tracker_request_time.is_none() {
                Event::Started
            } else {
                event
            };

            // avoid sending if this is a normal update and we already sent a request to the tracker
            if event == Event::None {
                let tracker_request_interval = t
                    .tracker_client
                    .lock()
                    .expect("another user panicked while holding the lock")
                    .tracker_request_interval;

                if let Some(last_req_time) = t.last_tracker_request_time
                    && Instant::now().duration_since(last_req_time) < tracker_request_interval
                {
                    continue;
                }
            }

            let join_handle = t
                .async_request_to_tracker(
                    event,
                    advertised_peers,
                    bytes_left,
                    uploaded_downloaded_bytes,
                    self.info_hash,
                )
                .await;
            join_handles.push(join_handle);
        }

        Some(join_handles)
    }
}

async fn request_to_tracker(
    tracker_client: &mut TrackerClient,
    bind_ip: IpAddr,
    event: Event,
    bytes_left: Option<u64>,
    info_hash: [u8; 20],
    uploaded_bytes: u64,
    downloaded_bytes: u64,
) -> Result<Vec<PeerAddrAndId>> {
    match tracker_client
        .request(
            bind_ip,
            info_hash,
            uploaded_bytes,
            downloaded_bytes,
            bytes_left,
            event,
        )
        .await
    {
        Err(e) => {
            match e.downcast_ref() {
                Some(NoTrackerError) => log::debug!("no known trackers to perform requests to"),
                _ => log::debug!(
                    "could not perform request to tracker (from source: \"{bind_ip}\"): {e}"
                ),
            }
            bail!(e);
        }
        Ok(Response::Failure(msg)) => {
            log::debug!("tracker responded with failure (from source: \"{bind_ip}\"): {msg}");
            bail!(msg);
        }
        Ok(Response::Ok(ok_response)) => {
            if let Some(msg) = ok_response.warning_message.clone() {
                log::warn!("tracker sent a warning (from source: \"{bind_ip}\"): {msg}");
            }
            log::info!(
                "tracker request succeeded: seeders: {}; leechers: {}; peers provided: {}",
                ok_response
                    .complete
                    .map_or("unknown".to_string(), |c| c.to_string()),
                ok_response
                    .incomplete
                    .map_or("unknown".to_string(), |i| i.to_string()),
                ok_response.peers.len()
            );
            log::trace!("full tracker response (from source: \"{bind_ip}\"):\n{ok_response:?}");

            // an http tracker could reply with a hostname, on this case, we immediatelly resolve the dns name
            // and we put as peers all the IPs resolved: when selecting the peers to connect, we will avoid
            // duplicates by matching the peer id
            let mut peers = Vec::new();
            for advertised_peer in ok_response.peers {
                match lookup_host((advertised_peer.ip.clone(), advertised_peer.port)).await {
                    Err(e) => {
                        log::debug!(
                            "failed to resolve address {}:{}, ignoring this host sent from tracker: {e}",
                            advertised_peer.ip,
                            advertised_peer.port
                        );
                    }
                    Ok(socket_addr_iter) => {
                        for p in socket_addr_iter {
                            log::trace!("adding address {p} as advertised peer");
                            peers.push(PeerAddrAndId::new(p, None));
                        }
                    }
                }
            }

            Ok(peers)
        }
    }
}

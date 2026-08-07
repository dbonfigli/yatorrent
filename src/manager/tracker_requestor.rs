use anyhow::{Result, bail};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::tracker;
use crate::tracker::{Event, NoTrackerError, Response, TrackerClient};
use crate::util::HostAndPort;

pub struct TrackerRequestor {
    tracker_client: Arc<Mutex<TrackerClient>>,
    last_tracker_request_time: SystemTime,
    completed_sent_to_tracker: bool,
    info_hash: [u8; 20],
}

impl TrackerRequestor {
    pub fn new(
        peer_id: String,
        trackers_url: Vec<Vec<String>>,
        listening_torrent_wire_protocol_port: u16,
        info_hash: [u8; 20],
    ) -> Self {
        TrackerRequestor {
            tracker_client: Arc::new(Mutex::new(TrackerClient::new(
                peer_id,
                trackers_url,
                listening_torrent_wire_protocol_port,
            ))),
            last_tracker_request_time: SystemTime::UNIX_EPOCH,
            completed_sent_to_tracker: false,
            info_hash,
        }
    }

    // used to send recurring updates to tracker
    pub async fn async_update_to_tracker(
        &mut self,
        advertised_peers: Arc<Mutex<HashMap<HostAndPort, (tracker::Peer, SystemTime)>>>,
        bytes_left: Option<u64>,
        uploaded_downloaded_bytes: (u64, u64),
    ) {
        let tracker_client_mg = self
            .tracker_client
            .lock()
            .expect("another user panicked while holding the lock");
        let tracker_request_interval = tracker_client_mg.tracker_request_interval;
        drop(tracker_client_mg);
        if let Ok(elapsed) = SystemTime::now().duration_since(self.last_tracker_request_time) {
            if elapsed > tracker_request_interval {
                let event = if self.last_tracker_request_time == SystemTime::UNIX_EPOCH {
                    Event::Started
                } else {
                    Event::None
                };
                self.async_request_to_tracker(
                    event,
                    advertised_peers,
                    bytes_left,
                    uploaded_downloaded_bytes,
                )
                .await;
            }
        }
    }

    // used for specific events
    pub async fn async_request_to_tracker(
        &mut self,
        event: Event,
        advertised_peers: Arc<Mutex<HashMap<HostAndPort, (tracker::Peer, SystemTime)>>>,
        bytes_left: Option<u64>,
        uploaded_downloaded_bytes: (u64, u64),
    ) {
        if event == Event::Completed {
            if self.completed_sent_to_tracker {
                return;
            }
            self.completed_sent_to_tracker = true;
        }

        self.last_tracker_request_time = SystemTime::now();
        let (uploaded_bytes, downloaded_bytes) = uploaded_downloaded_bytes;
        let tracker_client_mg = self
            .tracker_client
            .lock()
            .expect("another user panicked while holding the lock");
        let tracker_client = tracker_client_mg.clone();
        drop(tracker_client_mg);
        let tracker_client_arc = self.tracker_client.clone();
        let info_hash = self.info_hash;
        tokio::spawn(async move {
            if let Ok((updated_tracker_client, latest_advertised_peers)) = request_to_tracker(
                tracker_client,
                event,
                bytes_left,
                info_hash,
                uploaded_bytes,
                downloaded_bytes,
            )
            .await
            {
                update_tracker_client_and_advertised_peers(
                    tracker_client_arc,
                    advertised_peers,
                    updated_tracker_client,
                    latest_advertised_peers,
                );
            }
        });
    }
}

async fn request_to_tracker(
    mut tracker_client: TrackerClient,
    event: Event,
    bytes_left: Option<u64>,
    info_hash: [u8; 20],
    uploaded_bytes: u64,
    downloaded_bytes: u64,
) -> Result<(TrackerClient, Vec<tracker::Peer>)> {
    match tracker_client
        .request(
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
                Some(NoTrackerError) => log::info!("could not perform request to tracker: {e}"),
                _ => log::error!("could not perform request to tracker: {e}"),
            }
            bail!(e);
        }
        Ok(Response::Failure(msg)) => {
            log::error!("tracker responded with failure: {msg}");
            bail!(msg);
        }
        Ok(Response::Ok(ok_response)) => {
            if let Some(msg) = ok_response.warning_message.clone() {
                log::error!("tracker sent a warning: {msg}");
            }
            log::info!(
                "tracker request succeeded: seeders: {}; leechers: {}; peers provided: {}",
                ok_response.complete,
                ok_response.incomplete,
                ok_response.peers.len()
            );
            log::trace!("full tracker response:\n{ok_response:?}");

            Ok((tracker_client, ok_response.peers))
        }
    }
}

fn update_tracker_client_and_advertised_peers(
    tracker_client: Arc<Mutex<TrackerClient>>,
    advertised_peers: Arc<Mutex<HashMap<HostAndPort, (tracker::Peer, SystemTime)>>>,
    updated_tracker_client: TrackerClient,
    latest_advertised_peers: Vec<tracker::Peer>,
) {
    let mut tracker_client_mg = tracker_client
        .lock()
        .expect("another user panicked while holding the lock");
    *tracker_client_mg = updated_tracker_client;
    drop(tracker_client_mg);
    let mut advertised_peers = advertised_peers
        .lock()
        .expect("another user panicked while holding the lock");
    latest_advertised_peers.iter().for_each(|p| {
        advertised_peers
            .entry(format!("{}:{}", p.ip, p.port))
            .or_insert((p.clone(), SystemTime::UNIX_EPOCH));
    });
    drop(advertised_peers);
}

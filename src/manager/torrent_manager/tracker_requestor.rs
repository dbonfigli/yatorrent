use anyhow::{Result, bail};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::manager::peer_handler::PeerAddr;
use crate::manager::torrent_manager::TorrentManager;
use crate::tracker;
use crate::tracker::{Event, NoTrackerError, Response, TrackerClient};

impl TorrentManager {
    pub(super) async fn async_request_to_tracker(&mut self, event: Event) {
        self.tracker_state.last_tracker_request_time = SystemTime::now();
        let bytes_left = self.torrent_data_status.as_ref().map(|f| f.bytes_left());
        let info_hash = self.torrent_manager_config.info_hash;
        let uploaded_bytes = self.bandwidth_tracker.uploaded_bytes();
        let downloaded_bytes = self.bandwidth_tracker.downloaded_bytes();
        let advertised_peers = self.peers_state.advertised_peers.clone();
        let tracker_client_mg = self
            .tracker_state
            .tracker_client
            .lock()
            .expect("another user panicked while holding the lock");
        let tracker_client = tracker_client_mg.clone();
        drop(tracker_client_mg);
        let tracker_client_arc = self.tracker_state.tracker_client.clone();
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
    advertised_peers: Arc<Mutex<HashMap<PeerAddr, (tracker::Peer, SystemTime)>>>,
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
        advertised_peers.insert(
            format!("{}:{}", p.ip, p.port),
            (p.clone(), SystemTime::UNIX_EPOCH),
        );
    });
    drop(advertised_peers);
}

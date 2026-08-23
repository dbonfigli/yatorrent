use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{manager::peer::Peer, tracker, util::HostAndPort};

const HIGH_NUMBER_OF_BAD_PEERS: usize = 10000;
const HIGH_NUMBER_OF_ADVERTISED_PEERS: usize = 10000;

pub struct PeersContext {
    // todo: encapsulate this better
    pub peers: HashMap<HostAndPort, Peer>,
    pub advertised_peers: AdvertisedPeers,
    bad_peers: BadPeers,
}

impl PeersContext {
    pub fn new(initial_advertised_peers: Vec<tracker::Peer>) -> Self {
        PeersContext {
            peers: HashMap::new(),
            advertised_peers: AdvertisedPeers::new(initial_advertised_peers),
            bad_peers: BadPeers::new(),
        }
    }

    pub fn insert_bad_peer(&mut self, bad_peer: HostAndPort) {
        self.bad_peers.insert_bad_peer(bad_peer);
    }

    pub fn is_bad_peer(&self, peer: &HostAndPort) -> bool {
        self.bad_peers.is_bad_peer(peer)
    }

    pub fn bad_peers_count(&self) -> usize {
        self.bad_peers.len()
    }
}

#[derive(Clone)]
pub struct AdvertisedPeer {
    pub peer: tracker::Peer,
    pub last_connection_attempt: Option<Instant>,
    known_since: Instant,
}

#[derive(Clone)]
pub struct AdvertisedPeers {
    advertised_peers: Arc<Mutex<HashMap<HostAndPort, AdvertisedPeer>>>,
}

impl AdvertisedPeers {
    fn new(initial_advertised_peers: Vec<tracker::Peer>) -> Self {
        let mut s = AdvertisedPeers {
            advertised_peers: Arc::new(Mutex::new(HashMap::new())),
        };
        s.insert(initial_advertised_peers);
        s
    }

    pub fn update_last_connection_attempt(&mut self, peers: Vec<HostAndPort>) {
        let mut possible_peers_mg = self
            .advertised_peers
            .lock()
            .expect("another user panicked while holding the lock");
        for peer_addr in peers.iter() {
            possible_peers_mg
                .entry(peer_addr.clone())
                .and_modify(|possible_peer_entry| {
                    possible_peer_entry.last_connection_attempt = Some(Instant::now())
                });
        }
    }

    pub fn wipe_last_connection_attempt(&mut self, peers: Vec<HostAndPort>) {
        let mut advertised_peers_mg = self
            .advertised_peers
            .lock()
            .expect("another user panicked while holding the lock");
        for peer_addr in peers {
            if let Some(v) = advertised_peers_mg.get_mut(&peer_addr) {
                v.last_connection_attempt = None;
            }
        }
    }

    pub fn insert(&mut self, peers: Vec<tracker::Peer>) {
        let mut advertised_peers_mg = self
            .advertised_peers
            .lock()
            .expect("another user panicked while holding the lock");
        peers.iter().for_each(|p: &tracker::Peer| {
            advertised_peers_mg
                .entry(format!("{}:{}", p.ip, p.port))
                .or_insert(AdvertisedPeer {
                    peer: p.clone(),
                    last_connection_attempt: None,
                    known_since: Instant::now(),
                });
        });

        if advertised_peers_mg.len() > HIGH_NUMBER_OF_ADVERTISED_PEERS * 2 {
            let to_remove = advertised_peers_mg.len() / 2;

            let mut oldest: Vec<_> = advertised_peers_mg
                .iter()
                .map(|(peer, AdvertisedPeer { known_since, .. })| (peer.clone(), *known_since))
                .collect();
            oldest.sort_unstable_by_key(|(_, timestamp)| *timestamp);

            for (peer, _) in oldest.into_iter().take(to_remove) {
                advertised_peers_mg.remove(&peer);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.advertised_peers
            .lock()
            .expect("another user panicked while holding the lock")
            .len()
    }

    pub fn get_snapshot(&self) -> HashMap<HostAndPort, AdvertisedPeer> {
        self.advertised_peers
            .lock()
            .expect("another user panicked while holding the lock")
            .clone()
    }
}

struct BadPeers {
    bad_peers: HashMap<HostAndPort, Instant>, // peer -> insertion time as bad peer
}

impl BadPeers {
    fn new() -> Self {
        BadPeers {
            bad_peers: HashMap::new(),
        }
    }

    fn insert_bad_peer(&mut self, bad_peer: HostAndPort) {
        self.bad_peers.insert(bad_peer, Instant::now());

        if self.bad_peers.len() > HIGH_NUMBER_OF_BAD_PEERS * 2 {
            let to_remove = self.bad_peers.len() / 2;

            let mut oldest: Vec<_> = self
                .bad_peers
                .iter()
                .map(|(peer, timestamp)| (peer.clone(), *timestamp))
                .collect();
            oldest.sort_unstable_by_key(|(_, timestamp)| *timestamp);

            for (peer, _) in oldest.into_iter().take(to_remove) {
                self.bad_peers.remove(&peer);
            }
        }
    }

    fn is_bad_peer(&self, bad_peer: &HostAndPort) -> bool {
        self.bad_peers.contains_key(bad_peer)
    }

    fn len(&self) -> usize {
        self.bad_peers.len()
    }
}

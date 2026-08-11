use anyhow::Result;
use clap::{CommandFactory, Parser};
use manager::torrent_manager;
use size::{self, Size};
use std::env::current_dir;
use std::process::exit;
use std::{fmt, fs};
use torrent_manager::TorrentManager;

#[cfg(unix)]
use rlimit::{Resource, getrlimit, setrlimit};
#[cfg(unix)]
use std::cmp::min;

use crate::manager::torrent_manager::{
    FilesData, TorrentManagerLimitOptions, TorrentManagerNetworkOptions, TorrentManagerOptions,
    TorrentManagerStorageOptions,
};
use crate::torrent_protocol::MAX_MESSAGE_SIZE_B;

mod bencoding;
mod dht;
mod magnet;
mod manager;
mod metadata;
mod persistence;
mod torrent_protocol;
mod tracker;
mod util;

#[cfg(test)]
#[macro_use]
extern crate assert_matches;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the .torrent file (optional, either this or a magnet link must be provided)
    #[arg(short, long, env)]
    torrent_file: Option<String>,

    /// Magnet Link URI (optional, either this or a torrent file must be provided)
    #[arg(short, long, env)]
    magnet_uri: Option<String>,

    /// Base path where files are downloaded (directory will be created if it does not exist)
    #[arg(short, long, env, default_value_t = current_dir().expect("current directory cannot be used").to_str().expect("current path must be an utf8 string").to_string())]
    base_path: String,

    /// Listening port for Torrent protocol
    #[arg(short, long, env, default_value_t = 8000)]
    port: u16,

    /// Listening port for DHT protocol
    #[arg(short, long, env, default_value_t = 8001)]
    dht_port: u16,

    /// Log level
    #[arg(short, long, env, default_value_t = LogLevels::Info)]
    log_level: LogLevels,

    /// Show detailed stats per peer
    #[arg(short, long, env, default_value_t = false)]
    show_peers_stats: bool,

    /// Maximum number of connected peers allowed
    #[arg(short = 'c', long, env, default_value_t = 100)]
    max_connected_peers: usize,

    /// Max allowerd total download bandwidth, with associated unit, e.g 10MiB (MiB is different from MB, the value is always bytes regardless of the case of "b", optional, no limit if not provided)
    #[arg(short = 'z', long, env)]
    max_download_bandwidth: Option<String>,

    /// Max allowerd total upload bandwidth, with associated unit, e.g 10MiB (MiB is different from MB, the value is always bytes regardless of the case of "b", optional, no limit if not provided)
    #[arg(short = 'u', long, env)]
    max_upload_bandwidth: Option<String>,

    /// Exit the client when the download is complete
    #[arg(short = 'e', long, env, default_value_t = false)]
    exit_when_complete: bool,
}

#[derive(clap::ValueEnum, Debug, Clone)]
enum LogLevels {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl fmt::Display for LogLevels {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("{self:?}").to_lowercase())
    }
}

#[cfg(unix)]
const MAX_OPENED_FILES: u64 = 16384;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // init logging
    env_logger::init_from_env(
        env_logger::Env::default().filter_or("LOG_LEVEL", args.log_level.to_string()),
    );

    // bump ulimit if needed
    #[cfg(unix)]
    {
        let (soft_limit, hard_limit) =
            getrlimit(Resource::NOFILE).expect("could not read current NOFILE ulimit");
        if soft_limit < MAX_OPENED_FILES {
            setrlimit(
                Resource::NOFILE,
                min(hard_limit, MAX_OPENED_FILES),
                hard_limit,
            )
            .expect("could not increase NOFILE ulimit");
        }
    }

    let max_download_bandwidth = get_bandwitdh(args.max_download_bandwidth).map(|d| {
        log::info!("capping download bandwidth at {d}");
        d.bytes()
    });

    let max_upload_bandwidth = get_bandwitdh(args.max_upload_bandwidth).map(|d| {
        log::info!("capping upload bandwidth at {d}");
        d.bytes()
    });

    // read torrent file and start manager
    if let Some(torrent_file) = args.torrent_file {
        let contents = match fs::read(&torrent_file) {
            Ok(c) => c,
            Err(e) => {
                log::error!("could not read .torrent file {torrent_file}: {e}");
                exit(1);
            }
        };
        let torrent_content = bencoding::Value::new(&contents);
        let metainfo = metadata::metainfo::Metainfo::new(&torrent_content, &contents);
        match metainfo {
            Ok(m) => {
                log::info!("torrent file metainfo:\n{m}");
                if m.announce_list.is_empty() {
                    if !m.url_list.is_empty() {
                        log::warn!(
                            "The .torrent file contains a \"url-list\" field, this means the torrent can be downloaded via HTTP/FTP http://www.bittorrent.org/beps/bep_0019.html), this is not supported by this client"
                        );
                    }
                    log::warn!(
                        "The .torrent file does not contain valid announces (\"announce-list\" or \"announce\" fields): this is a trackless torrent relying only on DHT"
                    );
                }
                if !m.nodes.is_empty() {
                    log::info!(
                        "The .torrent file contains a \"nodes\" field, the torrent is announcing also via specific DHT nodes"
                    );
                }
                TorrentManager::new(TorrentManagerOptions {
                    info_hash: m.info_hash,
                    network_opts: TorrentManagerNetworkOptions {
                        listening_torrent_wire_protocol_port: args.port,
                        listening_dht_port: args.dht_port,
                        dht_nodes: m.nodes.clone(),
                        initial_peers: Vec::new(),
                    },
                    storage_opts: TorrentManagerStorageOptions {
                        base_path: args.base_path,
                        files_data: Some(FilesData {
                            file_list: m.get_files(),
                            piece_length: m.piece_length,
                            piece_hashes: m.piece_hashes,
                        }),
                        raw_metadata: Some(m.raw_metadata),
                    },
                    limit_opts: TorrentManagerLimitOptions {
                        max_connected_peers: args.max_connected_peers,
                        max_download_bandwidth,
                        max_upload_bandwidth,
                    },
                    tracker_announce_list: m.announce_list.clone(),
                    show_peers_stats: args.show_peers_stats,
                    exit_when_complete: args.exit_when_complete,
                })
                .start()
                .await;
                exit(0);
            }
            Err(e) => {
                log::error!("The .torrent file is invalid: could not parse metainfo: {e}");
                exit(1)
            }
        }
    } else if let Some(magnet_uri) = args.magnet_uri {
        match magnet::Magnet::new(magnet_uri) {
            Ok(magnet) => {
                TorrentManager::new(TorrentManagerOptions {
                    info_hash: magnet.info_hash,
                    network_opts: TorrentManagerNetworkOptions {
                        listening_torrent_wire_protocol_port: args.port,
                        listening_dht_port: args.dht_port,
                        dht_nodes: Vec::new(),
                        initial_peers: magnet.peer_addresses,
                    },
                    storage_opts: TorrentManagerStorageOptions {
                        base_path: args.base_path,
                        files_data: None,
                        raw_metadata: None,
                    },
                    limit_opts: TorrentManagerLimitOptions {
                        max_connected_peers: args.max_connected_peers,
                        max_download_bandwidth,
                        max_upload_bandwidth,
                    },
                    tracker_announce_list: vec![magnet.tracker_urls],
                    show_peers_stats: args.show_peers_stats,
                    exit_when_complete: args.exit_when_complete,
                })
                .start()
                .await;
                exit(0);
            }
            Err(e) => {
                log::error!("Could not parse magnet link: {e}");
                exit(1)
            }
        }
    }

    log::error!("A magnet link (-m) or .torrent file (-t) must be provided.");
    Args::command()
        .print_help()
        .expect("contradictory arguments");
    exit(1);
}

fn get_bandwitdh(bandwidth: Option<String>) -> Option<Size> {
    bandwidth.map(|b| match Size::from_str(&b) {
        Err(e) => {
            log::error!("could not parse bandwidth {b}: {e}");
            exit(1)
        }
        Ok(v) => {
            if v.bytes() <= MAX_MESSAGE_SIZE_B.try_into().unwrap() {
                log::error!("bandwitdh limit cannot be less than {MAX_MESSAGE_SIZE_B} bytes (the size of the biggest torrent protocol message we support)");
                exit(1)
            }
            v
        }
    })
}

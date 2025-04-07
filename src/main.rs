use anyhow::Result;
use clap::{CommandFactory, Parser};
use manager::torrent_manager;
use rlimit::{getrlimit, setrlimit, Resource};
use std::cmp::min;
use std::env::current_dir;
use std::path::Path;
use std::process::exit;
use std::{fmt, fs};

use torrent_manager::TorrentManager;

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

    /// Optional base path where files are downloaded (directory will be created if it does not exist)
    #[arg(short, long, env, default_value_t = current_dir().unwrap().to_str().expect("current path must be an utf8 string").to_string())]
    base_path: String,

    /// Optional listening port
    #[arg(short, long, env, default_value_t = 8000)]
    port: u16,

    /// Optional listening port for DHT protocol
    #[arg(short, long, env, default_value_t = 8001)]
    dht_port: u16,

    /// Optional log level
    #[arg(short, long, env, default_value_t = LogLevels::Info)]
    log_level: LogLevels,
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
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

const MAX_OPENED_FILES: u64 = 16384;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let base_path = Path::new(&args.base_path);

    // init logging
    env_logger::init_from_env(
        env_logger::Env::default().filter_or("LOG_LEVEL", args.log_level.to_string()),
    );

    // bump ulimit if needed
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

    // read torrent file and start manager
    if let Some(torrent_file) = args.torrent_file {
        let contents = match fs::read(&torrent_file) {
            Ok(c) => c,
            Err(e) => {
                log::error!("could not read .torrent file {}: {e}", torrent_file);
                exit(1);
            }
        };
        let torrent_content = bencoding::Value::new(&contents);
        let metainfo = metadata::metainfo::Metainfo::new(&torrent_content, &contents);
        match metainfo {
            Ok(m) => {
                log::info!("torrent file metainfo:\n{m}");
                if m.announce_list.len() == 0 {
                    if m.url_list.len() != 0 {
                        log::warn!("The .torrent file contains a \"url-list\" field, this means the torrent can be downloaded via HTTP/FTP http://www.bittorrent.org/beps/bep_0019.html), this is not supported by this client");
                    }
                    log::warn!("The .torrent file does not contain valid announces (\"announce-list\" or \"announce\" fields): this is a trackless torrent relying only on DHT");
                }
                if m.nodes.len() != 0 {
                    log::info!("The .torrent file contains a \"nodes\" field, the torrent is announcing also via specific DHT nodes");
                }
                TorrentManager::new(
                    m.info_hash,
                    base_path,
                    args.port,
                    m.announce_list.clone(),
                    Some((m.get_files(), m.piece_length, m.pieces)),
                    Some(m.raw_metadata),
                    // dht data
                    args.dht_port,
                    m.nodes,
                    Vec::new(),
                )
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
                TorrentManager::new(
                    magnet.info_hash,
                    base_path,
                    args.port,
                    vec![magnet.tracker_urls],
                    None,
                    None,
                    // dht data
                    args.dht_port,
                    Vec::new(),
                    magnet.peer_addresses,
                )
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
    Args::command().print_help().unwrap();
    exit(1);
}

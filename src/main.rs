use anyhow::Result;
use clap::Parser;
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
mod manager;
mod metainfo;
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
    /// Path to the .torrent file
    #[arg(short, long, env)]
    torrent_file: String,

    /// Optional base path where files are stored (directory will be created if it does not exist)
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

static MAX_OPENED_FILES: u64 = 16384;

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
    let contents = match fs::read(args.torrent_file.clone()) {
        Ok(c) => c,
        Err(e) => {
            log::error!("could not read .torrent file {}: {}", args.torrent_file, e);
            exit(1);
        }
    };
    let torrent_content = bencoding::Value::new(&contents);
    let metainfo = metainfo::Metainfo::new(&torrent_content, &contents);
    match metainfo {
        Ok(m) => {
            log::info!("torrent file metainfo:\n{}", m);
            if m.announce_list.len() == 0 {
                if m.url_list.len() != 0 {
                    log::warn!("The .torrent file contains a \"url-list\" field, this means the torrent can be dowloaded via HTTP/FTP http://www.bittorrent.org/beps/bep_0019.html), this is not supported by this client");
                }
                log::warn!("The .torrent file does not contain valid announces (\"announce-list\" or \"announce\" fields): this is a trackless torrent relying only on DHT");
            }
            if m.nodes.len() != 0 {
                log::info!("The .torrent file contains a \"nodes\" field, the torrent is announcing also via specific DHT nodes");
            }
            TorrentManager::new(base_path, args.port, args.dht_port, m)
                .start()
                .await;
            exit(0);
        }
        Err(e) => {
            log::error!(
                "The .torrent file is invalid: could not parse metainfo: {}",
                e
            );
            exit(1)
        }
    }
}

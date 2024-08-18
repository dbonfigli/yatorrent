use std::env::current_dir;
use std::error::Error;
use std::path::Path;
use std::process::exit;
use std::{env, fs};

use torrent_manager::TorrentManager;

mod bencoding;
mod file_manager;
mod metainfo;
mod peer;
mod piece;
mod tcp_wire_protocol;
mod torrent_manager;
mod tracker;
mod wire_protocol;

#[cfg(test)]
#[macro_use]
extern crate assert_matches;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // init logging
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    // get command parameters
    let args: Vec<_> = env::args().collect();
    if args.len() < 2 {
        println!("Must include the name of a .torrent file as first argument\nsecond argument is optional and is the base path where the files will be downloaded\n third argument is optional and is the listening port (default: 8000)");
        exit(1);
    }
    let torrent_file = args.get(1).unwrap();
    let current_path = current_dir().unwrap();
    let base_path = match args.get(2) {
        Some(p) => Path::new(p),
        None => current_path.as_path(),
    };
    let port = match args.get(3) {
        Some(p) => p
            .parse::<i32>()
            .expect("could not convert port argument to integer"),
        None => 8000,
    };

    let contents = fs::read(torrent_file).unwrap();
    let torrent_content = bencoding::Value::new(&contents);
    let metainfo = metainfo::Metainfo::new(&torrent_content, &contents);
    match metainfo {
        Ok(m) => {
            log::info!("torrent file metainfo:\n{}", m);
            TorrentManager::new(base_path, port, m).start().await;
            exit(0);
        }
        Err(e) => {
            log::error!(
                "The .torrent file is invalid: could not parse metainfo: {:?}",
                e
            );
            exit(1)
        }
    }
}

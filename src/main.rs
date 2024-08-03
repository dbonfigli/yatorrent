use std::env::current_dir;
use std::error::Error;
use std::path::Path;
use std::process::exit;
use std::{env, fs, str};

use file_manager::FileManager;
use metainfo::pretty_info_hash;
use tokio::net::TcpStream;
use tracker::TrackerClient;
use tracker::{Event, Response};
use wire_protocol::Protocol;

mod bencoding;
mod file_manager;
mod metainfo;
mod tcp_wire_protocol;
mod tracker;
mod wire_protocol;

#[cfg(test)]
#[macro_use]
extern crate assert_matches;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // init logging
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "debug"),
    );

    // get command parameters
    let args: Vec<_> = env::args().collect();
    if args.len() < 2 {
        println!("Must include the name of a .torrent file as first argument, second argument is optional and is the base path where the files will be downloaded");
        exit(1);
    }
    let torrent_file = args.get(1).unwrap();
    let current_path = current_dir().unwrap();
    let base_path = match args.get(2) {
        Some(p) => Path::new(p),
        None => current_path.as_path(),
    };

    let contents = fs::read(torrent_file).unwrap();
    let torrent_content = bencoding::Value::new(&contents);
    let metainfo = metainfo::Metainfo::new(&torrent_content, &contents);
    match metainfo {
        Ok(m) => {
            log::info!("torrent file metainfo:\n{}", m);

            let mut manager = FileManager::new(base_path, m.get_files(), m.piece_length, m.pieces);

            manager.refresh_completed_pieces();
            manager.refresh_completed_files();

            //log::info!("refreshed: {:?}", manager.piece_completed);

            // let mut tracker_client: TrackerClient = TrackerClient::new(m.announce, 1234);
            // let result = tracker_client
            //     .request(m.info_hash, 0, 0, 100, Event::Started)
            //     .await;
            // match result {
            //     Ok(response) => {
            //         log::info!("tracker request suceeded, tracker response:\n{}", response);

            //         match response {
            //             Response::Ok(ok_response) => {
            //                 if ok_response.peers.len() > 0 {
            //                     let dest = format!(
            //                         "{}:{}",
            //                         ok_response.peers[0].ip, ok_response.peers[0].port
            //                     );
            //                     log::info!("connecting to: {}", dest);
            //                     let mut stream = TcpStream::connect(dest).await?;
            //                     let (peer_protocol, _, info_hash, peer_id) = stream
            //                         .handshake(
            //                             m.info_hash,
            //                             tracker_client.peer_id.as_bytes().try_into()?,
            //                         )
            //                         .await?;
            //                     log::info!(
            //                         "recevied handshake info: peer protocol: {}, info_hash: {}, peer_id: {}",
            //                         peer_protocol,
            //                         pretty_info_hash(info_hash),
            //                         str::from_utf8(&peer_id)?
            //                     );
            //                     let rec = stream.receive().await?;
            //                     log::info!("rec: {:#?}", rec);
            //                     let rec = stream.receive().await?;
            //                     log::info!("rec: {:#?}", rec);
            //                     let rec = stream.receive().await?;
            //                     log::info!("rec: {:#?}", rec);
            //                 }
            //             }
            //             Response::Failure(err_message) => {}
            //         }

            //         exit(0);
            //     }
            //     Err(err) => {
            //         log::info!("tracker request failed: {:?}", err);
            //         exit(1);
            //     }
            // }
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

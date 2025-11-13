use core::str;
use std::{ascii, time::Duration};

use tokio::{sync::mpsc::Sender, time};

pub fn force_string(v: &Vec<u8>) -> String {
    str::from_utf8(v)
        .unwrap_or(
            format!(
                "<non_utf-8>{}",
                str::from_utf8(
                    &v.iter()
                        .flat_map(|b| ascii::escape_default(*b))
                        .collect::<Vec<u8>>()
                )
                .unwrap_or("??")
            )
            .as_str(),
        )
        .to_string()
}

pub fn pretty_info_hash(info_hash: [u8; 20]) -> String {
    info_hash
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join("")
}

pub async fn start_tick(tick_tx: Sender<()>, duration: Duration) {
    tokio::spawn(async move {
        let mut interval = time::interval(duration);
        loop {
            interval.tick().await;
            tick_tx.send(()).await.expect("tick receiver half closed");
        }
    });
}

use std::error::Error;

use tokio::net::UdpSocket;

use rand::Rng;

// NOTE! we are only supporting IPv4 DHT, i.e. BEP 32 (https://www.bittorrent.org/beps/bep_0032.html) is not implemented

pub struct DhtClient {
    listening_dht_port: u16,
    listening_torrent_wire_protocol_port: u16,
    own_node_id: String,
}

fn generate_transaction_id() -> [u8; 2] {
    let mut rng = rand::thread_rng();
    [rng.gen(), rng.gen()]
}

impl DhtClient {
    pub fn new(
        listening_torrent_wire_protocol_port: u16,
        listening_dht_port: u16,
        own_node_id: String,
    ) -> DhtClient {
        return DhtClient {
            listening_dht_port,
            listening_torrent_wire_protocol_port,
            own_node_id,
        };
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let socket = UdpSocket::bind(format!("0.0.0.0:{}", self.listening_dht_port)).await?;

        Ok(())
    }

    // pub async fn sendQuery(dest: String, query: KRPCMessage) {
    //     match query {
    //         Query::Ping => {
    //             let buf = Value::encode(Value::dict()
    //         }
    //         Query::FindNode => todo!(),
    //         Query::GetPeers => todo!(),
    //         Query::AnnouncePeer => todo!(),
    //     }
    // }
}

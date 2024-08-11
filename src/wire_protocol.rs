use parse_display::Display;
use std::{error::Error, fmt};

#[derive(Debug)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),                // piece index
    Bitfield(Vec<bool>),      // the high bit in the first byte corresponds to piece index 0
    Request(u32, u32, u32),   // index, begin, length
    Piece(u32, u32, Vec<u8>), // index, begin, block of data
    Cancel(u32, u32, u32),    // index, begin, length
    Port(u16),                // port number
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Message::KeepAlive => {
                write!(f, "keep-alive")
            }
            Message::Choke => {
                write!(f, "choke")
            }
            Message::Unchoke => {
                write!(f, "unchoke")
            }
            Message::Interested => {
                write!(f, "interested")
            }
            Message::NotInterested => {
                write!(f, "not interested")
            }
            Message::Have(piece_idx) => {
                write!(f, "have piece id {}", piece_idx)
            }
            Message::Bitfield(bitfield) => {
                let total_have = bitfield
                    .iter()
                    .fold(0, |acc, v| if *v { acc + 1 } else { acc });
                write!(
                    f,
                    "bitfield have {} total: {} (bitfield comes in bytes, number of pieces could be less)",
                    total_have,
                    bitfield.len()
                )
            }
            Message::Request(piece_idx, begin, end) => {
                write!(
                    f,
                    "request: piece idx: {}, begin: {}, end: {}",
                    piece_idx, begin, end
                )
            }
            Message::Piece(piece_idx, begin, data) => {
                write!(
                    f,
                    "piece: piece idx: {}, begin: {}, data len: {}",
                    piece_idx,
                    begin,
                    data.len()
                )
            }
            Message::Cancel(piece_idx, begin, end) => {
                write!(
                    f,
                    "cancel: piece idx: {}, begin: {}, end: {}",
                    piece_idx, begin, end
                )
            }
            Message::Port(p) => {
                write!(f, "port {}", p)
            }
        }
    }
}

#[trait_variant::make(Protocol: Send)]
pub trait LocalProtocol {
    #[allow(dead_code)]
    async fn handshake(
        &mut self,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
    ) -> Result<(String, [u8; 8], [u8; 20], [u8; 20]), Box<dyn Error + Send + Sync>>; // pstr, reserved, info_hash, peer_id
}

#[trait_variant::make(ProtocolReadHalf: Send)]
pub trait LocalProtocolReadHalf {
    #[allow(dead_code)]
    async fn receive(&mut self) -> Result<Message, Box<dyn Error + Send + Sync>>;
}

#[trait_variant::make(ProtocolWriteHalf: Send)]
pub trait LocalProtocolWriteHalf {
    #[allow(dead_code)]
    async fn send(&mut self, message: Message) -> Result<(), Box<dyn Error + Send + Sync>>;
}

#[derive(Display, Debug)]
pub struct ProtocolError {
    message: String,
}

impl ProtocolError {
    pub fn new(message: String) -> Self {
        ProtocolError { message }
    }
}

impl Error for ProtocolError {}

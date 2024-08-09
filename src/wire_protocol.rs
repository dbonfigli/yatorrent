use parse_display::Display;
use std::error::Error;

#[derive(Debug)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),                // piece index
    Bitfield(Vec<bool>),      // the high bit in the first byte corresponds to piece index 0
    Request(u32, u32, u32),   // index, begin, lenght
    Piece(u32, u32, Vec<u8>), // index, begin, block of data
    Cancel(u32, u32, u32),    // index, begin, lenght
    Port(u16),                // port number
}

#[trait_variant::make(Protocol: Send)]
pub trait LocalProtocol {
    async fn handshake(
        &mut self,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
    ) -> Result<(String, [u8; 8], [u8; 20], [u8; 20]), Box<dyn Error + Send + Sync>>; // pstr, reserved, info_hash, peer_id
}

#[trait_variant::make(ProtocolReadHalf: Send)]
pub trait LocalProtocolReadHalf {
    async fn receive(&mut self) -> Result<Message, Box<dyn Error + Send + Sync>>;
}

#[trait_variant::make(ProtocolWriteHalf: Send)]
pub trait LocalProtocolWriteHalf {
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

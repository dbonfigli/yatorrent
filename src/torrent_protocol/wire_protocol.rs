use std::{error::Error, fmt};

use crate::bencoding::Value;
use anyhow::Result;

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
    Extended(u8, Value),      // Extension Protocol id, bencoded message
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
                write!(f, "have piece id {piece_idx}")
            }
            Message::Bitfield(bitfield) => {
                let total_have = bitfield
                    .iter()
                    .fold(0, |acc, v| if *v { acc + 1 } else { acc });
                write!(
                    f,
                    "bitfield have {total_have} total: {} (bitfield comes in bytes, number of pieces could be less)",
                    bitfield.len()
                )
            }
            Message::Request(piece_idx, begin, length) => {
                write!(
                    f,
                    "request: piece idx: {piece_idx}, begin: {begin}, length: {length}"
                )
            }
            Message::Piece(piece_idx, begin, data) => {
                write!(
                    f,
                    "piece: piece idx: {piece_idx}, begin: {begin}, data len: {}",
                    data.len()
                )
            }
            Message::Cancel(piece_idx, begin, length) => {
                write!(
                    f,
                    "cancel: piece idx: {piece_idx}, begin: {begin}, length: {length}",
                )
            }
            Message::Port(p) => {
                write!(f, "port {p}")
            }
            Message::Extended(id, value) => {
                write!(f, "extension message: extension id: {id}, value: {value}")
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
    ) -> Result<(String, [u8; 8], [u8; 20], [u8; 20])>; // pstr, reserved, info_hash, peer_id
}

#[trait_variant::make(ProtocolReadHalf: Send)]
pub trait LocalProtocolReadHalf {
    #[allow(dead_code)]
    async fn receive(&mut self) -> Result<Message>;
}

#[trait_variant::make(ProtocolWriteHalf: Send)]
pub trait LocalProtocolWriteHalf {
    #[allow(dead_code)]
    async fn send(&mut self, message: Message) -> Result<()>;
}

#[derive(Debug)]
pub struct ProtocolError {
    message: String,
}

impl ProtocolError {
    pub fn new(message: String) -> Self {
        ProtocolError { message }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for ProtocolError {}

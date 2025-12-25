use std::fmt;

use crate::bencoding::Value;
use anyhow::Result;
use thiserror::Error;

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
pub struct BlockRequest {
    pub piece_idx: u32,
    pub block_begin: u32,
    pub data_len: u32,
}

#[derive(Debug)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),           // piece index
    Bitfield(Vec<bool>), // the high bit in the first byte corresponds to piece index 0
    Request(BlockRequest),
    Piece(u32, u32, Vec<u8>), // index, begin, block of data
    Cancel(BlockRequest),
    Port(u16),    // port number
    Suggest(u32), // piece index
    HaveAll,
    HaveNone,
    Reject(BlockRequest),
    AllowerdFast(u32),            // piece index
    Extended(u8, Value, Vec<u8>), // Extension Protocol id, bencoded message, additional raw data (optional, can be 0)
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
            Message::Request(block_request) => {
                write!(
                    f,
                    "request: piece idx: {}, begin: {}, length: {}",
                    block_request.piece_idx, block_request.block_begin, block_request.data_len
                )
            }
            Message::Piece(piece_idx, begin, data) => {
                write!(
                    f,
                    "piece: piece idx: {piece_idx}, begin: {begin}, data len: {}",
                    data.len()
                )
            }
            Message::Cancel(block_request) => {
                write!(
                    f,
                    "cancel: piece idx: {}, begin: {}, length: {}",
                    block_request.piece_idx, block_request.block_begin, block_request.data_len
                )
            }
            Message::Port(p) => {
                write!(f, "port {p}")
            }

            Message::Suggest(piece_idx) => {
                write!(f, "suggest piece id {piece_idx}")
            }
            Message::HaveAll => {
                write!(f, "have all")
            }
            Message::HaveNone => {
                write!(f, "have none")
            }
            Message::Reject(block_request) => {
                write!(
                    f,
                    "reject: piece idx: {}, begin: {}, length: {}",
                    block_request.piece_idx, block_request.block_begin, block_request.data_len
                )
            }
            Message::AllowerdFast(piece_idx) => {
                write!(f, "allowed fast piece id {piece_idx}")
            }
            Message::Extended(id, value, additional_data) => {
                write!(
                    f,
                    "extension message: extension id: {id}, value: {value}, additional data len: {}",
                    additional_data.len()
                )
            }
        }
    }
}

#[trait_variant::make(Send)]
pub trait Protocol {
    async fn handshake(
        &mut self,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
    ) -> Result<(String, [u8; 8], [u8; 20], [u8; 20])>; // pstr, reserved, info_hash, peer_id
}

#[trait_variant::make(Send)]
pub trait ProtocolReadHalf {
    async fn receive(&mut self) -> Result<Message>;
}

#[trait_variant::make(Send)]
pub trait ProtocolWriteHalf {
    async fn send(&mut self, message: Message) -> Result<()>;
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct ProtocolError {
    message: String,
}

impl ProtocolError {
    pub fn new(message: String) -> Self {
        ProtocolError { message }
    }
}

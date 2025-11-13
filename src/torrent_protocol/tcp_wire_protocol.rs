use anyhow::{Result, bail};
use core::str;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    join,
    net::TcpStream,
};

use crate::{
    bencoding::Value,
    torrent_protocol::wire_protocol::{
        Message, Protocol, ProtocolError, ProtocolReadHalf, ProtocolWriteHalf,
    },
};

impl Protocol for TcpStream {
    async fn handshake(
        &mut self,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        // peer_protocol, reserved, peer_info_hash, peer_id
    ) -> Result<(String, [u8; 8], [u8; 20], [u8; 20])> {
        let peer_addr = self.peer_addr()?;
        log::trace!("peer {}: performing handshake", &peer_addr);

        let (mut read, mut write) = tokio::io::split(self);

        let (write_result, read_result) = join!(
            //send
            async {
                log::trace!("peer {}: sending handshake", &peer_addr);
                let mut buf: [u8; 68] = [0; 68];
                buf[0] = 19;
                buf[1..20].copy_from_slice(b"BitTorrent protocol");
                buf[25] = 0x10; // send support for Extension Protocol
                buf[27] = 1u8; // send support for DHT
                buf[28..48].copy_from_slice(&info_hash);
                buf[48..68].copy_from_slice(&peer_id);
                return if let Err(e) = write.write_all(&buf).await {
                    Err(e)
                } else {
                    log::trace!("peer {}: full handshake sent", &peer_addr);
                    Ok(())
                };
            },
            // receive
            async {
                log::trace!("peer {}: receiving handshake", &peer_addr);

                let mut pstr_len_buf: [u8; 1] = [0; 1];
                if let Err(e) = read.read_exact(&mut pstr_len_buf).await {
                    return Err(e);
                }

                let mut pstr_buf: Vec<u8> = vec![0; pstr_len_buf[0].into()];
                if let Err(e) = read.read_exact(&mut pstr_buf).await {
                    return Err(e);
                }

                let pstr = str::from_utf8(&pstr_buf)
                    .unwrap_or("unknown non utf8 protocol string")
                    .to_string();

                let mut reserved_buf: [u8; 8] = [0; 8];
                if let Err(e) = read.read_exact(&mut reserved_buf).await {
                    return Err(e);
                }

                let mut info_hash_buf: [u8; 20] = [0; 20];
                if let Err(e) = read.read_exact(&mut info_hash_buf).await {
                    return Err(e);
                }

                log::trace!(
                    "peer {}: first part of handshake received, receiving handshake peer id",
                    &peer_addr
                );
                let mut peer_id: [u8; 20] = [0; 20];
                if let Err(e) = read.read_exact(&mut peer_id).await {
                    return Err(e);
                }

                log::trace!("peer {}: full handshake received", &peer_addr);
                return Ok((pstr, reserved_buf, info_hash_buf, peer_id));
            }
        );

        if let Err(e) = write_result {
            bail!(e);
        }
        Ok(read_result?)
    }
}

impl ProtocolWriteHalf for WriteHalf<TcpStream> {
    async fn send(&mut self, message: Message) -> Result<()> {
        match message {
            Message::KeepAlive => {
                let buf: [u8; 4] = [0; 4];
                if let Err(e) = self.write_all(&buf).await {
                    Err(e.into())
                } else {
                    Ok(())
                }
            }
            Message::Choke => {
                let mut buf: [u8; 5] = [0; 5];
                buf[3] = 1;
                if let Err(e) = self.write_all(&buf).await {
                    Err(e.into())
                } else {
                    Ok(())
                }
            }
            Message::Unchoke => {
                let mut buf: [u8; 5] = [0; 5];
                buf[3] = 1;
                buf[4] = 1;
                if let Err(e) = self.write_all(&buf).await {
                    Err(e.into())
                } else {
                    Ok(())
                }
            }
            Message::Interested => {
                let mut buf: [u8; 5] = [0; 5];
                buf[3] = 1;
                buf[4] = 2;
                if let Err(e) = self.write_all(&buf).await {
                    Err(e.into())
                } else {
                    Ok(())
                }
            }
            Message::NotInterested => {
                let mut buf: [u8; 5] = [0; 5];
                buf[3] = 1;
                buf[4] = 3;
                if let Err(e) = self.write_all(&buf).await {
                    Err(e.into())
                } else {
                    Ok(())
                }
            }
            Message::Have(piece_num) => {
                let mut buf: [u8; 9] = [0; 9];
                buf[3] = 5;
                buf[4] = 4;
                buf[5..9].copy_from_slice(&piece_num.to_be_bytes());
                if let Err(e) = self.write_all(&buf).await {
                    Err(e.into())
                } else {
                    Ok(())
                }
            }
            Message::Bitfield(bitfield) => {
                let buf = encode_bitfield(bitfield);
                if let Err(e) = self.write_all(&buf).await {
                    Err(e.into())
                } else {
                    Ok(())
                }
            }
            Message::Request(index, begin, end) => {
                let mut buf: [u8; 17] = [0; 17];
                buf[3] = 13;
                buf[4] = 6;
                buf[5..9].copy_from_slice(&index.to_be_bytes());
                buf[9..13].copy_from_slice(&begin.to_be_bytes());
                buf[13..17].copy_from_slice(&end.to_be_bytes());
                if let Err(e) = self.write_all(&buf).await {
                    Err(e.into())
                } else {
                    Ok(())
                }
            }
            Message::Piece(index, begin, block) => {
                let mut buf = vec![0; 13 + block.len()];
                buf[0..4].copy_from_slice(&(9 + block.len() as u32).to_be_bytes());
                buf[4] = 7;
                buf[5..9].copy_from_slice(&index.to_be_bytes());
                buf[9..13].copy_from_slice(&begin.to_be_bytes());
                buf[13..].copy_from_slice(&block);
                if let Err(e) = self.write_all(&buf).await {
                    Err(e.into())
                } else {
                    Ok(())
                }
            }
            Message::Cancel(index, begin, end) => {
                let mut buf: [u8; 17] = [0; 17];
                buf[3] = 13;
                buf[4] = 8;
                buf[5..9].copy_from_slice(&index.to_be_bytes());
                buf[9..13].copy_from_slice(&begin.to_be_bytes());
                buf[13..17].copy_from_slice(&end.to_be_bytes());
                if let Err(e) = self.write_all(&buf).await {
                    Err(e.into())
                } else {
                    Ok(())
                }
            }
            Message::Port(port) => {
                let mut buf: [u8; 7] = [0; 7];
                buf[3] = 3;
                buf[4] = 9;
                buf[5..7].copy_from_slice(&port.to_be_bytes());
                if let Err(e) = self.write_all(&buf).await {
                    Err(e.into())
                } else {
                    Ok(())
                }
            }
            Message::Extended(id, value, additional_data) => {
                let encoded_value = value.encode();
                let mut buf = vec![0; 6 + encoded_value.len() + additional_data.len()];
                buf[0..4].copy_from_slice(
                    &(2 + encoded_value.len() as u32 + additional_data.len() as u32).to_be_bytes(),
                );
                buf[4] = 20; // extension protocol magic number
                buf[5] = id;
                buf[6..6 + encoded_value.len()].copy_from_slice(&encoded_value);
                buf[6 + encoded_value.len()..].copy_from_slice(&additional_data);
                if let Err(e) = self.write_all(&buf).await {
                    Err(e.into())
                } else {
                    Ok(())
                }
            }
        }
    }
}

impl ProtocolReadHalf for ReadHalf<TcpStream> {
    async fn receive(&mut self) -> Result<Message> {
        // get size of message
        let mut size_message_buf: [u8; 4] = [0; 4];
        if let Err(e) = self.read_exact(&mut size_message_buf).await {
            return Err(e.into());
        }
        let size_message: u32 = u32::from_be_bytes(size_message_buf);
        if size_message == 0 {
            return Ok(Message::KeepAlive);
        }

        // get type of message
        let mut type_message_buf: [u8; 1] = [0; 1];
        if let Err(e) = self.read_exact(&mut type_message_buf).await {
            return Err(e.into());
        }
        match type_message_buf[0] {
            // choke
            0 => Ok(Message::Choke),
            // unchoke
            1 => Ok(Message::Unchoke),
            // interested
            2 => Ok(Message::Interested),
            // not interested
            3 => Ok(Message::NotInterested),
            // have
            4 => {
                let mut buf: [u8; 4] = [0; 4];
                if let Err(e) = self.read_exact(&mut buf).await {
                    return Err(e.into());
                }
                Ok(Message::Have(u32::from_be_bytes(buf)))
            }
            // bitfield
            5 => {
                let bitfield_byte_size: usize = (size_message - 1).try_into()?;
                let mut buf = vec![0; bitfield_byte_size];
                if let Err(e) = self.read_exact(&mut buf).await {
                    return Err(e.into());
                }
                let bitfield = decode_bitfield(buf);
                Ok(Message::Bitfield(bitfield))
            }
            // request
            6 => {
                let mut index_buf: [u8; 4] = [0; 4];
                if let Err(e) = self.read_exact(&mut index_buf).await {
                    return Err(e.into());
                }
                let mut begin_buf: [u8; 4] = [0; 4];
                if let Err(e) = self.read_exact(&mut begin_buf).await {
                    return Err(e.into());
                }
                let mut end_buf: [u8; 4] = [0; 4];
                if let Err(e) = self.read_exact(&mut end_buf).await {
                    return Err(e.into());
                }
                Ok(Message::Request(
                    u32::from_be_bytes(index_buf),
                    u32::from_be_bytes(begin_buf),
                    u32::from_be_bytes(end_buf),
                ))
            }
            // piece
            7 => {
                let mut index_buf: [u8; 4] = [0; 4];
                if let Err(e) = self.read_exact(&mut index_buf).await {
                    return Err(e.into());
                }
                let mut begin_buf: [u8; 4] = [0; 4];
                if let Err(e) = self.read_exact(&mut begin_buf).await {
                    return Err(e.into());
                }
                let block_size: usize = (size_message - 9).try_into()?;
                let mut block_buf = vec![0; block_size];
                if let Err(e) = self.read_exact(&mut block_buf).await {
                    return Err(e.into());
                }
                Ok(Message::Piece(
                    u32::from_be_bytes(index_buf),
                    u32::from_be_bytes(begin_buf),
                    block_buf,
                ))
            }
            // cancel
            8 => {
                let mut index_buf: [u8; 4] = [0; 4];
                if let Err(e) = self.read_exact(&mut index_buf).await {
                    return Err(e.into());
                }
                let mut begin_buf: [u8; 4] = [0; 4];
                if let Err(e) = self.read_exact(&mut begin_buf).await {
                    return Err(e.into());
                }
                let mut end_buf: [u8; 4] = [0; 4];
                if let Err(e) = self.read_exact(&mut end_buf).await {
                    return Err(e.into());
                }
                Ok(Message::Cancel(
                    u32::from_be_bytes(index_buf),
                    u32::from_be_bytes(begin_buf),
                    u32::from_be_bytes(end_buf),
                ))
            }
            // port
            9 => {
                let mut buf: [u8; 2] = [0; 2];
                if let Err(e) = self.read_exact(&mut buf).await {
                    return Err(e.into());
                }
                Ok(Message::Port(u16::from_be_bytes(buf)))
            }
            // extension message
            20 => {
                let mut buf: [u8; 1] = [0; 1];
                if let Err(e) = self.read_exact(&mut buf).await {
                    return Err(e.into());
                }
                let extended_message_id = buf[0]; // i.e. id of the extension. 0 means this message is an extension handshake
                let payload_extended_message_size: usize = (size_message - 2).try_into()?;
                let mut buf = vec![0; payload_extended_message_size];
                if let Err(e) = self.read_exact(&mut buf).await {
                    return Err(e.into());
                }
                let (extended_message, dict_size) = Value::new_with_size(&buf);
                Ok(Message::Extended(
                    extended_message_id,
                    extended_message,
                    buf[dict_size..].to_vec(),
                ))
            }
            unknown_message_id => Err(ProtocolError::new(
                format!("could not parse message type id: {}", unknown_message_id).to_string(),
            )
            .into()),
        }
    }
}

fn decode_bitfield(buf: Vec<u8>) -> Vec<bool> {
    let mut bitfield = vec![false; buf.len() * 8];
    for i in 0..buf.len() {
        let mut mask: u8 = 0b10000000;
        for j in 0..8 {
            bitfield[i * 8 + j] = (buf[i] & mask) != 0;
            mask >>= 1;
        }
    }
    bitfield
}

fn encode_bitfield(bitfield: Vec<bool>) -> Vec<u8> {
    let bitfield_bytes = (bitfield.len() / 8) + if bitfield.len() % 8 != 0 { 1 } else { 0 };
    let mut buf = vec![0; 5 + bitfield_bytes];
    let bitfield_bytes_u32: u32 = bitfield_bytes
        .try_into()
        .expect("number of bytes holding bitfield should always fit an u32");
    buf[0..4].copy_from_slice(&(1 + bitfield_bytes_u32).to_be_bytes());
    buf[4] = 5;
    for i in 0..bitfield_bytes {
        let mut bitfield_byte: u8 = 0;
        let mut mask: u8 = 0b10000000;
        for j in 0..8 {
            if bitfield.len() <= i * 8 + j {
                break;
            }
            if bitfield[i * 8 + j] {
                bitfield_byte |= mask;
            }
            mask >>= 1;
        }
        buf[5 + i] = bitfield_byte;
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::decode_bitfield;
    use super::encode_bitfield;

    #[test]
    fn decode_bitfield_test() {
        let buf = vec![0b10000001, 0b00001100];
        let bitfield = decode_bitfield(buf);
        assert_eq!(
            *bitfield,
            vec![
                true, false, false, false, false, false, false, true, // byte 1
                false, false, false, false, true, true, false, false // byte 2
            ]
        )
    }

    #[test]
    fn encode_bitfield_test_1() {
        let bitfield = vec![
            true, false, false, false, false, false, false, true, // byte 1
            false, false, false, false, true, true, // byte 2, only 6 bits
        ];
        let buf = encode_bitfield(bitfield);
        assert_eq!(
            buf,
            vec![
                0, 0, 0, 3, // len
                5, // type
                0b10000001, 0b00001100 // bitfiled bites
            ]
        );
    }

    #[test]
    fn encode_bitfield_test_2() {
        let bitfield = vec![
            true, false, false, false, false, false, false, true, // byte 1
            false, false, false, false, true, true, false, true, // byte 2
        ];
        let buf = encode_bitfield(bitfield);
        assert_eq!(
            buf,
            vec![
                0, 0, 0, 3, // len
                5, // type
                0b10000001, 0b00001101 // bitfiled bites
            ]
        );
    }

    #[test]
    fn encode_bitfield_test_3() {
        let bitfield = vec![
            false, true, // byte 1
        ];
        let buf = encode_bitfield(bitfield);
        assert_eq!(
            buf,
            vec![
                0, 0, 0, 2,          // len
                5,          // type
                0b01000000  // bitfiled bites
            ]
        );
    }
}

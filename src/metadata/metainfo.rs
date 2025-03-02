use crate::{bencoding::Value, util::pretty_info_hash};
use anyhow::{bail, Result};
use sha1::{Digest, Sha1};
use size::{Size, Style};
use std::{fmt, str};

use super::infodict::{self, MetainfoFile};

#[derive(PartialEq, Debug)]
pub struct Metainfo {
    pub announce_list: Vec<Vec<String>>,
    pub url_list: Vec<String>,
    pub nodes: Vec<String>,
    pub piece_length: u64,     // number of bytes in each piece (integer)
    pub pieces: Vec<[u8; 20]>, // 20-byte SHA1 of each piece
    pub info_hash: [u8; 20], // 20-byte SHA1 hash of the value of the info key from the Metainfo file
    pub file: MetainfoFile,
}

impl fmt::Display for Metainfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let files = self
            .get_files()
            .iter()
            .map(|f| format!("    - {} ({})", f.0, Size::from_bytes(f.1)))
            .collect::<Vec<String>>()
            .join("\n");
        write!(
            f,
            "announces: {:?}\nurl-list: {:?}\nnodes:{:?}\npiece_lenght: {}\nn. pieces: {}\ninfo_hash: {}\nfiles:\n{}",
            self.announce_list,
            self.url_list,
            self.nodes,
            Size::from_bytes(self.piece_length).format().with_style(Style::Abbreviated),
            self.pieces.len(),
            pretty_info_hash(self.info_hash),
            files
        )
    }
}

impl Metainfo {
    pub fn new(v: &Value, source: &Vec<u8>) -> Result<Self> {
        let torrent_map = match v {
            Value::Dict(m, _, _) => m,
            _ => bail!("The .torrent file is invalid: it does not contain a dict"),
        };

        // announce / announce-list
        let mut announces = Vec::new();

        match torrent_map.get(&b"announce-list".to_vec()) {
            None => {}
            Some(Value::List(announce_list)) => {
                for tier in announce_list {
                    if let Value::List(announces_in_tier) = tier {
                        let mut tier_list = Vec::new();
                        for announce_url in announces_in_tier {
                            if let Value::Str(announce_vec) = announce_url {
                                if let Ok(a) = str::from_utf8(&announce_vec) {
                                    tier_list.push(a.to_string());
                                } else {
                                    bail!("The .torrent file \"announce-list\" has an element in a tier list that is not an UTF-8 string");
                                }
                            } else {
                                bail!("The .torrent file \"announce-list\" has an element in a tier list that is not a string");
                            }
                        }
                        if tier_list.len() == 0 {
                            bail!("The .torrent file \"announce-list\" has a tier list without elements");
                        }
                        announces.push(tier_list);
                    } else {
                        bail!(
                            "The .torrent file \"announce-list\" does not contain a list of lists"
                        );
                    }
                }
            }
            Some(_) => bail!(
                "The .torrent file has a \"announce-list\" field but it does not contain a list"
            ),
        }

        if announces.len() == 0 {
            match torrent_map.get(&b"announce".to_vec()) {
                Some(Value::Str(announce_vec)) => match str::from_utf8(&announce_vec) {
                    Ok(a) => announces.push(vec![a.to_string()]),
                    _ => bail!("The .torrent file \"announce\" is not an UTF8 string"),
                },
                _ => {}
            };
        }

        let mut url_list = Vec::new();
        match torrent_map.get(&b"url-list".to_vec()) {
            None => {}
            Some(Value::List(l)) => {
                for url_value in l {
                    if let Value::Str(url_v) = url_value {
                        if let Ok(url) = str::from_utf8(&url_v) {
                            url_list.push(url.to_string());
                        } else {
                            bail!("The .torrent file \"url-list\" has an element that is not an UTF-8 string");
                        }
                    } else {
                        bail!("The .torrent file \"url-list\" has an element that is not a string");
                    }
                }
            }
            Some(Value::Str(url_v)) => {
                if let Ok(url) = str::from_utf8(&url_v) {
                    url_list.push(url.to_string());
                } else {
                    bail!("The .torrent file \"url-list\" has an element that is not an UTF-8 string");
                }
            }
            Some(_) => bail!( "The .torrent file has a \"url-list\" field but it does not contain a list or string")
        }

        let mut node_list = Vec::new();
        match torrent_map.get(&b"nodes".to_vec()) {
            None => {}
            Some(Value::List(l)) => {
                for node_host_port_list_value in l {
                    if let Value::List(node_host_port_list) = node_host_port_list_value {
                        if node_host_port_list.len() != 2 {
                            bail!("The .torrent file \"nodes\" has an element that is a list of size other than 2");
                        }
                        let node_host;
                        if let Value::Str(node_host_vec) = node_host_port_list[0].clone() {
                            if let Ok(host) = str::from_utf8(&node_host_vec) {
                                node_host = host.to_string();
                            } else {
                                bail!("The .torrent file \"nodes\" has a host that is not an UTF-8 string");
                            }
                        } else {
                            bail!("The .torrent file \"nodes\" has a host that is not a string");
                        }
                        let node_port;
                        if let Value::Int(node_port_int) = node_host_port_list[1].clone() {
                            node_port = node_port_int;
                        } else {
                            bail!("The .torrent file \"nodes\" has a port that is not a number");
                        }
                        node_list.push(format!("{node_host}:{node_port}"));
                    } else {
                        bail!("The .torrent file \"nodes\" has an element that is not a list");
                    }
                }
            }
            Some(_) => {
                bail!("The .torrent file has a \"nodes\" field but it does not contain a list")
            }
        }

        // info dict
        let (info_dict, info_hash) = match torrent_map.get(&b"info".to_vec()) {
            Some(Value::Dict(a, s, e)) => (
                a,
                Sha1::digest(&source[*s..*e]).as_slice().try_into().unwrap(),
            ),
            _ => bail!("The .torrent file does not contain a valid \"info\""),
        };

        let (piece_length, pieces, file) = infodict::get_infodict(info_dict)?;

        Ok(Metainfo {
            announce_list: announces,
            url_list,
            nodes: node_list,
            piece_length,
            pieces,
            info_hash,
            file,
        })
    }

    pub fn get_files(&self) -> Vec<(String, u64)> {
        match &self.file {
            MetainfoFile::SingleFile(m) => {
                vec![(m.name.clone(), m.length)]
            }
            MetainfoFile::MultiFile(m) => {
                let mut files = Vec::new();
                for file in &m.files {
                    files.push((file.path.join("/"), file.length))
                }
                files
            }
        }
    }
}

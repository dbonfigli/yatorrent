use crate::bencoding::Value;
use sha1::{Digest, Sha1};
use size::{Size, Style};
use std::{error::Error, fmt, str};
use Result;

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

#[derive(PartialEq, Debug)]
pub enum MetainfoFile {
    SingleFile(MetainfoSingleFile),
    MultiFile(MetainfoMultiFile),
}

#[derive(PartialEq, Debug)]
pub struct MetainfoSingleFile {
    pub name: String, // the filename. This is purely advisory. (string)
    pub length: u64,  // length of the file in bytes. (integer)
}

#[derive(PartialEq, Debug)]
pub struct MetainfoMultiFile {
    pub name: String, // the name of the directory in which to store all the files. This is purely advisory. (string)
    pub files: Vec<MultifileFile>, // a list of dictionaries, one for each file.
}

#[derive(PartialEq, Debug)]
pub struct MultifileFile {
    pub length: u64,       // length of the file in bytes. (integer)
    pub path: Vec<String>, // a list containing one or more string elements that together represent the path and filename. Each element in the list corresponds to either a directory name or (in the case of the final element) the filename. For example, a the file "dir1/dir2/file.ext" would consist of three string elements: "dir1", "dir2", and "file.ext". This is encoded as a bencoded list of strings such as l4:dir14:dir28:file.exte
}

pub fn pretty_info_hash(info_hash: [u8; 20]) -> String {
    info_hash
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join("")
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
    pub fn new(v: &Value, source: &Vec<u8>) -> Result<Self, Box<dyn Error>> {
        let torrent_map = match v {
            Value::Dict(m, _, _) => m,
            _ => {
                return Err(Box::from(
                    "The .torrent file is invalid: it does not contain a dict",
                ))
            }
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
                                    return Err(Box::from("The .torrent file \"announce-list\" has an element in a tier list that is not an UTF-8 string"));
                                }
                            } else {
                                return Err(Box::from(
                                        "The .torrent file \"announce-list\" has an element in a tier list that is not a string"),
                                    );
                            }
                        }
                        if tier_list.len() == 0 {
                            return Err(Box::from(
                                "The .torrent file \"announce-list\" has a tier list without elements"),
                            );
                        }
                        announces.push(tier_list);
                    } else {
                        return Err(Box::from(
                            "The .torrent file \"announce-list\" does not contain a list of lists",
                        ));
                    }
                }
            }
            Some(_) => {
                return Err(Box::from("The .torrent file has a \"announce-list\" field but it does not contain a list"));
            }
        }

        if announces.len() == 0 {
            match torrent_map.get(&b"announce".to_vec()) {
                Some(Value::Str(announce_vec)) => match str::from_utf8(&announce_vec) {
                    Ok(a) => announces.push(vec![a.to_string()]),
                    _ => {
                        return Err(Box::from(
                            "The .torrent file \"announce\" is not an UTF8 string",
                        ))
                    }
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
                            return Err(Box::from("The .torrent file \"url-list\" has an element that is not an UTF-8 string"));
                        }
                    } else {
                        return Err(Box::from(
                            "The .torrent file \"url-list\" has an element that is not a string",
                        ));
                    }
                }
            }
            Some(_) => {
                return Err(Box::from(
                    "The .torrent file has a \"url-list\" field but it does not contain a list",
                ));
            }
        }

        let mut node_list = Vec::new();
        match torrent_map.get(&b"nodes".to_vec()) {
            None => {}
            Some(Value::List(l)) => {
                for node_host_port_list_value in l {
                    if let Value::List(node_host_port_list) = node_host_port_list_value {
                        if node_host_port_list.len() != 2 {
                            return Err(Box::from(
                                "The .torrent file \"nodes\" has an element that is a list of size other than 2",
                            ));
                        }
                        let node_host;
                        if let Value::Str(node_host_vec) = node_host_port_list[0].clone() {
                            if let Ok(host) = str::from_utf8(&node_host_vec) {
                                node_host = host.to_string();
                            } else {
                                return Err(Box::from("The .torrent file \"nodes\" has a host that is not an UTF-8 string"));
                            }
                        } else {
                            return Err(Box::from(
                                "The .torrent file \"nodes\" has a host that is not a string",
                            ));
                        }
                        let node_port;
                        if let Value::Int(node_port_int) = node_host_port_list[1].clone() {
                            node_port = node_port_int;
                        } else {
                            return Err(Box::from(
                                "The .torrent file \"nodes\" has a port that is not a number",
                            ));
                        }
                        node_list.push(format!("{}:{}", node_host, node_port));
                    } else {
                        return Err(Box::from(
                            "The .torrent file \"nodes\" has an element that is not a list",
                        ));
                    }
                }
            }
            Some(_) => {
                return Err(Box::from(
                    "The .torrent file has a \"nodes\" field but it does not contain a list",
                ));
            }
        }

        // info dict
        let (info_dict, info_hash) = match torrent_map.get(&b"info".to_vec()) {
            Some(Value::Dict(a, s, e)) => (
                a,
                Sha1::digest(&source[*s..*e]).as_slice().try_into().unwrap(),
            ),
            _ => {
                return Err(Box::from(
                    "The .torrent file does not contain a valid \"info\"",
                ))
            }
        };

        // file / dir name
        let name_string = match info_dict.get(&b"name".to_vec()) {
            Some(Value::Str(name_vec)) => match str::from_utf8(name_vec) {
                Ok(a) => a.to_string(),
                _ => {
                    return Err(Box::from(
                        "The .torrent file \"info.name\" kv is not an UTF8 string",
                    ))
                }
            },
            _ => {
                return Err(Box::from(
                    "The .torrent file does not contain a valid \"info.name\"",
                ))
            }
        };

        // piece length
        let piece_length_i64_value = match info_dict.get(&b"piece length".to_vec()) {
            Some(Value::Int(a)) => a,
            _ => {
                return Err(Box::from(
                    "The .torrent file does not contain a valid \"info.piece length\"",
                ))
            }
        };
        if *piece_length_i64_value < 0 {
            return Err(Box::from(
                "The .torrent file \"info.piece length\" kv cannot be < 0",
            ));
        }

        // pieces
        let pieces_vec = match info_dict.get(&b"pieces".to_vec()) {
            Some(Value::Str(pieces_byte_vec)) => {
                if pieces_byte_vec.len() % 20 != 0 {
                    return Err(Box::from("The .torrent file contains \"info.pieces\" that is not a string of length divisible by 20"));
                }
                let mut pieces_vec = Vec::new();
                for p in (0..pieces_byte_vec.len()).step_by(20) {
                    let mut piece: [u8; 20] = [0; 20];
                    piece.clone_from_slice(&pieces_byte_vec[p..p + 20]);
                    pieces_vec.push(piece);
                }
                pieces_vec
            }
            _ => {
                return Err(Box::from(
                    "The .torrent file does not contain a valid \"info.pieces\"",
                ))
            }
        };

        // file / files
        let metainfo_file;
        if let Some(Value::Int(a)) = info_dict.get(&b"length".to_vec()) {
            if *a < 0 {
                return Err(Box::from(
                    "The .torrent file \"info.length\" kv cannot be < 0",
                ));
            }
            metainfo_file = MetainfoFile::SingleFile(MetainfoSingleFile {
                name: name_string,
                length: *a as u64,
            });
        } else if let Some(Value::List(files_list)) = info_dict.get(&b"files".to_vec()) {
            let mut files = Vec::new();
            for f in files_list {
                let entry =
                    match f {
                        Value::Dict(a, _, _) => a,
                        _ => return Err(Box::from(
                            "The .torrent file \"info.files\" kv has an entry that is not a dict",
                        )),
                    };

                // lenght
                let entry_length = match entry.get(&b"length".to_vec()) {
                    Some(Value::Int(a)) => a,
                    _ => return Err(Box::from("The .torrent file \"info.files\" kv has an entry that has no valid \"length\"")),
                };
                if *entry_length < 0 {
                    return Err(Box::from("The .torrent file \"info.files\" kv has an entry with \"length\" that is < 0"));
                }

                // path
                let entry_path_value_list = match entry.get(&b"path".to_vec()) {
                    Some(Value::List(a)) => a,
                    _ => return Err(Box::from(
                        "The .torrent file \"info.files\" kv has an entry that has no valid \"path\""),
                    ),
                };
                let mut entry_path_list = Vec::new();
                for e in entry_path_value_list {
                    let p = match e {
                        Value::Str(a) => match str::from_utf8(a) {
                            Ok(a) => a.to_string(),
                            _ => return Err(Box::from("The .torrent file \"info.files\" kv has an entry with \"path\" with an element that is not an UTF8 string")),
                        }
                        _ => return Err(Box::from("The .torrent file \"info.files\" kv has an entry with \"path\" with an element that is not a string")),
                    };
                    entry_path_list.push(p);
                }

                files.push(MultifileFile {
                    length: *entry_length as u64,
                    path: entry_path_list,
                })
            }
            metainfo_file = MetainfoFile::MultiFile(MetainfoMultiFile {
                name: name_string,
                files: files,
            })
        } else {
            return Err(Box::from("The .torrent file does not contain either a valid \"info.length\" or a \"info.files\""));
        }

        Ok(Metainfo {
            announce_list: announces,
            url_list: url_list,
            nodes: node_list,
            piece_length: *piece_length_i64_value as u64,
            pieces: pieces_vec,
            info_hash: info_hash,
            file: metainfo_file,
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

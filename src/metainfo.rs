use crate::bencoding::Value;
use sha1::{Digest, Sha1};
use std::str;
use Result;

#[derive(PartialEq, Debug)]
pub struct Metainfo {
    announce: String,
    piece_length: i64,
    pieces: Vec<u8>,
    info_hash: [u8; 20],
    file: MetainfoFile,
}

#[derive(PartialEq, Debug)]
pub enum MetainfoFile {
    singleFile(MetainfoSingleFile),
    multiFile(MetainfoMultiFile),
}

#[derive(PartialEq, Debug)]
pub struct MetainfoSingleFile {
    name: String,
    length: i64,
}

#[derive(PartialEq, Debug)]
pub struct MetainfoMultiFile {
    name: String,
    files: Vec<MultifileFile>,
}

#[derive(PartialEq, Debug)]
pub struct MultifileFile {
    length: i64,
    path: Vec<String>,
}

impl Metainfo {
    pub fn new(v: &Value, source: &Vec<u8>) -> Result<Self, &'static str> {
        let torrent_map;
        match v {
            Value::Dict(m, _, _) => torrent_map = m,
            _ => return Err("The .torrent file is invalid: it does not contain a dict"),
        }

        // announce
        let announce_string = match torrent_map.get(&b"announce".to_vec()) {
            Some(Value::Str(announce_vec)) => match str::from_utf8(&announce_vec) {
                Ok(a) => a.to_string(),
                _ => return Err("The .torrent file \"announce\" is not an UTF8 string"),
            },
            _ => return Err("The .torrent file does not contain a valid \"announce\""),
        };

        // info dict
        let (info_dict, info_hash) = match torrent_map.get(&b"info".to_vec()) {
            Some(Value::Dict(a, s, e)) => (
                a,
                Sha1::digest(&source[*s..*e]).as_slice().try_into().unwrap(),
            ),
            _ => return Err("The .torrent file does not contain a valid \"info\""),
        };

        // file / dir name
        let name_string = match info_dict.get(&b"name".to_vec()) {
            Some(Value::Str(name_vec)) => match str::from_utf8(name_vec) {
                Ok(a) => a.to_string(),
                _ => return Err("The .torrent file \"info.name\" kv is not an UTF8 string"),
            },
            _ => return Err("The .torrent file does not contain a valid \"info.name\""),
        };

        // piece lenght
        let piece_length_i64_value = match info_dict.get(&b"piece length".to_vec()) {
            Some(Value::Int(a)) => a,
            _ => return Err("The .torrent file does not contain a valid \"info.piece length\""),
        };
        if *piece_length_i64_value < 0 {
            return Err("The .torrent file \"info.piece length\" kv cannot be < 0");
        }

        // pieces
        let pieces_vec = match info_dict.get(&b"pieces".to_vec()) {
            Some(Value::Str(a)) => a.clone(),
            _ => return Err("The .torrent file does not contain a valid \"info.pieces\""),
        };

        // file / files
        let metainfo_file;
        if let Some(Value::Int(a)) = info_dict.get(&b"length".to_vec()) {
            metainfo_file = MetainfoFile::singleFile(MetainfoSingleFile {
                name: name_string,
                length: *a,
            });
        } else if let Some(Value::List(files_list)) = info_dict.get(&b"files".to_vec()) {
            let mut files = Vec::new();
            for f in files_list {
                let entry =
                    match f {
                        Value::Dict(a, _, _) => a,
                        _ => return Err(
                            "The .torrent file \"info.files\" kv has an entry that is not a dict",
                        ),
                    };

                // lenght
                let entry_length = match entry.get(&b"length".to_vec()) {
                    Some(Value::Int(a)) => a,
                    _ => return Err("The .torrent file \"info.files\" kv has an entry that has no valid \"length\""),
                };
                if *entry_length < 0 {
                    return Err("The .torrent file \"info.files\" kv has an entry with \"length\" that is < 0");
                }

                // path
                let entry_path_value_list = match entry.get(&b"path".to_vec()) {
                    Some(Value::List(a)) => a,
                    _ => return Err(
                        "The .torrent file \"info.files\" kv has an entry that has no valid \"path\"",
                    ),
                };
                let mut entry_path_list = Vec::new();
                for e in entry_path_value_list {
                    let p = match e {
                        Value::Str(a) => match str::from_utf8(a) {
                            Ok(a) => a.to_string(),
                            _ => return Err("The .torrent file \"info.files\" kv has an entry with \"path\" with an element that is not an UTF8 string"),
                        }
                        _ => return Err("The .torrent file \"info.files\" kv has an entry with \"path\" with an element that is not a string"),
                    };
                    entry_path_list.push(p);
                }

                files.push(MultifileFile {
                    length: *entry_length,
                    path: entry_path_list,
                })
            }
            metainfo_file = MetainfoFile::multiFile(MetainfoMultiFile {
                name: name_string,
                files: files,
            })
        } else {
            return Err("The .torrent file does not contain either a valid \"info.length\" or a \"info.files\"");
        }

        Ok(Metainfo {
            announce: announce_string,
            piece_length: *piece_length_i64_value,
            pieces: pieces_vec,
            info_hash: info_hash,
            file: metainfo_file,
        })
    }
}

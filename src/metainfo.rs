use crate::bencoding::Value;
use sha1::{Digest, Sha1};
use size::Size;
use std::{fmt, str};
use Result;

#[derive(PartialEq, Debug)]
pub struct Metainfo {
    pub announce: String,
    pub piece_length: i64,   // number of bytes in each piece (integer)
    pub pieces: Vec<u8>, // string consisting of the concatenation of all 20-byte SHA1 hash values, one per piece (byte string, i.e. not urlencoded)
    pub info_hash: [u8; 20], // urlencoded 20-byte SHA1 hash of the value of the info key from the Metainfo file. Note that the value will be a bencoded dictionary, given the definition of the info key above.
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
    pub length: i64,  // length of the file in bytes. (integer)
}

#[derive(PartialEq, Debug)]
pub struct MetainfoMultiFile {
    pub name: String, // the name of the directory in which to store all the files. This is purely advisory. (string)
    pub files: Vec<MultifileFile>, // a list of dictionaries, one for each file.
}

#[derive(PartialEq, Debug)]
pub struct MultifileFile {
    pub length: i64,       // length of the file in bytes. (integer)
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
            "announce: {}\npiece_lenght: {}\npieces: {}\ninfo_hash: {}\nfiles:\n{}",
            self.announce,
            self.piece_length,
            self.pieces.len(),
            pretty_info_hash(self.info_hash),
            files
        )
    }
}

impl Metainfo {
    pub fn new(v: &Value, source: &Vec<u8>) -> Result<Self, &'static str> {
        let torrent_map = match v {
            Value::Dict(m, _, _) => m,
            _ => return Err("The .torrent file is invalid: it does not contain a dict"),
        };

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
            metainfo_file = MetainfoFile::SingleFile(MetainfoSingleFile {
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
            metainfo_file = MetainfoFile::MultiFile(MetainfoMultiFile {
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

    pub fn get_files(&self) -> Vec<(String, i64)> {
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

use std::collections::HashMap;

use crate::{bencoding::Value, util::FileEntry};
use anyhow::{Result, bail};
use std::str;

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
    pub path: Vec<String>, // a list containing one or more string elements that together represent the path and filename. Each element in the list corresponds to either a directory name or (in the case of the final element) the filename. For example, the file "dir1/dir2/file.ext" would consist of three string elements: "dir1", "dir2", and "file.ext". This is encoded as a bencoded list of strings such as l4:dir14:dir28:file.exte
}

pub struct ParsedInfodict {
    pub piece_length: u64,
    pub piece_hashes: Vec<[u8; 20]>,
    pub metainfo_file: MetainfoFile,
}

pub fn parse_infodict(info_dict: &HashMap<Vec<u8>, Value>) -> Result<ParsedInfodict> {
    // file / dir name
    let name_string = match info_dict.get(&b"name".to_vec()) {
        Some(Value::Str(name_vec)) => match str::from_utf8(name_vec) {
            Ok(a) => a.to_string(),
            _ => bail!("The .torrent file \"info.name\" kv is not an UTF8 string"),
        },
        _ => bail!("The .torrent file does not contain a valid \"info.name\""),
    };

    // piece length
    let piece_length_i64_value = match info_dict.get(&b"piece length".to_vec()) {
        Some(Value::Int(a)) => a,
        _ => bail!("The .torrent file does not contain a valid \"info.piece length\""),
    };
    if *piece_length_i64_value <= 0 {
        bail!("The .torrent file \"info.piece length\" kv cannot be <= 0");
    }

    // pieces
    let piece_hashes = match info_dict.get(&b"pieces".to_vec()) {
        Some(Value::Str(piece_hashes_byte_vec)) => {
            if piece_hashes_byte_vec.len() % 20 != 0 {
                bail!(
                    "The .torrent file contains \"info.pieces\" that is not a string of length divisible by 20"
                );
            }
            let mut piece_hashes = Vec::new();
            for p in (0..piece_hashes_byte_vec.len()).step_by(20) {
                let mut piece_hash: [u8; 20] = [0; 20];
                piece_hash.clone_from_slice(&piece_hashes_byte_vec[p..p + 20]);
                piece_hashes.push(piece_hash);
            }
            piece_hashes
        }
        _ => bail!("The .torrent file does not contain a valid \"info.pieces\""),
    };

    // file / files
    let metainfo_file;
    if let Some(Value::Int(a)) = info_dict.get(&b"length".to_vec()) {
        if *a < 0 {
            bail!("The .torrent file \"info.length\" kv cannot be < 0");
        }
        metainfo_file = MetainfoFile::SingleFile(MetainfoSingleFile {
            name: name_string,
            length: *a as u64,
        });
    } else if let Some(Value::List(files_list)) = info_dict.get(&b"files".to_vec()) {
        let mut files = Vec::new();
        for f in files_list {
            let entry = match f {
                Value::Dict { dict: a, .. } => a,
                _ => {
                    bail!("The .torrent file \"info.files\" kv has an entry that is not a dict")
                }
            };

            // length
            let entry_length = match entry.get(&b"length".to_vec()) {
                Some(Value::Int(a)) => a,
                _ => bail!(
                    "The .torrent file \"info.files\" kv has an entry that has no valid \"length\""
                ),
            };
            if *entry_length < 0 {
                bail!(
                    "The .torrent file \"info.files\" kv has an entry with \"length\" that is < 0"
                );
            }

            // path
            let entry_path_value_list = match entry.get(&b"path".to_vec()) {
                Some(Value::List(a)) => a,
                _ => bail!(
                    "The .torrent file \"info.files\" kv has an entry that has no valid \"path\""
                ),
            };
            let mut entry_path_list = Vec::new();
            for e in entry_path_value_list {
                let p = match e {
                    Value::Str(a) => match str::from_utf8(a) {
                        Ok(a) => a.to_string(),
                        _ => bail!(
                            "The .torrent file \"info.files\" kv has an entry with \"path\" with an element that is not an UTF8 string"
                        ),
                    },
                    _ => bail!(
                        "The .torrent file \"info.files\" kv has an entry with \"path\" with an element that is not a string"
                    ),
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
            files,
        })
    } else {
        bail!(
            "The .torrent file does not contain either a valid \"info.length\" or a \"info.files\""
        );
    }

    Ok(ParsedInfodict {
        piece_length: *piece_length_i64_value as u64,
        piece_hashes,
        metainfo_file,
    })
}

pub fn get_files(metainfo_file: &MetainfoFile) -> Vec<FileEntry> {
    match metainfo_file {
        MetainfoFile::SingleFile(m) => {
            vec![FileEntry::new(m.name.clone(), m.length)]
        }
        MetainfoFile::MultiFile(m) => {
            let mut files = Vec::new();
            for file in &m.files {
                files.push(FileEntry::new(file.path.join("/"), file.length))
            }
            files
        }
    }
}

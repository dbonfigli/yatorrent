use crate::bencoding::Value;
use std::str;

#[derive(PartialEq, Debug)]
pub struct Metainfo {
    announce: String,
    piece_length: i64,
    pieces: Vec<u8>,
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
    pub fn new(v: Value) -> Result<Self, &'static str> {
        let torrent_map;
        match v {
            Value::Dict(m) => torrent_map = m,
            _ => return Result::Err("The .torrent file is invalid: it does not contain a dict"),
        }

        // announce
        let announce_opt_value = torrent_map.get(&b"announce".to_vec());
        let announce_value;
        match announce_opt_value {
            Some(a) => announce_value = a,
            _ => return Result::Err("The .torrent file does not contain an \"announce\" key"),
        }
        let announce_vec;
        match announce_value {
            Value::Str(a) => announce_vec = a,
            _ => return Result::Err("The .torrent file \"announce\" kv is not a string"),
        }
        let announce_string;
        let announce_utf8_res = str::from_utf8(&announce_vec);
        match announce_utf8_res {
            Ok(a) => announce_string = a.to_string(),
            _ => return Result::Err("The .torrent file \"announce\" is not an UTF8 string"),
        }

        // info dict
        let info_opt_value = torrent_map.get(&b"info".to_vec());
        let info_value;
        match info_opt_value {
            Some(a) => info_value = a,
            _ => return Result::Err("The .torrent file does not contain an \"info\" key"),
        }
        let info_dict;
        match info_value {
            Value::Dict(a) => info_dict = a,
            _ => return Result::Err("The .torrent file \"info\" kv is not a dict"),
        }

        // file / dir name
        let name_opt_value = info_dict.get(&b"name".to_vec());
        let name_value;
        match name_opt_value {
            Some(a) => name_value = a,
            _ => return Result::Err("The .torrent file does not contain a \"info.name\" key"),
        }
        let name_vec;
        match name_value {
            Value::Str(a) => name_vec = a,
            _ => return Result::Err("The .torrent file \"info.name\" kv is not a string"),
        }
        let name_string;
        let name_vec_ut8_res = str::from_utf8(name_vec);
        match name_vec_ut8_res {
            Ok(a) => name_string = a.to_string(),
            _ => return Result::Err("The .torrent file \"info.name\" kv is not an UTF8 string"),
        }

        // piece lenght
        let piece_length_opt_value = info_dict.get(&b"piece length".to_vec());
        let piece_length_value;
        match piece_length_opt_value {
            Some(a) => piece_length_value = a,
            _ => {
                return Result::Err(
                    "The .torrent file does not contain an \"info.piece length\" key",
                )
            }
        }
        let piece_length_i64_value;
        match piece_length_value {
            Value::Int(a) => piece_length_i64_value = a,
            _ => return Result::Err("The .torrent file \"info.piece length\" kv is not a number"),
        }
        if *piece_length_i64_value < 0 {
            return Result::Err("The .torrent file \"info.piece length\" kv cannot be < 0");
        }

        // pieces
        let pieces_opt_value = info_dict.get(&b"pieces".to_vec());
        let pieces_value;
        match pieces_opt_value {
            Some(a) => pieces_value = a,
            _ => return Result::Err("The .torrent file does not contain a \"info.pieces\" key"),
        }
        let pieces_vec;
        match pieces_value {
            Value::Str(a) => pieces_vec = a.clone(),
            _ => return Result::Err("The .torrent file \"info.pieces\" kv is not a string"),
        }

        // file / files
        let metainfo_file;
        let length_opt_value = info_dict.get(&b"length".to_vec());
        if let Some(lentgh_value) = length_opt_value {
            match lentgh_value {
                Value::Int(a) => {
                    metainfo_file = MetainfoFile::singleFile(MetainfoSingleFile {
                        name: name_string,
                        length: *a,
                    })
                }
                _ => return Result::Err(
                    "The .torrent file \"info.length\" kv has been provided but it is not a number",
                ),
            }
        } else {
            let files_value;
            let files_opt_value = info_dict.get(&b"files".to_vec());
            match files_opt_value {
                Some(a) => files_value = a,
                _ => return Result::Err("The .torrent file does not contain either a \"info.length\" or a \"info.files\" key"),
            }
            let files_list;
            match files_value {
                Value::List(a) => files_list = a,
                _ => return Result::Err("The .torrent file \"info.files\" kv is not a list"),
            }
            let mut files = Vec::new();
            for f in files_list {
                let entry;
                match f {
                    Value::Dict(a) => entry = a,
                    _ => {
                        return Result::Err(
                            "The .torrent file \"info.files\" kv has an entry that is not a dict",
                        )
                    }
                }

                // lenght
                let entry_lengh_value;
                let entry_length_opt = entry.get(&b"length".to_vec());
                match entry_length_opt {
                    Some(a) => entry_lengh_value = a,
                    _ => return Result::Err("The .torrent file \"info.files\" kv has an entry that has no \"length\" key"),
                }
                let entry_length;
                match entry_lengh_value {
                    Value::Int(a) => entry_length = a,
                    _ => return Result::Err("The .torrent file \"info.files\" kv has an entry with \"length\" key that is not a number"),
                }
                if *entry_length < 0 {
                    return Result::Err("The .torrent file \"info.files\" kv has an entry with \"length\" key that is < 0");
                }

                // path
                let entry_path_value;
                let entry_path_opt = entry.get(&b"path".to_vec());
                match entry_path_opt {
                    Some(a) => entry_path_value = a,
                    _ => return Result::Err(
                        "The .torrent file \"info.files\" kv has an entry that has no \"path\" key",
                    ),
                }
                let entry_path_value_list;
                match entry_path_value {
                    Value::List(a) => entry_path_value_list = a,
                    _ => return Result::Err("The .torrent file \"info.files\" kv has an entry with \"path\" key that is not a list"),
                }
                let mut entry_path_list = Vec::new();
                for e in entry_path_value_list {
                    let p_result;
                    match e {
                        Value::Str(a) => p_result = str::from_utf8(a),
                        _ => return Result::Err("The .torrent file \"info.files\" kv has an entry with \"path\" key with an element that is not a string"),
                    }
                    let p;
                    match p_result {
                        Ok(a) => p = a.to_string(),
                        _ => return Result::Err("The .torrent file \"info.files\" kv has an entry with \"path\" key with an element that is not an UTF8 string"),
                    }
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
        }

        Result::Ok(Metainfo {
            announce: announce_string,
            piece_length: *piece_length_i64_value,
            pieces: pieces_vec,
            file: metainfo_file,
        })
    }
}

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::{cmp, fs};

use std::fs::File;

use sha1::{Digest, Sha1};
use size::Size;

pub struct FileManager {
    file_list: Vec<(PathBuf, i64, bool)>, // name with path, size, download completed / incomplete
    piece_hashes: Vec<[u8; 20]>,          // piece identified by position in array -> hash
    piece_to_files: Vec<Vec<(PathBuf, i64, i64)>>, // piece identified by position in array -> list of files the piece belong to, with start byte and end byte within that file. A piece can span many files
    pub piece_completion_status: Vec<bool>, // piece identified by position in array -> download completed / incomplete
    file_handles: FileHandles,
    piece_length: i64,
}

struct FileHandles {
    file_handles: HashMap<PathBuf, File>,
    opened_for_write: HashSet<PathBuf>,
}

impl FileHandles {
    fn new() -> FileHandles {
        FileHandles {
            file_handles: HashMap::new(),
            opened_for_write: HashSet::new(),
        }
    }

    fn get_file(
        &mut self,
        file_path: &PathBuf,
        open_for_write: bool,
    ) -> Result<&File, Box<dyn Error>> {
        if !self.file_handles.contains_key(file_path)
            || (open_for_write && !self.opened_for_write.contains(file_path))
        {
            if open_for_write && !self.opened_for_write.contains(file_path) {
                if let Some(dir) = file_path.parent() {
                    fs::create_dir_all(dir)?;
                }
            }
            let f = File::options()
                .read(true)
                .write(open_for_write)
                .create(open_for_write)
                .open(file_path)?;
            self.file_handles.insert(file_path.clone(), f);
        }
        Ok(self.file_handles.get(&file_path.clone()).unwrap())
    }
}

impl FileManager {
    pub fn new(
        base_path: &Path,
        file_list: Vec<(String, i64)>,
        piece_length: i64,
        piece_hashes: Vec<[u8; 20]>,
    ) -> FileManager {
        // warn if files do not match pieces
        let mut total_file_size = 0;
        for (_, size) in file_list.iter() {
            total_file_size += size;
        }
        if total_file_size > piece_length * piece_hashes.len() as i64 {
            log::warn!("the total file size of all files exceed the #pieces * piece_length we have, the .torrent file could be malformed, the exceeding files will not be downloaded");
        }

        // generate file list
        let fm_file_list = file_list
            .iter()
            .map(|(file_name_path, s)| {
                (
                    Path::new(base_path).join(file_name_path).to_owned(),
                    *s,
                    false,
                )
            })
            .collect();

        // generate piece_to_files
        let mut piece_to_files = Vec::new();
        let mut current_file_index = 0;
        let mut current_position_in_file = 0;
        for piece_index in 0..piece_hashes.len() {
            let mut remaining_piece_bytes_to_allocate = piece_length;
            let mut files_spanning_piece = Vec::new();

            while remaining_piece_bytes_to_allocate > 0 {
                if current_file_index >= file_list.len() {
                    // there are no more files in the list
                    if piece_index >= piece_hashes.len() - 1 {
                        // this was the last piece, it is normal that the piece does not span the full piece_length size for the last file
                        break;
                    } else {
                        panic!("there are no more files, but there are more pieces still to be matched to files, it seem piece_length * #pieces > sum of all the file sizes, this should never happen, the .torrent file is malformed")
                    }
                }

                let (file_name, file_size) = &file_list[current_file_index];
                let remaining_bytes_in_file = file_size - current_position_in_file;

                let piece_bytes_fitting_in_file =
                    cmp::min(remaining_bytes_in_file, remaining_piece_bytes_to_allocate);

                let file_name_path = Path::new(file_name);
                if file_name_path.is_absolute() {
                    panic!("the torrent file contained a file with absolute path, this is not acceptable")
                }
                let path = Path::new(base_path).join(file_name_path);

                files_spanning_piece.push((
                    path,
                    current_position_in_file,
                    current_position_in_file + piece_bytes_fitting_in_file,
                ));
                remaining_piece_bytes_to_allocate -= piece_bytes_fitting_in_file;
                current_position_in_file += piece_bytes_fitting_in_file;
                if current_position_in_file >= *file_size {
                    current_position_in_file = 0;
                    current_file_index += 1;
                }
            }

            piece_to_files.push(files_spanning_piece);
        }

        let piece_completion_status = vec![false; piece_hashes.len()];

        FileManager {
            file_list: fm_file_list,
            piece_hashes,
            piece_to_files,
            piece_completion_status,
            file_handles: FileHandles::new(),
            piece_length,
        }
    }

    pub fn refresh_completed_pieces(&mut self) {
        log::info!("checking pieces already downloaded...");
        for idx in 0..self.piece_hashes.len() {
            // print progress
            if idx % (self.piece_to_files.len() / 10) == 0 {
                log::info!(
                    "{}%...",
                    f64::round((idx as f64 * 100.0) / self.piece_to_files.len() as f64)
                );
            }
            match self.read_piece_block_with_check(idx, 0, self.piece_length, false) {
                Err(_) => {
                    self.piece_completion_status[idx] = false;
                }
                Ok(buf) => {
                    let piece_sha: [u8; 20] = Sha1::digest(&buf).as_slice().try_into().unwrap();
                    self.piece_completion_status[idx] = self.piece_hashes[idx] == piece_sha;
                }
            }
        }

        let total_completed =
            self.piece_completion_status
                .iter()
                .fold(0, |acc, v| if *v { acc + 1 } else { acc });
        log::info!(
            "checking pieces already downloaded completed: {} out of {} ({}%) pieces already completed",
            total_completed,
            self.piece_completion_status.len(),
            total_completed * 100 / self.piece_completion_status.len()
        );
    }

    // this depends on an up-to-date piece_completion_status
    pub fn refresh_completed_files(&mut self) {
        for i in 0..self.file_list.len() {
            self.file_list[i].2 = true;
        }

        let mut cur_file_idx = 0;
        for (idx, file_vec) in self.piece_to_files.iter().enumerate() {
            for (piece_fragment_file_path, _, _) in file_vec.iter() {
                if self.file_list[cur_file_idx].0 != *piece_fragment_file_path {
                    cur_file_idx += 1;
                }
                self.file_list[cur_file_idx].2 =
                    self.file_list[cur_file_idx].2 & self.piece_completion_status[idx];
            }
        }
    }

    pub fn log_file_completion_stats(&self) {
        let total_completed = self
            .file_list
            .iter()
            .fold(0, |acc, v| if v.2 { acc + 1 } else { acc });
        log::info!(
            "files completed: {} out of {} ({}%)",
            total_completed,
            self.file_list.len(),
            total_completed * 100 / self.file_list.len()
        );
        log::info!("files status:");
        for (file_path, size, status) in self.file_list.iter() {
            log::info!(
                "  - {:#?} ({}): {}",
                file_path,
                Size::from_bytes(*size),
                if *status { "completed" } else { "incomplete" }
            );
        }
    }

    fn read_piece_block(
        &mut self,
        piece_idx: usize,
        block_begin: i64,
        block_length: i64,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        return self.read_piece_block_with_check(piece_idx, block_begin, block_length, true);
    }

    fn read_piece_block_with_check(
        &mut self,
        piece_idx: usize,
        block_begin: i64,
        block_length: i64,
        check_if_have_piece: bool,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        if self.piece_length < block_begin + block_length {
            return Err(Box::from(format!(
                "requested to read piece idx {} out of range: block_begin {} + block_length {} > piece_length {}",
                piece_idx,
                block_begin, block_length, self.piece_length
            )));
        }
        if piece_idx >= self.piece_to_files.len() {
            return Err(Box::from(format!(
                "requested to read piece idx {} that is not in range (total pieces: {})",
                piece_idx,
                self.piece_to_files.len()
            )));
        }
        if check_if_have_piece && !self.piece_completion_status[piece_idx] {
            return Err(Box::from(format!(
                "requested to read piece idx {} that we don't have",
                piece_idx
            )));
        }
        let mut block_buf: Vec<u8> = Vec::new();
        let mut current_piece_offset = 0;
        let mut block_bytes_still_to_read = block_length;
        for (file_path, start, end) in self.piece_to_files[piece_idx].iter() {
            let mut file_offset = *start;
            if current_piece_offset != block_begin {
                let piece_fragment_size_in_file = end - start;
                if block_begin - current_piece_offset - piece_fragment_size_in_file > 0 {
                    // the current chunk of data in the file is not enough to reach the begin of the block we want to read
                    // move forward to the next file
                    current_piece_offset += piece_fragment_size_in_file;
                    continue;
                } else {
                    file_offset = start + (block_begin - current_piece_offset);
                    current_piece_offset = block_begin;
                }
            }

            let bytes_to_read;
            if block_bytes_still_to_read == 0 {
                break;
            } else if end - file_offset > block_bytes_still_to_read {
                bytes_to_read = block_bytes_still_to_read;
                block_bytes_still_to_read = 0;
            } else {
                bytes_to_read = end - file_offset;
                block_bytes_still_to_read -= end - file_offset;
            }

            let mut opened_file = self.file_handles.get_file(file_path, false)?;
            opened_file.seek(SeekFrom::Start(file_offset as u64))?;
            let mut file_buf: Vec<u8> = vec![0; bytes_to_read.try_into().unwrap()];
            opened_file.read_exact(&mut file_buf)?;
            block_buf.append(&mut file_buf);
        }

        Ok(block_buf)
    }

    pub fn write_piece(&mut self, piece_idx: usize, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        if self.piece_completion_status[piece_idx] {
            log::debug!(
                "we already have the piece {}, will avoid to write it again",
                piece_idx
            );
            return Ok(());
        }
        let piece_sha: [u8; 20] = Sha1::digest(&data).as_slice().try_into().unwrap();
        if piece_sha != self.piece_hashes[piece_idx] {
            return Err(Box::from(
              format!("the sha of the data we want to write for piece {} do not match the sha we expect, write aborted", piece_idx)));
        }

        let mut written: i64 = 0;
        for (file_path, start, end) in self.piece_to_files[piece_idx].iter() {
            let mut opened_file = self.file_handles.get_file(file_path, true)?;
            opened_file.seek(SeekFrom::Start(*start as u64))?;
            opened_file.write_all(&data[written as usize..(end - start) as usize])?;
            written += end - start;
        }

        self.piece_completion_status[piece_idx] = true;
        self.refresh_completed_files(); //todo: optimize this
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::FileManager;

    #[test]
    fn test_pieces_to_files_1() {
        let file_list = vec![
            ("f1".to_string(), 5),
            ("f2".to_string(), 20),
            ("f3".to_string(), 5),
        ];
        let pieces = vec![
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
        ];
        let piece_length = 10;

        let res = FileManager::new(Path::new("relative/"), file_list, piece_length, pieces);
        assert_eq!(
            res.piece_to_files,
            vec![
                vec![
                    (PathBuf::from("relative/f1"), 0, 5),
                    (PathBuf::from("relative/f2"), 0, 5)
                ],
                vec![(PathBuf::from("relative/f2"), 5, 15)],
                vec![
                    (PathBuf::from("relative/f2"), 15, 20),
                    (PathBuf::from("relative/f3"), 0, 5)
                ]
            ]
        );
    }

    #[test]
    fn test_pieces_to_files_2() {
        let file_list = vec![("f1".to_string(), 5)];
        let pieces = vec![b"aaaaaaaaaaaaaaaaaaaa".to_owned()];
        let piece_length = 5;

        let res = FileManager::new(Path::new("/absolute/"), file_list, piece_length, pieces);
        assert_eq!(
            res.piece_to_files,
            vec![vec![(PathBuf::from("/absolute/f1"), 0, 5)]]
        );
    }

    #[test]
    fn test_pieces_to_files_3() {
        let file_list = vec![("f1".to_string(), 5)];
        let pieces = vec![b"aaaaaaaaaaaaaaaaaaaa".to_owned()];
        let piece_length = 6;

        let res = FileManager::new(Path::new("hello/moto"), file_list, piece_length, pieces);
        assert_eq!(
            res.piece_to_files,
            vec![vec![(PathBuf::from("hello/moto/f1"), 0, 5)]]
        );
    }

    #[test]
    fn test_pieces_to_files_4() {
        let file_list = vec![
            ("f1".to_string(), 10),
            ("f2".to_string(), 10),
            ("f3".to_string(), 5),
            ("f4".to_string(), 3),
            ("f5".to_string(), 3),
        ];
        let pieces = vec![
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
        ];
        let piece_length = 10;

        let res = FileManager::new(Path::new("./"), file_list, piece_length, pieces);
        assert_eq!(
            res.piece_to_files,
            vec![
                vec![(PathBuf::from("./f1"), 0, 10)],
                vec![(PathBuf::from("./f2"), 0, 10)],
                vec![
                    (PathBuf::from("./f3"), 0, 5),
                    (PathBuf::from("./f4"), 0, 3),
                    (PathBuf::from("./f5"), 0, 2),
                ]
            ]
        );
    }

    #[test]
    fn test_refresh_completed_files_1() {
        let file_list = vec![
            ("f1".to_string(), 10),
            ("f2".to_string(), 10),
            ("f3".to_string(), 5),
            ("f4".to_string(), 3),
            ("f5".to_string(), 3),
        ];
        let pieces = vec![
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
        ];
        let piece_length = 10;

        let mut res = FileManager::new(Path::new("./"), file_list, piece_length, pieces);
        res.piece_completion_status = vec![false, true, false];
        res.refresh_completed_files();
        assert_eq!(
            res.file_list,
            vec![
                (std::path::PathBuf::from("./f1"), 10, false),
                (std::path::PathBuf::from("./f2"), 10, true),
                (std::path::PathBuf::from("./f3"), 5, false),
                (std::path::PathBuf::from("./f4"), 3, false),
                (std::path::PathBuf::from("./f5"), 3, false),
            ]
        )
    }

    #[test]
    fn test_refresh_completed_files_2() {
        let file_list = vec![
            ("f1".to_string(), 10),
            ("f2".to_string(), 10),
            ("f3".to_string(), 5),
            ("f4".to_string(), 3),
            ("f5".to_string(), 3),
        ];
        let pieces = vec![
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
        ];
        let piece_length = 10;

        let mut res = FileManager::new(Path::new("./"), file_list, piece_length, pieces);
        res.piece_completion_status = vec![true, false, true];
        res.refresh_completed_files();
        assert_eq!(
            res.file_list,
            vec![
                (std::path::PathBuf::from("./f1"), 10, true),
                (std::path::PathBuf::from("./f2"), 10, false),
                (std::path::PathBuf::from("./f3"), 5, true),
                (std::path::PathBuf::from("./f4"), 3, true),
                (std::path::PathBuf::from("./f5"), 3, true),
            ]
        )
    }

    #[test]
    fn test_refresh_completed_files_3() {
        let file_list = vec![
            ("f1".to_string(), 5),
            ("f2".to_string(), 20),
            ("f3".to_string(), 5),
        ];
        let pieces = vec![
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
        ];
        let piece_length = 10;

        let mut res = FileManager::new(Path::new("relative/"), file_list, piece_length, pieces);
        res.piece_completion_status = vec![true, true, true];
        res.refresh_completed_files();
        assert_eq!(
            res.file_list,
            vec![
                (std::path::PathBuf::from("relative/f1"), 5, true),
                (std::path::PathBuf::from("relative/f2"), 20, true),
                (std::path::PathBuf::from("relative/f3"), 5, true),
            ]
        );
    }
}

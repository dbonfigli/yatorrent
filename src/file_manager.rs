use std::cmp;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use std::fs::File;

use sha1::{Digest, Sha1};
use size::Size;

pub struct FileManager {
    file_list: Vec<(PathBuf, i64, bool)>, // name with path, size, download completed / incomplete
    piece_hashes: Vec<[u8; 20]>,          // piece identified by position in array -> hash
    piece_to_files: Vec<Vec<(PathBuf, i64, i64)>>, // piece identified by position in array -> list of files the piece belong to, with start byte and end byte within that file. A piece can span many files
    pub piece_completion_status: Vec<bool>, // piece identified by position in array -> download completed / incomplete
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
            log::warn!("the total file size of all files exceed the #pieces * piece_length we have, this is strange, the exceeded files will not be downloaded");
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
                        panic!("there are no more files, but there are more pieces still to be matched to files, it seem piece_length * #pieces > sum of all the file sizes, this should never happen")
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
        }
    }

    pub fn refresh_completed_pieces(&mut self) {
        log::info!("checking pieces already downloaded...");

        let mut cur_file_path = Path::new("").to_path_buf();
        let mut opened_cur_file = Err(ErrorKind::InvalidData.into());
        let mut could_not_read_piece = false;

        for (idx, file_vec) in self.piece_to_files.iter().enumerate() {
            if idx % (self.piece_to_files.len() / 10) == 0 {
                log::info!(
                    "{}%...",
                    f64::round((idx as f64 * 100.0) / self.piece_to_files.len() as f64)
                );
            }

            self.piece_completion_status[idx] = false;

            // read the data of the piece from the files
            let mut piece_data: Vec<u8> = Vec::new();
            for (piece_fragment_file_path, start, end) in file_vec {
                // forward to a new file if the current piece fragment refers to a different file
                if *piece_fragment_file_path != cur_file_path {
                    cur_file_path = piece_fragment_file_path.clone();
                    opened_cur_file = File::open(cur_file_path.clone());
                    could_not_read_piece = false;
                } else if could_not_read_piece {
                    // if the file is the same as the previous one, and we got an error before, there is no point on repeating the read for this new piece fragment
                    continue;
                }

                match opened_cur_file {
                    Err(ref e) => {
                        // log::error!(
                        //     "error opening file, path {:#?}: {}",
                        //     piece_fragment_file_path,
                        //     e
                        // );
                        could_not_read_piece = true;
                        break;
                    }
                    Ok(ref mut f) => {
                        if let Err(_) = f.seek(SeekFrom::Start(*start as u64)) {
                            //log::error!("error seeking file");
                            could_not_read_piece = true;
                            break;
                        }

                        let mut buffer: Vec<u8> = vec![0; (end - start).try_into().unwrap()];
                        if let Err(_) = f.read_exact(&mut buffer) {
                            //log::error!("error reading file");
                            could_not_read_piece = true;
                            break;
                        }

                        piece_data.append(&mut buffer);
                    }
                }
            }

            if could_not_read_piece {
                continue;
            }

            let piece_sha: [u8; 20] = Sha1::digest(&piece_data).as_slice().try_into().unwrap();
            if self.piece_hashes[idx] == piece_sha {
                self.piece_completion_status[idx] = true;
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

    // this must be run only if piece_completion_status is refreshed
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
}

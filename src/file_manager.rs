use std::cmp;
use std::path::{Path, PathBuf};

pub struct FileManager {
    file_list: Vec<(String, i64)>, // name with path, size
    piece_length: i64,
    piece_hashes: Vec<[u8; 20]>, // list of piece hashes
    piece_idx_to_files: Vec<Vec<(PathBuf, i64, i64)>>, // piece (identified by position in `pieces`) -> list fo files the piece belong to, with start byte and end byte within that file. A piece can span many files
}

impl FileManager {
    pub fn new(
        base_path: String,
        file_list: Vec<(String, i64)>,
        piece_length: i64,
        piece_hashes: Vec<[u8; 20]>,
    ) -> FileManager {
        let mut piece_dix_to_files = Vec::new();
        let mut current_file_index = 0;
        let mut current_position_in_file = 0;

        let mut total_file_size = 0;
        for (_, size) in file_list.iter() {
            total_file_size += size;
        }
        if (total_file_size > piece_length * piece_hashes.len() as i64) {
            log::warn!("the total file size of all files exceed the #pieces * piece_lenght we have, this is strange, the exceeded files will not be downloaded");
        }

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
                let path = Path::new(&base_path).join(file_name_path);

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

            piece_dix_to_files.push(files_spanning_piece);
        }

        FileManager {
            file_list,
            piece_length,
            piece_hashes,
            piece_idx_to_files: piece_dix_to_files,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

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

        let res = FileManager::new("relative/".to_string(), file_list, piece_length, pieces);
        assert_eq!(
            res.piece_idx_to_files,
            vec![
                vec![(PathBuf::from("relative/f1"), 0, 5), (PathBuf::from("relative/f2"), 0, 5)],
                vec![(PathBuf::from("relative/f2"), 5, 15)],
                vec![(PathBuf::from("relative/f2"), 15, 20), (PathBuf::from("relative/f3"), 0, 5)]
            ]
        );
    }

    #[test]
    fn test_pieces_to_files_2() {
        let file_list = vec![("f1".to_string(), 5)];
        let pieces = vec![b"aaaaaaaaaaaaaaaaaaaa".to_owned()];
        let piece_length = 5;

        let res = FileManager::new("/absolute/".to_string(), file_list, piece_length, pieces);
        assert_eq!(
            res.piece_idx_to_files,
            vec![vec![(PathBuf::from("/absolute/f1"), 0, 5)]]
        );
    }

    #[test]
    fn test_pieces_to_files_3() {
        let file_list = vec![("f1".to_string(), 5)];
        let pieces = vec![b"aaaaaaaaaaaaaaaaaaaa".to_owned()];
        let piece_length = 6;

        let res = FileManager::new("hello/moto".to_string(), file_list, piece_length, pieces);
        assert_eq!(
            res.piece_idx_to_files,
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

        let res = FileManager::new("./".to_string(), file_list, piece_length, pieces);
        assert_eq!(
            res.piece_idx_to_files,
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

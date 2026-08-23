use std::{
    cmp,
    path::{Path, PathBuf},
};

use crate::util::FileEntry;
use anyhow::{Result, bail};

// A piece can span many files.
// The following is the list of files (file paths, start byte as offset of the piece, end byte as offset of the piece) a piece belong to, ordered.
type FilePathsForPiece = Vec<(PathBuf, u64, u64)>;
type InternalFileId = usize; // internal id used instead of directly using paths to save on memory
type FileIdsForPiece = Vec<(InternalFileId, u64, u64)>; // same as FilePathsForPiece but with internal file ids

pub struct PiecesToFilePathsMapper {
    piece_id_to_file_info: Vec<FileIdsForPiece>, // piece identified by position in the array -> FileIdsForPiece
    file_id_to_path: Vec<PathBuf>,               // position in vec is the file id -> path
}

impl PiecesToFilePathsMapper {
    pub fn new(
        base_path: &Path,
        total_pieces: usize,
        piece_length: u64,
        file_list: &Vec<FileEntry>,
    ) -> Result<Self> {
        let mut pieces_to_file_paths_mapper = PiecesToFilePathsMapper {
            piece_id_to_file_info: Vec::with_capacity(total_pieces),
            file_id_to_path: vec![PathBuf::new(); file_list.len()],
        };

        let mut current_file_index = 0;
        let mut current_position_in_file = 0;
        for piece_index in 0..total_pieces {
            let mut remaining_piece_bytes_to_allocate = piece_length;
            let mut files_spanning_piece = Vec::new();

            while remaining_piece_bytes_to_allocate > 0 {
                if current_file_index >= file_list.len() {
                    // there are no more files in the list
                    if piece_index >= total_pieces - 1 {
                        // this was the last piece, it is normal that the piece does not span the full piece_length size for the last file
                        break;
                    } else {
                        // with the validation on the caller of this function, this is effectively impossible
                        bail!(
                            "there are no more files, but there are more pieces still to be matched to files, it seem piece_length * #pieces > sum of all the file sizes, this should never happen, the .torrent file is malformed"
                        )
                    }
                }

                let FileEntry {
                    path: file_name,
                    size: file_size,
                } = &file_list[current_file_index];

                if *file_size == 0 {
                    // torrent allow zero byte files, in such cases, move on
                    current_file_index += 1;
                    continue;
                }

                let remaining_bytes_in_file = file_size - current_position_in_file;

                let piece_bytes_fitting_in_file =
                    cmp::min(remaining_bytes_in_file, remaining_piece_bytes_to_allocate);

                let path = base_path.join(file_name);
                pieces_to_file_paths_mapper.file_id_to_path[current_file_index] = path;

                files_spanning_piece.push((
                    current_file_index,
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

            pieces_to_file_paths_mapper
                .piece_id_to_file_info
                .push(files_spanning_piece);
        }

        Ok(pieces_to_file_paths_mapper)
    }

    pub fn get(&self, piece_id: usize) -> FilePathsForPiece {
        self.piece_id_to_file_info[piece_id]
            .iter()
            .map(|(file_id, start, end)| (self.file_id_to_path[*file_id].clone(), *start, *end))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        persistence::file_manager::pieces_to_file_paths_mapper::PiecesToFilePathsMapper,
        util::FileEntry,
    };
    use std::path::{Path, PathBuf};

    #[test]
    fn generate_pieces_to_file_paths_mapper_1() {
        let file_list = vec![
            FileEntry::new("f1".to_string(), 5),
            FileEntry::new("f2".to_string(), 20),
            FileEntry::new("f3".to_string(), 5),
        ];
        let pieces = vec![
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
        ];
        let piece_length = 10;

        let pieces_to_file_paths_mapper = PiecesToFilePathsMapper::new(
            Path::new("relative/"),
            pieces.len(),
            piece_length,
            &file_list,
        )
        .unwrap();

        assert_eq!(
            pieces_to_file_paths_mapper.file_id_to_path[0],
            PathBuf::from("relative/f1")
        );
        assert_eq!(
            pieces_to_file_paths_mapper.file_id_to_path[1],
            PathBuf::from("relative/f2")
        );
        assert_eq!(
            pieces_to_file_paths_mapper.file_id_to_path[2],
            PathBuf::from("relative/f3")
        );

        assert_eq!(
            pieces_to_file_paths_mapper.piece_id_to_file_info,
            vec![
                vec![(0, 0, 5), (1, 0, 5)],
                vec![(1, 5, 15)],
                vec![(1, 15, 20), (2, 0, 5)]
            ]
        );
    }

    #[test]
    fn generate_pieces_to_file_paths_mapper_2() {
        let file_list = vec![FileEntry::new("f1".to_string(), 5)];
        let pieces = vec![b"aaaaaaaaaaaaaaaaaaaa".to_owned()];
        let piece_length = 5;

        let pieces_to_file_paths_mapper = PiecesToFilePathsMapper::new(
            Path::new("/absolute/"),
            pieces.len(),
            piece_length,
            &file_list,
        )
        .unwrap();

        assert_eq!(
            pieces_to_file_paths_mapper.file_id_to_path[0],
            PathBuf::from("/absolute/f1")
        );

        assert_eq!(
            pieces_to_file_paths_mapper.piece_id_to_file_info,
            vec![vec![(0, 0, 5)]]
        );
    }

    #[test]
    fn generate_pieces_to_file_paths_mapper_3() {
        let file_list = vec![FileEntry::new("f1".to_string(), 5)];
        let pieces = vec![b"aaaaaaaaaaaaaaaaaaaa".to_owned()];
        let piece_length = 6;

        let pieces_to_file_paths_mapper = PiecesToFilePathsMapper::new(
            Path::new("hello/moto"),
            pieces.len(),
            piece_length,
            &file_list,
        )
        .unwrap();

        assert_eq!(
            pieces_to_file_paths_mapper.file_id_to_path[0],
            PathBuf::from("hello/moto/f1")
        );

        assert_eq!(
            pieces_to_file_paths_mapper.piece_id_to_file_info,
            vec![vec![(0, 0, 5)]]
        );
    }

    #[test]
    fn generate_pieces_to_file_paths_mapper_4() {
        let file_list = vec![
            FileEntry::new("f1".to_string(), 10),
            FileEntry::new("f2".to_string(), 10),
            FileEntry::new("f3zero".to_string(), 0),
            FileEntry::new("f3".to_string(), 5),
            FileEntry::new("f4".to_string(), 3),
            FileEntry::new("f5".to_string(), 3),
        ];
        let pieces = vec![
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
            b"aaaaaaaaaaaaaaaaaaaa".to_owned(),
        ];
        let piece_length = 10;

        let pieces_to_file_paths_mapper =
            PiecesToFilePathsMapper::new(Path::new("./"), pieces.len(), piece_length, &file_list)
                .unwrap();

        assert_eq!(
            pieces_to_file_paths_mapper.file_id_to_path[0],
            PathBuf::from("./f1")
        );
        assert_eq!(
            pieces_to_file_paths_mapper.file_id_to_path[1],
            PathBuf::from("./f2")
        );
        assert_eq!(
            pieces_to_file_paths_mapper.file_id_to_path[3],
            PathBuf::from("./f3")
        );
        assert_eq!(
            pieces_to_file_paths_mapper.file_id_to_path[4],
            PathBuf::from("./f4")
        );
        assert_eq!(
            pieces_to_file_paths_mapper.file_id_to_path[5],
            PathBuf::from("./f5")
        );

        assert_eq!(
            pieces_to_file_paths_mapper.piece_id_to_file_info,
            vec![
                vec![(0, 0, 10)],
                vec![(1, 0, 10)],
                vec![(3, 0, 5), (4, 0, 3), (5, 0, 2),]
            ]
        );
    }
}

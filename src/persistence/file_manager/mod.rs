use anyhow::{Result, anyhow, bail};
use sha1::{Digest, Sha1};
use size::Size;
use std::fs::File;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::{cmp, fs};
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, UnboundedSender};

use crate::persistence::file_manager::file_handles::ReadFileHandles;
use crate::persistence::file_manager::pieces_to_file_paths_mapper::PiecesToFilePathsMapper;
use crate::persistence::file_manager::reads::{read_data, reads_loop};
use crate::persistence::file_manager::writes::writes_loop;
use crate::persistence::torrent_data_status::TorrentDataStatus;
use crate::util::FileEntry;

mod file_handles;
mod pieces_to_file_paths_mapper;
mod reads;
mod writes;

pub use writes::{
    ShaCheckReadError, ShaCorruptedError, WritePieceBlockRequest, WritePieceBlockResponse,
};

pub use reads::{ReadPieceBlockRequest, ReadPieceBlockResponse};

#[derive(Error, Debug)]
#[error("refusing to access torrent data through symlink {path}")]
pub struct SymlinkPathError {
    path: PathBuf,
}

#[derive(Clone)]
struct PieceSizer {
    total_pieces: usize,
    normal_piece_length: u64,
    last_piece_length: u64,
}

impl PieceSizer {
    fn piece_length(&self, piece_idx: usize) -> u64 {
        if piece_idx == self.total_pieces - 1 {
            self.last_piece_length
        } else {
            self.normal_piece_length
        }
    }

    fn total_pieces(&self) -> usize {
        self.total_pieces
    }
}

type PieceCompletionStatus = Vec<AtomicBool>; // piece identified by position in array -> completed / not completed
type PieceHashes = Vec<[u8; 20]>; // piece identified by position in array -> hash

pub fn start_file_manager(
    base_path: &Path,
    file_list: Vec<FileEntry>,
    normal_piece_length: u64,
    piece_hashes: PieceHashes,
    read_requests_rx: Receiver<ReadPieceBlockRequest>,
    read_responses_tx: UnboundedSender<ReadPieceBlockResponse>,
    write_requests_rx: Receiver<WritePieceBlockRequest>,
    write_responses_tx: UnboundedSender<WritePieceBlockResponse>,
) -> Result<TorrentDataStatus> {
    let mut total_file_size = 0u64;
    for FileEntry { size, .. } in file_list.iter() {
        total_file_size = total_file_size
            .checked_add(*size)
            .ok_or_else(|| anyhow!("total torrent file size overflows u64"))?;
    }
    let total_pieces = piece_hashes.len();
    if total_pieces == 0 {
        bail!("the torrent metadata does not contain any piece hashes");
    }
    let total_pieces_u64 = u64::try_from(total_pieces)
        .map_err(|_| anyhow!("number of torrent pieces does not fit u64"))?;
    let maximum_file_size = normal_piece_length
        .checked_mul(total_pieces_u64)
        .ok_or_else(|| anyhow!("torrent piece length times piece count overflows u64"))?;
    if total_file_size > maximum_file_size {
        bail!(
            "the total file size of all files exceed the #pieces * piece_length we have, the .torrent file / metedata could be malformed"
        );
    }
    let minimum_file_size = normal_piece_length
        .checked_mul(total_pieces_u64 - 1)
        .ok_or_else(|| anyhow!("torrent piece length times piece count overflows u64"))?;
    if total_file_size <= minimum_file_size {
        bail!(
            "the total file size of all files does not cover all the declared pieces and piece_length we have, the .torrent file / metedata could be malformed"
        );
    }

    validate_file_paths(base_path, &file_list)?;
    create_zero_length_files(base_path, &file_list)?;

    let pieces_to_file_paths_mapper = Arc::new(PiecesToFilePathsMapper::new(
        base_path,
        total_pieces,
        normal_piece_length,
        &file_list,
    )?);

    let mut last_piece_length = 0;
    for (_, start, end) in pieces_to_file_paths_mapper.get(total_pieces - 1).iter() {
        last_piece_length += end - start;
    }
    let piece_sizer = PieceSizer {
        total_pieces,
        normal_piece_length,
        last_piece_length,
    };

    let mut file_handles = ReadFileHandles::new(pieces_to_file_paths_mapper.clone());
    let piece_completion_status =
        refresh_completed_pieces(&piece_hashes, &piece_sizer, &mut file_handles);

    log_file_completion_stats(
        base_path,
        &file_list,
        pieces_to_file_paths_mapper.clone(),
        &piece_completion_status,
    );

    let shared_piece_completion_status: Arc<PieceCompletionStatus> = Arc::new(
        piece_completion_status
            .iter()
            .map(|completed| AtomicBool::new(*completed))
            .collect(),
    );
    let last_piece_length = piece_sizer.last_piece_length;

    let piece_completion_status_for_reads = shared_piece_completion_status.clone();
    let piece_sizer_for_reads = piece_sizer.clone();
    tokio::spawn(async move {
        reads_loop(
            read_requests_rx,
            read_responses_tx,
            piece_completion_status_for_reads,
            &piece_sizer_for_reads,
            &mut file_handles,
        )
        .await;
    });

    // write request loop
    tokio::spawn(async move {
        writes_loop(
            pieces_to_file_paths_mapper,
            &piece_hashes,
            write_requests_rx,
            write_responses_tx,
            shared_piece_completion_status,
            &piece_sizer,
        )
        .await;
    });

    Ok(TorrentDataStatus::new(
        piece_completion_status,
        normal_piece_length,
        last_piece_length,
    ))
}

fn refresh_completed_pieces(
    piece_hashes: &PieceHashes,
    piece_sizer: &PieceSizer,
    file_handles: &mut ReadFileHandles,
) -> Vec<bool> {
    log::info!("checking pieces already downloaded...");

    let mut piece_completion_status = vec![false; piece_hashes.len()];
    let mut total_completed = 0;
    for idx in 0..piece_sizer.total_pieces() {
        // print progress
        if idx % (cmp::max(10, piece_sizer.total_pieces()) / 10) == 0 {
            log::info!(
                "{:>3}%...",
                f64::round((idx as f64 * 100.0) / piece_sizer.total_pieces() as f64)
            );
        }

        match file_handles.get_files_for_piece(idx) {
            Err(_) => {
                piece_completion_status[idx] = false;
            }
            Ok(file_handles_for_piece) => {
                match read_data(file_handles_for_piece, 0, piece_sizer.piece_length(idx)) {
                    Err(_) => {
                        piece_completion_status[idx] = false;
                    }
                    Ok(buf) => {
                        let piece_sha: [u8; 20] = Sha1::digest(buf).into();
                        let sha_ok = piece_hashes[idx] == piece_sha;
                        piece_completion_status[idx] = sha_ok;
                        if sha_ok {
                            total_completed += 1;
                        }
                    }
                }
            }
        };
    }

    log::info!(
        "checking pieces already downloaded completed: {total_completed} out of {} ({}%) pieces already completed",
        piece_sizer.total_pieces(),
        total_completed * 100 / piece_sizer.total_pieces()
    );

    piece_completion_status
}

fn get_file_list_with_completion_status(
    base_path: &Path,
    file_list: &Vec<FileEntry>,
    pieces_to_file_paths_mapper: Arc<PiecesToFilePathsMapper>,
    piece_completion_status: &Vec<bool>,
) -> Vec<(PathBuf, u64, bool)> {
    let mut file_list_with_completion_status: Vec<(PathBuf, u64, bool)> = file_list
        .iter()
        .map(|file_entry| {
            (
                Path::new(base_path).join(file_entry.path.clone()),
                file_entry.size,
                true,
            )
        })
        .collect();

    let mut cur_file_idx = 0;
    for idx in 0..piece_completion_status.len() {
        let file_paths_for_piece = pieces_to_file_paths_mapper.get(idx);
        for (piece_fragment_file_path, _, _) in file_paths_for_piece.iter() {
            if file_list_with_completion_status[cur_file_idx].0 != *piece_fragment_file_path {
                cur_file_idx += 1;
            }
            file_list_with_completion_status[cur_file_idx].2 &= piece_completion_status[idx];
        }
    }

    file_list_with_completion_status
}

fn log_file_completion_stats(
    base_path: &Path,
    file_list: &Vec<FileEntry>,
    pieces_to_file_paths_mapper: Arc<PiecesToFilePathsMapper>,
    piece_completion_status: &Vec<bool>,
) {
    let file_list_with_completion_status = get_file_list_with_completion_status(
        base_path,
        file_list,
        pieces_to_file_paths_mapper,
        piece_completion_status,
    );

    let total_completed = file_list_with_completion_status
        .iter()
        .fold(0, |acc, v| if v.2 { acc + 1 } else { acc });
    log::info!(
        "files completed: {total_completed} out of {} ({}%)",
        file_list.len(),
        total_completed * 100 / file_list.len()
    );
    log::info!("files status:");
    for (file_path, size, status) in file_list_with_completion_status.iter() {
        log::info!(
            "  - {file_path:#?} ({}): {}",
            Size::from_bytes(*size),
            if *status { "completed" } else { "incomplete" }
        );
    }
}

fn validate_file_paths(base_path: &Path, file_list: &[FileEntry]) -> Result<()> {
    for FileEntry {
        path: file_name, ..
    } in file_list
    {
        let file_name_path = Path::new(file_name);
        if file_name_path.is_absolute() {
            bail!(
                "the torrent file {} contained a file with absolute path, this is not acceptable",
                file_name
            )
        }
        for c in file_name_path.components() {
            if matches!(c, Component::ParentDir) {
                bail!(
                    "the torrent file {} contained a reference to a parent directory, this is not acceptable",
                    file_name
                )
            }
            if matches!(c, Component::Prefix(_)) {
                bail!(
                    "the torrent file {} contained a Windows prefix, this is not acceptable",
                    file_name
                )
            }
        }
        reject_symlink_components(base_path, file_name_path)?;
    }
    Ok(())
}

fn reject_symlink_components(base_path: &Path, relative_path: &Path) -> Result<()> {
    let mut current = base_path.to_path_buf();
    for component in relative_path.components() {
        if let Component::Normal(component) = component {
            current.push(component);
            if fs::symlink_metadata(&current)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                bail!(SymlinkPathError { path: current });
            }
        }
    }
    Ok(())
}

fn create_zero_length_files(base_path: &Path, file_list: &Vec<FileEntry>) -> Result<()> {
    // bittorrent allow zero lenght files, we don't need to download them, we can just create them here
    for f in file_list {
        if f.size > 0 {
            continue;
        }
        let file_path = base_path.join(&f.path);
        if let Some(dir) = file_path.parent() {
            fs::create_dir_all(dir)?;
        }
        File::create(file_path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::persistence::file_manager::FileEntry;
    use crate::persistence::file_manager::PiecesToFilePathsMapper;
    use crate::persistence::file_manager::get_file_list_with_completion_status;
    use crate::persistence::file_manager::validate_file_paths;
    use std::path::Path;
    use std::sync::Arc;

    #[test]
    fn test_validate_file_paths_1() {
        let file_list = vec![FileEntry::new("../f1".to_string(), 10)];
        let res = validate_file_paths(Path::new("./"), &file_list);
        assert!(res.is_err());
    }

    #[test]
    fn test_validate_file_paths_2() {
        let file_list = vec![FileEntry::new("/f1".to_string(), 10)];
        let res = validate_file_paths(Path::new("./"), &file_list);
        assert!(res.is_err());
    }

    #[test]
    fn test_validate_file_paths_3() {
        let file_list = vec![FileEntry::new("./f1".to_string(), 10)];
        let res = validate_file_paths(Path::new("./"), &file_list);
        assert!(res.is_ok());
    }

    #[test]
    fn test_refresh_completed_files_1() {
        let file_list = vec![
            FileEntry::new("f1".to_string(), 10),
            FileEntry::new("f2".to_string(), 10),
            FileEntry::new("f3".to_string(), 5),
            FileEntry::new("f4".to_string(), 3),
            FileEntry::new("f5".to_string(), 3),
        ];
        let pieces_to_file_paths_mapper =
            Arc::new(PiecesToFilePathsMapper::new(Path::new("./"), 3, 10, &file_list).unwrap());
        let piece_completion_status = vec![false, true, false];

        let res = get_file_list_with_completion_status(
            Path::new("./"),
            &file_list,
            pieces_to_file_paths_mapper,
            &piece_completion_status,
        );
        assert_eq!(
            res,
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
            FileEntry::new("f1".to_string(), 10),
            FileEntry::new("f2".to_string(), 10),
            FileEntry::new("f3".to_string(), 5),
            FileEntry::new("f4".to_string(), 3),
            FileEntry::new("f5".to_string(), 3),
        ];

        let pieces_to_file_paths_mapper =
            Arc::new(PiecesToFilePathsMapper::new(Path::new("./"), 3, 10, &file_list).unwrap());
        let piece_completion_status = vec![true, false, true];

        let res = get_file_list_with_completion_status(
            Path::new("./"),
            &file_list,
            pieces_to_file_paths_mapper,
            &piece_completion_status,
        );
        assert_eq!(
            res,
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
            FileEntry::new("f1".to_string(), 5),
            FileEntry::new("f2".to_string(), 20),
            FileEntry::new("f3".to_string(), 5),
        ];

        let pieces_to_file_paths_mapper = Arc::new(
            PiecesToFilePathsMapper::new(Path::new("relative/"), 3, 10, &file_list).unwrap(),
        );
        let piece_completion_status = vec![true, true, true];

        let res = get_file_list_with_completion_status(
            Path::new("relative/"),
            &file_list,
            pieces_to_file_paths_mapper,
            &piece_completion_status,
        );
        assert_eq!(
            res,
            vec![
                (std::path::PathBuf::from("relative/f1"), 5, true),
                (std::path::PathBuf::from("relative/f2"), 20, true),
                (std::path::PathBuf::from("relative/f3"), 5, true),
            ]
        );
    }
}

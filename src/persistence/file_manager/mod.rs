use anyhow::{Result, bail};
use sha1::{Digest, Sha1};
use size::Size;
use std::cmp;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{Receiver, UnboundedSender};

use crate::persistence::file_manager::file_handles::{
    FileHandlesForPiece, ReadFileHandles, WriteFileHandles,
};
use crate::persistence::file_manager::pieces_to_file_paths_mapper::PiecesToFilePathsMapper;
use crate::persistence::piece::Piece;
use crate::persistence::torrent_data_status::TorrentDataStatus;
use crate::util::{FileEntry, HostAndPort};

mod file_handles;
mod pieces_to_file_paths_mapper;

const MAX_CONCURRENT_READ_OPS: usize = 10; // todo: make this dynamic depending on the read speed (spinning disk should have this set to 1)

#[derive(Error, Debug)]
#[error(
    "the sha of the data we just wrote for piece {piece_idx} do not match the sha we expect, marking this piece as missing"
)]
pub struct ShaCorruptedError {
    piece_idx: usize,
}

#[derive(Error, Debug)]
#[error(
    "error on reading whole piece for final sha check {piece_idx}, marking this piece as missing: {error}"
)]
pub struct ShaCheckReadError {
    piece_idx: usize,
    error: anyhow::Error,
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

type PieceCompletionStatus = Vec<bool>; // piece identified by position in array -> download completed / incomplete
type PieceHashes = Vec<[u8; 20]>; // piece identified by position in array -> hash

pub struct WritePieceBlockRequest {
    pub requestor_peer_addr: HostAndPort,
    pub piece_idx: usize,
    pub data: Vec<u8>,
    pub block_begin: u64,
}

pub struct WritePieceBlockRequestReference {
    pub requestor_peer_addr: HostAndPort,
    pub piece_idx: usize,
}

pub struct TorrentDataStatusUpdates {
    pub piece_is_completed: bool,
    pub wasted_bytes: usize,
    pub incomplete_piece: Option<Piece>,
}

pub struct WritePieceBlockResponse {
    pub request: WritePieceBlockRequestReference,
    pub response: Result<TorrentDataStatusUpdates>,
}

pub struct ReadPieceBlockRequest {
    pub requestor_peer_addr: HostAndPort,
    pub piece_idx: usize,
    pub block_begin: u64,
    pub block_length: u64,
}

pub struct ReadPieceBlockResponse {
    pub request: ReadPieceBlockRequest,
    pub response: Result<Vec<u8>>,
}

#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::io::ErrorKind;
    use std::os::windows::fs::FileExt;
    let mut read = 0;
    while read < buf.len() {
        let n = file.seek_read(&mut buf[read..], offset + read as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        read += n;
    }
    Ok(())
}

#[cfg(unix)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

#[cfg(windows)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::io::ErrorKind;
    use std::os::windows::fs::FileExt;
    let mut written = 0;
    while written < buf.len() {
        let n = file.seek_write(&buf[written..], offset + written as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        written += n;
    }
    Ok(())
}

pub fn start_file_manager(
    base_path: &Path,
    file_list: Vec<FileEntry>,
    normal_piece_length: u64,
    piece_hashes: PieceHashes,
    mut read_requests_rx: Receiver<ReadPieceBlockRequest>,
    read_responses_tx: UnboundedSender<ReadPieceBlockResponse>,
    mut write_requests_rx: Receiver<WritePieceBlockRequest>,
    write_responses_tx: UnboundedSender<WritePieceBlockResponse>,
) -> TorrentDataStatus {
    let mut total_file_size = 0;
    for FileEntry { size, .. } in file_list.iter() {
        total_file_size += size;
    }
    let total_pieces = piece_hashes.len();
    if total_file_size > normal_piece_length * total_pieces as u64 {
        panic!(
            "the total file size of all files exceed the #pieces * piece_length we have, the .torrent file / metedata could be malformed"
        );
    }
    if total_file_size <= (normal_piece_length * (total_pieces as u64 - 1)) {
        panic!(
            "the total file size of all files does not cover all the declared pieces and piece_length we have, the .torrent file / metedata could be malformed"
        );
    }

    let pieces_to_file_paths_mapper = Arc::new(PiecesToFilePathsMapper::new(
        base_path,
        total_pieces,
        normal_piece_length,
        &file_list,
    ));

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

    let shared_piece_completion_status = Arc::new(Mutex::new(piece_completion_status.clone()));
    let last_piece_length = piece_sizer.last_piece_length;

    // read request loop
    let piece_completion_status_for_reads = shared_piece_completion_status.clone();
    let piece_sizer_for_reads = piece_sizer.clone();
    tokio::spawn(async move {
        let fs_reads_semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_READ_OPS));
        loop {
            tokio::select! {
                Some(read_piece_block_request) = read_requests_rx.recv() => {
                    handle_read_piece_block(
                        read_piece_block_request,
                        fs_reads_semaphore.clone(),
                        &read_responses_tx,
                        piece_completion_status_for_reads.clone(),
                        &piece_sizer_for_reads,
                        &mut file_handles,
                    ).await;
                }
                else => break,
            }
        }
    });

    // write request loop
    let handle = Handle::current();
    std::thread::spawn(move || {
        handle.block_on(async move {
            let mut file_handles = WriteFileHandles::new(pieces_to_file_paths_mapper);
            let mut incomplete_pieces = HashMap::new();
            loop {
                tokio::select! {
                    Some(write_piece_block_request) = write_requests_rx.recv() => {
                        handle_write_piece_block(
                        write_piece_block_request,
                        &write_responses_tx,
                        &piece_sizer,
                        shared_piece_completion_status.clone(),
                        &mut file_handles,
                        &piece_hashes,
                        &mut incomplete_pieces
                        );
                    }
                    else => break,
                }
            }
        })
    });

    TorrentDataStatus::new(
        piece_completion_status,
        normal_piece_length,
        last_piece_length,
    )
}

fn read_data(
    file_handles_for_piece: FileHandlesForPiece,
    block_begin: u64,
    block_length: u64,
) -> Result<Vec<u8>> {
    let mut block_buf = vec![0u8; block_length as usize];
    let mut block_buf_offset = 0;
    let mut current_piece_offset = 0;
    let mut block_bytes_still_to_read = block_length;
    for (file, start, end) in file_handles_for_piece.iter() {
        let mut file_offset = *start;
        if current_piece_offset != block_begin {
            let piece_fragment_size_in_file = end - start;
            if piece_fragment_size_in_file < block_begin - current_piece_offset {
                // the current chunk of data in the file is not enough to reach the beginning of the block we want to read
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

        read_at(
            file,
            &mut block_buf[block_buf_offset..block_buf_offset + bytes_to_read as usize],
            file_offset,
        )?;
        block_buf_offset += bytes_to_read as usize;
    }

    Ok(block_buf)
}

async fn handle_read_piece_block(
    read_piece_block_request: ReadPieceBlockRequest,
    fs_reads_semaphore: Arc<tokio::sync::Semaphore>,
    reads_file_manager_to_torrent_manager_tx: &UnboundedSender<ReadPieceBlockResponse>,

    piece_completion_status: Arc<Mutex<PieceCompletionStatus>>,
    piece_sizer: &PieceSizer,
    read_file_handles: &mut ReadFileHandles,
) {
    match read_piece_block_pre_checks(
        piece_completion_status,
        piece_sizer,
        read_piece_block_request.piece_idx,
        read_piece_block_request.block_begin,
        read_piece_block_request.block_length,
        true,
    ) {
        Ok(_) => {}
        Err(e) => {
            let _ = reads_file_manager_to_torrent_manager_tx.send(ReadPieceBlockResponse {
                request: read_piece_block_request,
                response: Result::Err(e),
            });
            return;
        }
    }

    let file_handles_for_piece =
        match read_file_handles.get_files_for_piece(read_piece_block_request.piece_idx) {
            Ok(f) => f,
            Err(e) => {
                let _ = reads_file_manager_to_torrent_manager_tx.send(ReadPieceBlockResponse {
                    request: read_piece_block_request,
                    response: Result::Err(e),
                });
                return;
            }
        };

    let reads_file_manager_to_torrent_manager_tx = reads_file_manager_to_torrent_manager_tx.clone();
    let permit = fs_reads_semaphore
        .acquire_owned()
        .await
        .expect("semaphore cannot be closed");
    tokio::task::spawn_blocking(move || {
        let result = read_data(
            file_handles_for_piece,
            read_piece_block_request.block_begin,
            read_piece_block_request.block_length,
        );
        drop(permit);
        let _ = reads_file_manager_to_torrent_manager_tx.send(ReadPieceBlockResponse {
            request: read_piece_block_request,
            response: result,
        });
    });
}

fn read_piece_block_pre_checks(
    piece_completion_status: Arc<Mutex<PieceCompletionStatus>>,
    piece_sizer: &PieceSizer,

    piece_idx: usize,
    block_begin: u64,
    block_length: u64,
    check_if_have_piece: bool,
) -> Result<()> {
    if piece_idx >= piece_sizer.total_pieces() {
        bail!(
            "requested to read piece idx {piece_idx} that is not in range (total pieces: {})",
            piece_sizer.total_pieces()
        );
    }
    let piece_length = piece_sizer.piece_length(piece_idx);
    if piece_length < block_begin + block_length {
        bail!(
            "requested to read piece idx {piece_idx} out of range: block_begin {block_begin} + block_length {block_length} > piece_length {piece_length}"
        );
    }
    if check_if_have_piece {
        let piece_completion_status_mg = piece_completion_status
            .lock()
            .expect("another user panicked while holding the lock");
        let completed = piece_completion_status_mg[piece_idx];
        drop(piece_completion_status_mg);
        if !completed {
            bail!("requested to read piece idx {piece_idx} that we don't have");
        }
    }
    Ok(())
}

fn handle_write_piece_block(
    write_piece_block_request: WritePieceBlockRequest,
    writes_file_manager_to_torrent_manager_tx: &UnboundedSender<WritePieceBlockResponse>,

    piece_sizer: &PieceSizer,
    piece_completion_status: Arc<Mutex<PieceCompletionStatus>>,
    write_file_handles: &mut WriteFileHandles,
    piece_hashes: &PieceHashes,
    incomplete_pieces: &mut HashMap<usize, Piece>,
) {
    let result = write_piece_block(
        write_piece_block_request.piece_idx,
        write_piece_block_request.data,
        write_piece_block_request.block_begin,
        piece_sizer,
        piece_completion_status,
        write_file_handles,
        piece_hashes,
        incomplete_pieces,
    );
    _ = writes_file_manager_to_torrent_manager_tx.send(WritePieceBlockResponse {
        request: WritePieceBlockRequestReference {
            requestor_peer_addr: write_piece_block_request.requestor_peer_addr,
            piece_idx: write_piece_block_request.piece_idx,
        },
        response: result,
    });
}

fn write_piece_block(
    piece_idx: usize,
    data: Vec<u8>,
    block_begin: u64, // position in the piece where to start writing data

    piece_sizer: &PieceSizer,
    piece_completion_status: Arc<Mutex<PieceCompletionStatus>>,
    write_file_handles: &mut WriteFileHandles,
    piece_hashes: &PieceHashes,
    incomplete_pieces: &mut HashMap<usize, Piece>,
) -> Result<TorrentDataStatusUpdates> {
    if piece_idx >= piece_sizer.total_pieces() {
        bail!(
            "cannot write block: piece idx {piece_idx} would overflow total pieces ({})",
            piece_sizer.total_pieces()
        );
    }

    if data.is_empty() {
        bail!("cannot write block: the block carries no data");
    }

    // avoid useless writes if we already have the piece
    let piece_completion_status_mg = piece_completion_status
        .lock()
        .expect("another user panicked while holding the lock");
    let completed = piece_completion_status_mg[piece_idx];
    drop(piece_completion_status_mg);
    if completed {
        log::trace!("we already have the piece {piece_idx}, will avoid to writing it again");
        return Ok(TorrentDataStatusUpdates {
            piece_is_completed: true,
            wasted_bytes: data.len(),
            incomplete_piece: None,
        });
    }

    let piece_len = piece_sizer.piece_length(piece_idx);
    let data_len = data.len() as u64;
    if block_begin + data_len > piece_len {
        bail!("cannot write block: data would overflow the piece");
    }

    let piece = incomplete_pieces
        .entry(piece_idx)
        .or_insert(Piece::new(piece_len));

    if piece.contains(block_begin, block_begin + data_len - 1) {
        log::trace!(
            "we already have written all the data in this block (begin: {block_begin} length: {data_len}) for piece {piece_idx}, will avoid writing it again"
        );
        return Ok(TorrentDataStatusUpdates {
            piece_is_completed: false,
            wasted_bytes: data.len(),
            incomplete_piece: Some(piece.clone()),
        });
    }

    // finally write this block
    let mut data_cursor: u64 = 0;
    let mut data_still_to_be_written = data_len;
    let mut piece_cursor_to_begin = 0;
    let file_handles_for_piece = write_file_handles.get_files_for_piece(piece_idx)?;
    for (file, file_start, file_end) in file_handles_for_piece.iter() {
        if data_still_to_be_written == 0 {
            break;
        }
        let mut file_start = *file_start;
        let file_end = *file_end;
        if block_begin - piece_cursor_to_begin < file_end - file_start {
            file_start += block_begin - piece_cursor_to_begin;
            piece_cursor_to_begin = block_begin;
        } else {
            piece_cursor_to_begin += file_end - file_start;
            continue;
        }
        let data_to_write = cmp::min(file_end - file_start, data_still_to_be_written);
        write_at(
            &file,
            &data[data_cursor as usize..(data_cursor + data_to_write) as usize],
            file_start,
        )?;
        data_cursor += data_to_write;
        data_still_to_be_written -= data_to_write;
    }

    piece.add_fragment(block_begin, block_begin + data_len - 1);

    // check if piece is completed
    if piece.complete() {
        incomplete_pieces.remove(&piece_idx);

        read_piece_block_pre_checks(
            piece_completion_status.clone(),
            piece_sizer,
            piece_idx,
            0,
            piece_len,
            false,
        )?;

        let read_piece_data = match read_data(file_handles_for_piece, 0, piece_len) {
            Ok(data) => data,
            Err(error) => bail!(ShaCheckReadError { piece_idx, error }),
        };

        let piece_sha: [u8; 20] = Sha1::digest(read_piece_data).into();
        if piece_sha != piece_hashes[piece_idx] {
            bail!(ShaCorruptedError { piece_idx });
        } else {
            let mut piece_completion_status_mg = piece_completion_status
                .lock()
                .expect("another user panicked while holding the lock");
            piece_completion_status_mg[piece_idx] = true;
            drop(piece_completion_status_mg);
        }
        Ok(TorrentDataStatusUpdates {
            piece_is_completed: true,
            wasted_bytes: 0,
            incomplete_piece: None,
        })
    } else {
        Ok(TorrentDataStatusUpdates {
            piece_is_completed: false,
            wasted_bytes: 0,
            incomplete_piece: Some(piece.clone()),
        })
    }
}

fn refresh_completed_pieces(
    piece_hashes: &PieceHashes,
    piece_sizer: &PieceSizer,
    file_handles: &mut ReadFileHandles,
) -> PieceCompletionStatus {
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
    piece_completion_status: &PieceCompletionStatus,
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
    piece_completion_status: &PieceCompletionStatus,
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

#[cfg(test)]
mod tests {
    use crate::persistence::file_manager::FileEntry;
    use crate::persistence::file_manager::PiecesToFilePathsMapper;
    use crate::persistence::file_manager::get_file_list_with_completion_status;
    use std::path::Path;
    use std::sync::Arc;

    #[test]
    fn test_refresh_completed_files_1() {
        let file_list = vec![
            FileEntry::new("f1".to_string(), 10),
            FileEntry::new("f2".to_string(), 10),
            FileEntry::new("f3".to_string(), 5),
            FileEntry::new("f4".to_string(), 3),
            FileEntry::new("f5".to_string(), 3),
        ];
        let pieces_to_file_paths_mapper = Arc::new(PiecesToFilePathsMapper::new(
            Path::new("./"),
            3,
            10,
            &file_list,
        ));
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

        let pieces_to_file_paths_mapper = Arc::new(PiecesToFilePathsMapper::new(
            Path::new("./"),
            3,
            10,
            &file_list,
        ));
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

        let pieces_to_file_paths_mapper = Arc::new(PiecesToFilePathsMapper::new(
            Path::new("relative/"),
            3,
            10,
            &file_list,
        ));
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

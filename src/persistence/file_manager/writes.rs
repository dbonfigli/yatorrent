use anyhow::{Error, Result, anyhow};
use dashmap::DashMap;
use sha1::{Digest, Sha1};
use std::cmp::min;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{Receiver, UnboundedSender};

use crate::manager::BLOCK_SIZE_B;
use crate::persistence::file_manager::file_handles::{FileHandlesForPiece, WriteFileHandles};
use crate::persistence::file_manager::pieces_to_file_paths_mapper::PiecesToFilePathsMapper;
use crate::persistence::file_manager::reads::read_data;
use crate::persistence::file_manager::{
    DiskConfig, PieceCompletionStatus, PieceHashes, PieceSizer,
};
use crate::persistence::piece::Piece;
use crate::util::HostAndPort;

//max memory used to hold unordered data so to allow incremental hashing and avoid the final readback from disk for piece verification
const MAX_UNHASHED_DATA_SIZE: usize = 1024 * 16 * 100; // i.e. 100 standard blocks, 1.6MB

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
    error: Error,
}

pub struct WritePieceBlockRequest {
    pub requestor_peer_addr: HostAndPort,
    pub piece_idx: usize,
    pub data: Vec<u8>,
    pub block_begin: u64,
}

pub struct WritePieceBlockRequestReference {
    pub requestor_peer_addr: HostAndPort,
    pub piece_idx: usize,
    pub block_begin: u64,
    pub data_len: u64,
}

pub struct WritePieceBlockResponse {
    pub request: WritePieceBlockRequestReference,
    pub response: Result<TorrentDataStatusUpdates>,
}

pub struct TorrentDataStatusUpdates {
    pub piece_is_completed: bool,
    pub wasted_bytes: usize,
}

struct IncompletePiece {
    committed_piece: Piece, // piece with info about data really written
    claimed_piece: Piece, // piece with info about data that we declared we are writing, i.e. writes are in flight and we do not know they completed. committed_piece holds always a subset of claimed_piece.

    incremental_hash: Sha1, // hash already calculated with the sequential data already arrived
    already_hashed_to: u64, // until which byte we have already hashed in incremental_hash (not inclusive)
    piece_data: HashMap<u64, Vec<u8>>, // we divide the data in blocks of 16KB (BLOCK_SIZE_B, a standard block), the index is the block of data we have that has not been used yet for hashing
}

impl IncompletePiece {
    fn new(piece_len: u64) -> Self {
        IncompletePiece {
            committed_piece: Piece::new(piece_len),
            claimed_piece: Piece::new(piece_len),
            incremental_hash: Sha1::new(),
            already_hashed_to: 0,
            piece_data: HashMap::new(),
        }
    }
}

struct UnhashedDataSize {
    current: AtomicUsize,
}

impl UnhashedDataSize {
    fn new() -> Self {
        UnhashedDataSize {
            current: AtomicUsize::new(0),
        }
    }

    fn decrease(&self, size: usize) {
        // relaxed here and below is ok since threshold check is approximate by nature
        self.current
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(size))
            })
            .expect("etch_update closure always returns Some");
    }

    fn increase(&self, size: usize) {
        self.current.fetch_add(size, Ordering::Relaxed);
    }

    fn is_over_threshold(&self) -> bool {
        self.current.load(Ordering::Relaxed) > MAX_UNHASHED_DATA_SIZE
    }
}

pub async fn run_writes_loop(
    pieces_to_file_paths_mapper: Arc<PiecesToFilePathsMapper>,
    piece_hashes: &PieceHashes,
    mut write_requests_rx: Receiver<WritePieceBlockRequest>,
    write_responses_tx: UnboundedSender<WritePieceBlockResponse>,
    shared_piece_completion_status: Arc<PieceCompletionStatus>,
    piece_sizer: &PieceSizer,
    disk_config: &DiskConfig,
) {
    let fs_writes_semaphore = Arc::new(tokio::sync::Semaphore::new(
        disk_config.max_concurrent_disk_writes, // todo: maybe make this dynamic
    ));
    let mut file_handles = WriteFileHandles::new(pieces_to_file_paths_mapper);
    let incomplete_pieces = Arc::new(DashMap::new());
    let piece_state_transition_lock = Arc::new(Mutex::new(()));
    let unhashed_data_size = Arc::new(UnhashedDataSize::new());
    let piece_sizer = Arc::new(piece_sizer.clone());
    let piece_hases = Arc::new(piece_hashes.clone());
    loop {
        tokio::select! {
            Some(write_piece_block_request) = write_requests_rx.recv() => {
                handle_write_piece_block(
                write_piece_block_request,
                fs_writes_semaphore.clone(),
                write_responses_tx.clone(),
                piece_state_transition_lock.clone(),
                shared_piece_completion_status.clone(),
                incomplete_pieces.clone(),
                piece_sizer.clone(),
                &mut file_handles,
                piece_hases.clone(),
                unhashed_data_size.clone(),
                ).await;
            }
            else => break,
        }
    }
}

async fn handle_write_piece_block(
    write_request: WritePieceBlockRequest,
    fs_writes_semaphore: Arc<Semaphore>,
    write_responses_tx: UnboundedSender<WritePieceBlockResponse>,

    piece_state_transition_lock: Arc<Mutex<()>>,
    piece_completion_status: Arc<PieceCompletionStatus>,
    incomplete_pieces: Arc<DashMap<usize, Arc<Mutex<IncompletePiece>>>>,
    piece_sizer: Arc<PieceSizer>,
    file_handles: &mut WriteFileHandles,
    piece_hashes: Arc<PieceHashes>,
    unhashed_data_size: Arc<UnhashedDataSize>,
) {
    if write_request.piece_idx >= piece_sizer.total_pieces() {
        send_write_piece_block_reply(
            &write_responses_tx,
            &write_request,
            Err(anyhow!(
                "cannot write block: piece idx {} would overflow total pieces ({})",
                write_request.piece_idx,
                piece_sizer.total_pieces(),
            )),
        );
        return;
    }

    if write_request.data.is_empty() {
        send_write_piece_block_reply(
            &write_responses_tx,
            &write_request,
            Err(anyhow!("cannot write block: the block carries no data")),
        );
        return;
    }

    let piece_len = piece_sizer.piece_length(write_request.piece_idx);
    let data_len = write_request.data.len() as u64;
    if write_request.block_begin + data_len > piece_len {
        send_write_piece_block_reply(
            &write_responses_tx,
            &write_request,
            Err(anyhow!("cannot write block: data would overflow the piece")),
        );
        return;
    }

    let file_handles_for_piece = match file_handles.get_files_for_piece(write_request.piece_idx) {
        Ok(file_handles_for_piece) => file_handles_for_piece,
        Err(e) => {
            send_write_piece_block_reply(&write_responses_tx, &write_request, Err(e.into()));
            return;
        }
    };
    {
        // here we want to avoid proceeding with the writing if the piece is complete or the piece block is already claimed.
        // Otherwise, we risk corrupting the piece that has been verified.

        // The two checks must be coherent, we must avoid a race condition like this:
        // 1. we pass the completion check (completed == false)
        // 2. another thread sets completed to true, and remove the entries in incomplete_pieces
        // 3. we find the incomplete_pieces entry missing, we create a new one
        // 4. no one else claimed the block, we proceed to writing it -> potential corruption
        //
        // To avoid this, we use piece_state_transition_lock: we hold it whenever we adds/removes entries
        // in incomplete_pieces, and at the same time reading/writing piece_completion_status.
        let piece_state_transition_lock_mg = piece_state_transition_lock
            .lock()
            .expect("another user panicked while holding the lock");

        // avoid useless writes if we already have the piece
        let completed = piece_completion_status[write_request.piece_idx].load(Ordering::Acquire);
        if completed {
            drop(piece_state_transition_lock_mg);
            log::trace!(
                "we already have the piece {}, will avoid to writing it again",
                write_request.piece_idx
            );
            send_write_piece_block_reply(
                &write_responses_tx,
                &write_request,
                Ok(TorrentDataStatusUpdates {
                    piece_is_completed: true,
                    wasted_bytes: write_request.data.len(),
                }),
            );
            return;
        }

        // avoid concurrent writes on the same block
        let incomplete_piece = incomplete_pieces
            .entry(write_request.piece_idx)
            .or_insert_with(|| Arc::new(Mutex::new(IncompletePiece::new(piece_len))))
            .clone();
        let mut incomplete_piece = incomplete_piece
            .lock()
            .expect("another user panicked while holding the lock");

        if incomplete_piece.claimed_piece.overlaps(
            write_request.block_begin,
            write_request.block_begin + data_len - 1,
        ) {
            drop(piece_state_transition_lock_mg);
            log::trace!(
                "some or all of the data in this block (begin: {} length: {}) for piece {} is already written or writes are already inflight, will avoid writing it again so to not corrupt possible pieces already confirmed",
                write_request.block_begin,
                data_len,
                write_request.piece_idx,
            );
            send_write_piece_block_reply(
                &write_responses_tx,
                &write_request,
                Ok(TorrentDataStatusUpdates {
                    piece_is_completed: false,
                    wasted_bytes: write_request.data.len(),
                }),
            );
            return;
        }

        // we are about to write data to this piece, keep track of it in claimed_piece
        // from now on, on handling this piece block write, if it fails, we have to remove it on claimed_piece
        incomplete_piece.claimed_piece.add_fragment(
            write_request.block_begin,
            write_request.block_begin + data_len - 1,
        );
    }

    let permit = fs_writes_semaphore
        .acquire_owned()
        .await
        .expect("semaphore cannot be closed");
    tokio::task::spawn_blocking(move || {
        do_write_piece_block(
            write_request,
            write_responses_tx,
            piece_hashes,
            piece_state_transition_lock,
            piece_completion_status,
            incomplete_pieces,
            piece_sizer,
            file_handles_for_piece,
            unhashed_data_size,
        );
        drop(permit);
    });
}

fn do_write_piece_block(
    write_request: WritePieceBlockRequest,
    write_responses_tx: UnboundedSender<WritePieceBlockResponse>,
    piece_hashes: Arc<PieceHashes>,
    piece_state_transition_lock: Arc<Mutex<()>>,
    piece_completion_status: Arc<PieceCompletionStatus>,
    incomplete_pieces: Arc<DashMap<usize, Arc<Mutex<IncompletePiece>>>>,
    piece_sizer: Arc<PieceSizer>,
    file_handles_for_piece: FileHandlesForPiece,
    unhashed_data_size: Arc<UnhashedDataSize>,
) {
    let data_len = write_request.data.len() as u64;
    if let Err(e) = disk_write_piece_block(
        &write_request.data,
        write_request.block_begin,
        &file_handles_for_piece,
    ) {
        // remove uncommitted piece block that failed to be written
        if let Some(incomplete_piece) = incomplete_pieces.get(&write_request.piece_idx) {
            incomplete_piece
                .lock()
                .expect("another user panicked while holding the lock")
                .claimed_piece
                .remove_fragment(
                    write_request.block_begin,
                    write_request.block_begin + data_len - 1,
                );
        }
        send_write_piece_block_reply(&write_responses_tx, &write_request, Err(e.into()));
        return;
    }

    let incomplete_piece = match incomplete_pieces.get(&write_request.piece_idx) {
        Some(incomplete_piece) => incomplete_piece.clone(),
        None => {
            send_write_piece_block_reply(
                &write_responses_tx,
                &write_request,
                Err(anyhow!(
                    "we could not find the incomplete piece for a write request, this should never happen"
                )),
                // because none should ever remove the incomplete piece unless the piece is complete, and write requests
                // are rejected if the piece is completed and or data is already scheduled to be written
            );
            return;
        }
    };
    let (piece_is_complete, already_hashed_to, incremental_hash) = {
        let mut incomplete_piece = incomplete_piece
            .lock()
            .expect("another user panicked while holding the lock");
        incomplete_piece.committed_piece.add_fragment(
            write_request.block_begin,
            write_request.block_begin + data_len - 1,
        );
        stash_and_hash(
            &mut *incomplete_piece,
            &write_request,
            unhashed_data_size.clone(),
            &piece_sizer,
        );
        (
            incomplete_piece.committed_piece.complete(),
            incomplete_piece.already_hashed_to,
            incomplete_piece.incremental_hash.clone(),
        )
    };

    if !piece_is_complete {
        send_write_piece_block_reply(
            &write_responses_tx,
            &write_request,
            Ok(TorrentDataStatusUpdates {
                piece_is_completed: false,
                wasted_bytes: 0,
            }),
        );
    } else {
        match verify_completed_piece(
            &write_request,
            piece_sizer,
            file_handles_for_piece,
            piece_hashes,
            already_hashed_to,
            incremental_hash,
        ) {
            Ok(()) => {
                {
                    let removed = {
                        let _piece_state_transition_lock_mg = piece_state_transition_lock
                            .lock()
                            .expect("another user panicked while holding the lock");

                        // mark this piece as completed while holding the lock on piece_state_transition_lock
                        piece_completion_status[write_request.piece_idx]
                            .store(true, Ordering::Release);
                        // remove the incomplete piece, we don't need it anymore once the piece is completed
                        incomplete_pieces.remove(&write_request.piece_idx)
                    };
                    if let Some((_, r)) = removed {
                        let r = r
                            .lock()
                            .expect("another user panicked while holding the lock");
                        unhashed_data_size.decrease(r.piece_data.values().map(Vec::len).sum());
                    }
                }

                send_write_piece_block_reply(
                    &write_responses_tx,
                    &write_request,
                    Ok(TorrentDataStatusUpdates {
                        piece_is_completed: true,
                        wasted_bytes: 0,
                    }),
                );
            }
            Err(e) => {
                // clear incomplete piece data to start over the download of the whole piece
                let removed = incomplete_pieces.remove(&write_request.piece_idx);
                if let Some((_, r)) = removed {
                    let r = r
                        .lock()
                        .expect("another user panicked while holding the lock");
                    unhashed_data_size.decrease(r.piece_data.values().map(Vec::len).sum());
                }
                send_write_piece_block_reply(&write_responses_tx, &write_request, Err(e.into()));
            }
        }
    }
}

fn stash_and_hash(
    incomplete_piece: &mut IncompletePiece,
    write_request: &WritePieceBlockRequest,
    unhashed_data_size: Arc<UnhashedDataSize>,
    piece_sizer: &PieceSizer,
) {
    if !write_request.block_begin.is_multiple_of(BLOCK_SIZE_B) {
        log::debug!(
            "the begin of the block we are writing ({}) is not divisible by the block size ({}), this should never happen (did we receive a block we did not request?), we cannot store it for incremental hashing",
            write_request.block_begin,
            BLOCK_SIZE_B
        );
        return;
    }

    let expected_block_len = min(
        BLOCK_SIZE_B,
        piece_sizer.piece_length(write_request.piece_idx) - write_request.block_begin,
    );
    if write_request.data.len() as u64 != expected_block_len {
        log::debug!(
            "the block request length we are writing ({}) is not one we expect ({}), this should never happen (did we receive a block we did not request?), we cannot store it for incremental hashing",
            write_request.block_begin,
            BLOCK_SIZE_B
        );
        return;
    }

    let block_index = write_request.block_begin / BLOCK_SIZE_B;

    if incomplete_piece.already_hashed_to as u64 == write_request.block_begin {
        incomplete_piece
            .incremental_hash
            .update(&write_request.data);
        incomplete_piece.already_hashed_to =
            incomplete_piece.already_hashed_to + write_request.data.len() as u64;

        let mut idx = block_index + 1;
        while let Some(data) = incomplete_piece.piece_data.get(&idx) {
            incomplete_piece.incremental_hash.update(data);
            incomplete_piece.already_hashed_to =
                incomplete_piece.already_hashed_to + data.len() as u64;
            let removed = incomplete_piece.piece_data.remove(&idx); // we don't need this anymore, already hashed
            if let Some(r) = removed {
                unhashed_data_size.decrease(r.len());
            }
            idx += 1;
        }
    } else {
        if unhashed_data_size.is_over_threshold() {
            log::debug!("cannot cache, exeeding write cache size");
            // since we cannot stash this block of data, there is no point keeping the data kept in memory after this block
            incomplete_piece.piece_data.retain(|i, v| {
                if *i > block_index {
                    unhashed_data_size.decrease(v.len());
                    return false;
                }
                return true;
            });
            return;
        }
        incomplete_piece
            .piece_data
            .insert(block_index, write_request.data.clone());
        unhashed_data_size.increase(write_request.data.len());
    }
}

fn send_write_piece_block_reply(
    writes_file_manager_to_torrent_manager_tx: &UnboundedSender<WritePieceBlockResponse>,
    write_piece_block_request: &WritePieceBlockRequest,
    write_piece_block_result: Result<TorrentDataStatusUpdates>,
) {
    _ = writes_file_manager_to_torrent_manager_tx.send(WritePieceBlockResponse {
        request: WritePieceBlockRequestReference {
            requestor_peer_addr: write_piece_block_request.requestor_peer_addr.clone(),
            piece_idx: write_piece_block_request.piece_idx,
            block_begin: write_piece_block_request.block_begin,
            data_len: write_piece_block_request.data.len() as u64,
        },
        response: write_piece_block_result,
    });
}

fn verify_completed_piece(
    write_piece_block_request: &WritePieceBlockRequest,
    piece_sizer: Arc<PieceSizer>,
    file_handles_for_piece: FileHandlesForPiece,
    piece_hashes: Arc<PieceHashes>,
    already_hashed_to: u64,
    mut incremental_hash: Sha1,
) -> Result<()> {
    let piece_idx = write_piece_block_request.piece_idx;
    let piece_size = piece_sizer.piece_length(piece_idx);
    if already_hashed_to < piece_size {
        let read_piece_data = match read_data(
            file_handles_for_piece,
            already_hashed_to,
            piece_size - already_hashed_to,
        ) {
            Ok(data) => data,
            Err(error) => {
                return Err(anyhow!(ShaCheckReadError { piece_idx, error }));
            }
        };

        incremental_hash.update(&read_piece_data);
    }

    // we are in a spawn_blocking context so in theory this sha verification could starve the cpu since thread pools are not bound to CPUs available;
    // in practice, writes and then the read of the whole piece above are dominating time so we don't expect to incur in such situation
    let piece_sha: [u8; 20] = incremental_hash.finalize().into();
    if piece_sha != piece_hashes[piece_idx] {
        return Err(anyhow!(ShaCorruptedError { piece_idx }));
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

fn disk_write_piece_block(
    data: &Vec<u8>,
    block_begin: u64,
    file_handles_for_piece: &FileHandlesForPiece,
) -> io::Result<()> {
    let mut data_cursor: u64 = 0;
    let mut data_still_to_be_written = data.len() as u64;
    let mut piece_cursor_to_begin = 0;
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
        let data_to_write = min(file_end - file_start, data_still_to_be_written);
        write_at(
            &file,
            &data[data_cursor as usize..(data_cursor + data_to_write) as usize],
            file_start,
        )?;
        data_cursor += data_to_write;
        data_still_to_be_written -= data_to_write;
    }

    if data_still_to_be_written > 0 {
        // this should never happen, it is a bug if it does
        log::warn!("not all data was written for a block request");
    }

    Ok(())
}

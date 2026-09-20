use anyhow::{Result, bail};
use std::fs::File;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{Receiver, UnboundedSender};

use crate::persistence::file_manager::file_handles::{FileHandlesForPiece, ReadFileHandles};
use crate::persistence::file_manager::{PieceCompletionStatus, PieceSizer};
use crate::util::HostAndPort;

// todo: make this dynamic depending on the read speed (spinning disk should have concurrent read ops set 1)
const MAX_CONCURRENT_READ_OPS: usize = 10;

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

pub fn read_data(
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

pub async fn reads_loop(
    mut read_requests_rx: Receiver<ReadPieceBlockRequest>,
    read_responses_tx: UnboundedSender<ReadPieceBlockResponse>,
    piece_completion_status: Arc<PieceCompletionStatus>,
    piece_sizer: &PieceSizer,
    file_handles: &mut ReadFileHandles,
) {
    let fs_reads_semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_READ_OPS));
    loop {
        tokio::select! {
            Some(read_piece_block_request) = read_requests_rx.recv() => {
                handle_read_piece_block(
                    read_piece_block_request,
                    fs_reads_semaphore.clone(),
                    &read_responses_tx,
                    piece_completion_status.clone(),
                    &piece_sizer,
                     file_handles,
                ).await;
            }
            else => break,
        }
    }
}

async fn handle_read_piece_block(
    read_request: ReadPieceBlockRequest,
    fs_reads_semaphore: Arc<Semaphore>,
    read_responses_tx: &UnboundedSender<ReadPieceBlockResponse>,

    piece_completion_status: Arc<PieceCompletionStatus>,
    piece_sizer: &PieceSizer,
    file_handles: &mut ReadFileHandles,
) {
    if let Err(e) = read_piece_block_pre_checks(
        piece_completion_status,
        piece_sizer,
        read_request.piece_idx,
        read_request.block_begin,
        read_request.block_length,
    ) {
        let _ = read_responses_tx.send(ReadPieceBlockResponse {
            request: read_request,
            response: Err(e),
        });
        return;
    }

    let file_handles_for_piece = match file_handles.get_files_for_piece(read_request.piece_idx) {
        Ok(f) => f,
        Err(e) => {
            let _ = read_responses_tx.send(ReadPieceBlockResponse {
                request: read_request,
                response: Err(e),
            });
            return;
        }
    };

    let read_responses_tx_for_spawn = read_responses_tx.clone();
    let permit = fs_reads_semaphore
        .acquire_owned()
        .await
        .expect("semaphore cannot be closed");
    tokio::task::spawn_blocking(move || {
        let result = read_data(
            file_handles_for_piece,
            read_request.block_begin,
            read_request.block_length,
        );
        drop(permit);
        let _ = read_responses_tx_for_spawn.send(ReadPieceBlockResponse {
            request: read_request,
            response: result,
        });
    });
}

fn read_piece_block_pre_checks(
    piece_completion_status: Arc<PieceCompletionStatus>,
    piece_sizer: &PieceSizer,

    piece_idx: usize,
    block_begin: u64,
    block_length: u64,
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
    if !piece_completion_status[piece_idx].load(Ordering::Acquire) {
        bail!("requested to read piece idx {piece_idx} that we don't have");
    }

    Ok(())
}

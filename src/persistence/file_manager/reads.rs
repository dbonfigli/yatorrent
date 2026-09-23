use anyhow::{Result, bail};
use moka::future::Cache;
use std::cmp::{max, min};
use std::fs::File;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, UnboundedSender};
use tokio::sync::{OnceCell, Semaphore};

use crate::persistence::file_manager::file_handles::{FileHandlesForPiece, ReadFileHandles};
use crate::persistence::file_manager::{DiskConfig, PieceCompletionStatus, PieceSizer};
use crate::util::HostAndPort;

// the chunk size is not just 16 KB because this size has also the double duty of read ahead cache:
// we read also data follwing the requested block from clients (usually 16 KB), since most probably future requests
// from the same client will arrive, and when it happens we will have that already in cache
pub const READ_CACHE_CHUNK_SIZE: usize = 1024 * 256; // 256 KB

// this regulates how many concurrent tokio tasks we spawn from the read requests queue.
// We don't want to spawn an uncontrollable amount of tasks, because it just another queue, unbounded,
// and could overwhelm the memory, but also we don't want to limit this artifically if the user configured
// a high number for max_concurrent_disk_writes
const MIN_CONCURRENT_READ_PIECE_BLOCK_TASKS: usize = 50;

type ChunkKey = (usize, usize); // (piece index, chunk index in ascending order within that piece)

struct ChunkStore {
    // Cache is moka concurrent cache, LRU + LFU
    // the value is:
    // * an Arc, so that we can pass it around (moka does not allow getting back references)
    // * a OnceCell, so that the initialization is done only once and not concurrently, i.e. concurrent requests for the same chunk
    //   are correctly handled: only the first is served via reading the disk, and the others wait
    // * anoter Arc to the data itself, so that we can reference it and not just copy it when requets are served from the cache when we do get_chunk
    chunks: Cache<ChunkKey, Arc<OnceCell<Arc<Vec<u8>>>>>,
}

impl ChunkStore {
    fn new(max_read_cache_size: usize, read_cache_idle_time: usize) -> Self {
        // we need the cache to hold at least 2 chunks so to correctl serve a block request spanning 2 chunks
        // a chunk is always bigger than a the maximum allowed block size anyway
        let cache_entries = max(2, max_read_cache_size / READ_CACHE_CHUNK_SIZE);
        let mut cache_builder = Cache::builder().max_capacity(cache_entries as u64);
        if read_cache_idle_time > 0 {
            cache_builder =
                cache_builder.time_to_idle(Duration::from_secs(read_cache_idle_time as u64));
        }

        let cache = Self {
            chunks: cache_builder.build(),
        };

        let chunks = cache.chunks.clone();
        // we need this otherwise if there is no traffic time_to_idle is not honored (need an operation to be triggered)
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                chunks.run_pending_tasks().await;
                log::debug!("read cache entries: {}", chunks.entry_count());
            }
        });

        cache
    }

    async fn get_chunk(
        &self,
        piece_idx: usize,
        chunk_idx: usize,
        fs_reads_semaphore: Arc<Semaphore>,
        files_handles_for_piece: FileHandlesForPiece,
        piece_sizer: &PieceSizer,
    ) -> Result<Arc<Vec<u8>>> {
        let cell = self
            .chunks
            .entry((piece_idx, chunk_idx))
            .or_insert_with(async { Arc::new(OnceCell::new()) })
            .await;

        let chunk = cell
            .value()
            .get_or_try_init(|| async move {
                let chunk_begin = (chunk_idx * READ_CACHE_CHUNK_SIZE) as u64;
                let piece_length = piece_sizer.piece_length(piece_idx);
                if chunk_begin >= piece_length {
                    bail!("chunk index is past the end of the piece");
                }
                let chunk_length = min(READ_CACHE_CHUNK_SIZE as u64, piece_length - chunk_begin);

                let permit = fs_reads_semaphore
                    .acquire_owned()
                    .await
                    .expect("semaphore cannot be closed");

                let data = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    read_data(files_handles_for_piece, chunk_begin, chunk_length)
                })
                .await??;

                Ok(Arc::new(data))
            })
            .await?;

        Ok(chunk.clone())
    }
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

    if block_bytes_still_to_read > 0 {
        // this should never happen, it is a bug if it does
        log::warn!("not all data was read for a block request");
    }

    Ok(block_buf)
}

pub async fn run_reads_loop(
    mut read_requests_rx: Receiver<ReadPieceBlockRequest>,
    read_responses_tx: UnboundedSender<ReadPieceBlockResponse>,
    piece_completion_status: Arc<PieceCompletionStatus>,
    piece_sizer: &PieceSizer,
    file_handles: &mut ReadFileHandles,
    disk_config: &DiskConfig,
) {
    let read_requests_semaphore = Arc::new(Semaphore::new(max(
        MIN_CONCURRENT_READ_PIECE_BLOCK_TASKS,
        disk_config.max_concurrent_disk_reads,
    )));
    let fs_reads_semaphore = Arc::new(Semaphore::new(disk_config.max_concurrent_disk_reads)); // todo: maybe make this dynamic
    let chunk_store = Arc::new(ChunkStore::new(
        disk_config.max_read_cache_size,
        disk_config.read_cache_idle_time,
    ));

    loop {
        tokio::select! {
            Some(read_request) = read_requests_rx.recv() => {
                let fs_reads_semaphore = fs_reads_semaphore.clone();
                let read_responses_tx = read_responses_tx.clone();
                let piece_completion_status = piece_completion_status.clone();
                let chunk_store = chunk_store.clone();
                let piece_sizer = piece_sizer.clone();
                let files_handles_for_piece =
                    match file_handles.get_files_for_piece(read_request.piece_idx) {
                        Ok(f) => f,
                        Err(e) => {
                            let _ = read_responses_tx.send(ReadPieceBlockResponse {
                                request: read_request,
                                response: Err(e),
                            });
                            return;
                        }
                    };

                let permit = read_requests_semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("semaphore cannot be closed");
                tokio::spawn(async move {
                    handle_read_piece_block(
                        read_request,
                        fs_reads_semaphore.clone(),
                        &read_responses_tx,
                        piece_completion_status.clone(),
                        &piece_sizer,
                        files_handles_for_piece,
                        chunk_store.clone(),
                    )
                    .await;
                    drop(permit);
                });
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
    files_handles_for_piece: FileHandlesForPiece,
    chunk_store: Arc<ChunkStore>,
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

    let piece_idx = read_request.piece_idx;
    let block_begin = read_request.block_begin as usize;
    let block_length = read_request.block_length as usize;
    let block_end = block_begin + block_length;
    let first_chunk_idx = block_begin / READ_CACHE_CHUNK_SIZE;
    let last_chunk_idx = (block_end - 1) / READ_CACHE_CHUNK_SIZE;
    let mut response = Vec::with_capacity(block_length);
    for chunk_idx in first_chunk_idx..=last_chunk_idx {
        let chunk = match chunk_store
            .get_chunk(
                piece_idx,
                chunk_idx,
                fs_reads_semaphore.clone(),
                files_handles_for_piece.clone(),
                piece_sizer,
            )
            .await
        {
            Ok(chunk) => chunk,
            Err(e) => {
                let _ = read_responses_tx.send(ReadPieceBlockResponse {
                    request: read_request,
                    response: Err(e),
                });
                return;
            }
        };

        let chunk_begin_position_in_piece = chunk_idx * READ_CACHE_CHUNK_SIZE;
        let chunk_end_position_in_piece = chunk_begin_position_in_piece + chunk.len();
        let chunk_offset_begin =
            max(block_begin, chunk_begin_position_in_piece) - chunk_begin_position_in_piece;
        let chunk_offset_end =
            min(block_end, chunk_end_position_in_piece) - chunk_begin_position_in_piece;
        response.extend_from_slice(&chunk[chunk_offset_begin..chunk_offset_end]);
    }

    let _ = read_responses_tx.send(ReadPieceBlockResponse {
        request: read_request,
        response: Ok(response),
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

    if block_length == 0 {
        bail!("requested to read piece idx {piece_idx} with a zero lenght block");
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

use std::collections::{BTreeSet, HashMap};

use crate::persistence::{
    file_manager::{ShaCheckReadError, ShaCorruptedError, WritePieceBlockResponse},
    piece::Piece,
};

pub struct TorrentDataStatus {
    missing_pieces: BTreeSet<usize>, // ordered set of piece indexes yet to be completed
    incomplete_pieces: HashMap<usize, Piece>, // piece id -> piece with downloaded fragments
    wasted_bytes: usize,

    // immutable fields
    total_pieces: usize,
    normal_piece_length: u64,
    last_piece_length: u64,
}

impl TorrentDataStatus {
    pub fn new(
        piece_completion_status: Vec<bool>, // piece idx -> true if completed
        normal_piece_length: u64,
        last_piece_length: u64,
    ) -> TorrentDataStatus {
        let mut missing_pieces = BTreeSet::new();
        for (idx, present) in piece_completion_status.iter().enumerate() {
            if !present {
                missing_pieces.insert(idx);
            }
        }
        let total_pieces = piece_completion_status.len();
        TorrentDataStatus {
            missing_pieces,
            incomplete_pieces: HashMap::new(),
            wasted_bytes: 0,
            total_pieces,
            normal_piece_length,
            last_piece_length,
        }
    }

    pub fn current_piece_completion_status(&self) -> Vec<bool> {
        let mut completed = vec![false; self.total_pieces];
        for idx in 0..completed.len() {
            completed[idx] = self.piece_is_completed(idx);
        }
        completed
    }

    pub fn piece_is_completed(&self, idx: usize) -> bool {
        !self.missing_pieces.contains(&idx)
    }

    pub fn num_pieces(&self) -> usize {
        self.total_pieces
    }

    pub fn completed_pieces(&self) -> usize {
        self.total_pieces - self.missing_pieces.len()
    }

    pub fn bytes_left(&self) -> u64 {
        let mut left = self.missing_pieces.len() as u64 * self.normal_piece_length;
        if self.missing_pieces.contains(&(self.total_pieces - 1)) {
            // last piece yet to be downloaded, adjust its size
            left = left - self.normal_piece_length + self.last_piece_length;
        }
        left
    }
    pub fn wasted_bytes(&self) -> usize {
        self.wasted_bytes
    }

    pub fn completed(&self) -> bool {
        self.missing_pieces.is_empty()
    }

    pub fn incomplete_pieces(&self) -> &HashMap<usize, Piece> {
        &self.incomplete_pieces
    }
    pub fn missing_pieces(&self) -> &BTreeSet<usize> {
        &self.missing_pieces
    }
    pub fn piece_length(&self, piece_idx: usize) -> u64 {
        if piece_idx == self.total_pieces - 1 {
            return self.last_piece_length;
        }
        self.normal_piece_length
    }

    pub fn update(&mut self, write_piece_block_response: &WritePieceBlockResponse) {
        match &write_piece_block_response.response {
            Ok(updates) => {
                self.wasted_bytes += updates.wasted_bytes;

                if updates.piece_is_completed {
                    self.missing_pieces
                        .remove(&write_piece_block_response.request.piece_idx);
                    self.incomplete_pieces
                        .remove(&write_piece_block_response.request.piece_idx);
                } else {
                    if let Some(written) = updates.written.as_ref() {
                        self.incomplete_pieces
                            .entry(write_piece_block_response.request.piece_idx)
                            .or_insert(Piece::new(written.piece_len))
                            .add_fragment(
                                written.block_begin,
                                written.block_begin + written.data_len - 1,
                            );
                    };
                }
            }
            Err(e) => {
                if e.downcast_ref::<ShaCorruptedError>().is_some()
                    || e.downcast_ref::<ShaCheckReadError>().is_some()
                {
                    // we could not verify the whole piece, wipe current download status and start over
                    self.incomplete_pieces
                        .remove(&write_piece_block_response.request.piece_idx);
                }
            }
        }
    }
}

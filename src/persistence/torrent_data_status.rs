use std::collections::{BTreeSet, HashMap};

use crate::persistence::{
    file_manager::{ShaCheckReadError, ShaCorruptedError, WritePieceBlockResponse},
    piece::Piece,
};

pub struct TorrentDataStatus {
    piece_completion_status: Vec<bool>, // piece identified by position in array -> download completed / incomplete TODO WE CAN DERIVE THIS
    missing_pieces: BTreeSet<usize>,    // ordered set of piece indexes yet to be completed
    incomplete_pieces: HashMap<usize, Piece>, // piece id -> piece with downloaded fragments
    wasted_bytes: usize,

    // immutable fields
    total_pieces: usize,
    normal_piece_length: u64,
    last_piece_lenght: u64,
}

impl TorrentDataStatus {
    pub fn new(
        piece_completion_status: Vec<bool>,
        normal_piece_length: u64,
        last_piece_lenght: u64,
    ) -> TorrentDataStatus {
        let mut missing_pieces = BTreeSet::new();
        for (idx, present) in piece_completion_status.iter().enumerate() {
            if !present {
                missing_pieces.insert(idx);
            }
        }
        let total_pieces = piece_completion_status.len();
        TorrentDataStatus {
            piece_completion_status,
            missing_pieces,
            incomplete_pieces: HashMap::new(),
            wasted_bytes: 0,
            total_pieces,
            normal_piece_length,
            last_piece_lenght,
        }
    }

    // TODO OPTIMIZE THIS
    pub fn current_piece_completion_status(&self) -> Vec<bool> {
        self.piece_completion_status.clone()
    }

    pub fn piece_completion_status(&self, idx: usize) -> bool {
        self.piece_completion_status[idx]
    }

    pub fn num_pieces(&self) -> usize {
        self.total_pieces
    }

    pub fn completed_pieces(&self) -> usize {
        self.total_pieces - self.missing_pieces.len()
    }

    pub fn bytes_left(&self) -> u64 {
        let mut left = self.missing_pieces.len() as u64 * self.normal_piece_length;
        if self.missing_pieces.get(&(self.total_pieces - 1)).is_none() {
            // last piece already downloaded, adjust its size
            left = left - self.normal_piece_length + self.last_piece_lenght;
        }
        left
    }
    pub fn wasted_bytes(&self) -> usize {
        self.wasted_bytes
    }

    pub fn completed(&self) -> bool {
        self.missing_pieces.len() == 0
    }

    pub fn incomplete_pieces(&self) -> &HashMap<usize, Piece> {
        &self.incomplete_pieces
    }
    pub fn missing_pieces(&self) -> &BTreeSet<usize> {
        &self.missing_pieces
    }
    pub fn piece_length(&self, piece_idx: usize) -> u64 {
        if piece_idx == self.total_pieces - 1 {
            return self.last_piece_lenght as u64;
        }
        return self.normal_piece_length as u64;
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
                    self.piece_completion_status[write_piece_block_response.request.piece_idx] =
                        true;
                } else {
                    self.incomplete_pieces.insert(
                        write_piece_block_response.request.piece_idx,
                        updates
                            .incomplete_piece
                            .clone()
                            .expect("this is always present if piece_is_completed == false"),
                    );
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

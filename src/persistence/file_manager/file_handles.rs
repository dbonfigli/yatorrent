use anyhow::Result;
use std::{
    collections::HashMap,
    fs::{self, File},
    path::PathBuf,
    sync::Arc,
};

use crate::persistence::file_manager::pieces_to_file_paths_mapper::PiecesToFilePathsMapper;

// A piece can span many files.
// The following is the list of files (file handles, start byte as offset of the piece, end byte as offset of the piece) a piece belong to, ordered.
pub type FileHandlesForPiece = Vec<(Arc<File>, u64, u64)>;

pub struct ReadFileHandles {
    file_handles: HashMap<PathBuf, Arc<File>>,
}

impl ReadFileHandles {
    pub fn new() -> ReadFileHandles {
        ReadFileHandles {
            file_handles: HashMap::new(),
        }
    }

    pub fn get_file(&mut self, file_path: &PathBuf) -> Result<Arc<File>> {
        // this will fail if the file does not exist, but we have a gate to prevent this if we know we don't have it
        if !self.file_handles.contains_key(file_path) {
            let f = File::options().read(true).open(file_path)?;
            self.file_handles.insert(file_path.clone(), Arc::new(f));
        }
        Ok(self
            .file_handles
            .get(file_path)
            .expect("file is present since we fetched it or inserted if missing")
            .clone())
    }
}

pub struct WriteFileHandles {
    file_handles: HashMap<PathBuf, Arc<File>>,
}

impl WriteFileHandles {
    pub fn new() -> WriteFileHandles {
        WriteFileHandles {
            file_handles: HashMap::new(),
        }
    }

    pub fn get_file(&mut self, file_path: &PathBuf) -> Result<Arc<File>> {
        if !self.file_handles.contains_key(file_path) {
            if let Some(dir) = file_path.parent() {
                fs::create_dir_all(dir)?;
            }
            let f = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(file_path)?;
            self.file_handles.insert(file_path.clone(), Arc::new(f));
        }
        Ok(self
            .file_handles
            .get(file_path)
            .expect("file is present since we fetched it or inserted if missing")
            .clone())
    }
}

pub fn get_files_for_piece_for_r(
    pieces_to_file_paths_mapper: Arc<PiecesToFilePathsMapper>,
    read_file_handles: &mut ReadFileHandles,
    piece_idx: usize,
) -> Result<FileHandlesForPiece> {
    let mut file_handles_for_piece = Vec::new();
    for (file_path, start, end) in pieces_to_file_paths_mapper.get(piece_idx).iter() {
        let f = read_file_handles.get_file(file_path)?;
        file_handles_for_piece.push((f, *start, *end));
    }
    Ok(file_handles_for_piece)
}

pub fn get_files_for_piece_for_w(
    pieces_to_file_paths_mapper: Arc<PiecesToFilePathsMapper>,
    write_file_handles: &mut WriteFileHandles,
    piece_idx: usize,
) -> Result<FileHandlesForPiece> {
    let mut file_handles_for_piece = Vec::new();
    for (file_path, start, end) in pieces_to_file_paths_mapper.get(piece_idx).iter() {
        let f = write_file_handles.get_file(file_path)?;
        file_handles_for_piece.push((f, *start, *end));
    }
    Ok(file_handles_for_piece)
}

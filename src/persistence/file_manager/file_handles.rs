use anyhow::{Result, bail};
use std::{
    collections::HashMap,
    fs::{self, File},
    path::PathBuf,
    sync::Arc,
};

use super::SymlinkPathError;
use crate::persistence::file_manager::pieces_to_file_paths_mapper::PiecesToFilePathsMapper;

// A piece can span many files.
// The following is the list of files (file handles, start byte as offset of the piece, end byte as offset of the piece) a piece belong to, ordered.
pub type FileHandlesForPiece = Vec<(Arc<File>, u64, u64)>;

pub struct ReadFileHandles {
    file_handles: HashMap<PathBuf, Arc<File>>,
    pieces_to_file_paths_mapper: Arc<PiecesToFilePathsMapper>,
}

impl ReadFileHandles {
    pub fn new(pieces_to_file_paths_mapper: Arc<PiecesToFilePathsMapper>) -> ReadFileHandles {
        ReadFileHandles {
            file_handles: HashMap::new(),
            pieces_to_file_paths_mapper,
        }
    }

    fn get_file(&mut self, file_path: &PathBuf) -> Result<Arc<File>> {
        // this will fail if the file does not exist, but we have a gate to prevent this if we know we don't have it
        if !self.file_handles.contains_key(file_path) {
            if fs::symlink_metadata(file_path)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                bail!(SymlinkPathError {
                    path: file_path.clone()
                });
            }
            let f = File::options().read(true).open(file_path)?;
            self.file_handles.insert(file_path.clone(), Arc::new(f));
        }
        Ok(self
            .file_handles
            .get(file_path)
            .expect("file is present since we fetched it or inserted if missing")
            .clone())
    }

    pub fn get_files_for_piece(&mut self, piece_idx: usize) -> Result<FileHandlesForPiece> {
        let mut file_handles_for_piece = Vec::new();
        for (file_path, start, end) in self.pieces_to_file_paths_mapper.get(piece_idx).iter() {
            let f = self.get_file(file_path)?;
            file_handles_for_piece.push((f, *start, *end));
        }
        Ok(file_handles_for_piece)
    }
}

pub struct WriteFileHandles {
    file_handles: HashMap<PathBuf, Arc<File>>,
    pieces_to_file_paths_mapper: Arc<PiecesToFilePathsMapper>,
}

impl WriteFileHandles {
    pub fn new(pieces_to_file_paths_mapper: Arc<PiecesToFilePathsMapper>) -> WriteFileHandles {
        WriteFileHandles {
            file_handles: HashMap::new(),
            pieces_to_file_paths_mapper,
        }
    }

    fn get_file(&mut self, file_path: &PathBuf) -> Result<Arc<File>> {
        if !self.file_handles.contains_key(file_path) {
            if let Some(dir) = file_path.parent() {
                fs::create_dir_all(dir)?;
            }
            if fs::symlink_metadata(file_path)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                bail!(SymlinkPathError {
                    path: file_path.clone()
                });
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

    pub fn get_files_for_piece(&mut self, piece_idx: usize) -> Result<FileHandlesForPiece> {
        let mut file_handles_for_piece = Vec::new();
        for (file_path, start, end) in self.pieces_to_file_paths_mapper.get(piece_idx).iter() {
            let f = self.get_file(file_path)?;
            file_handles_for_piece.push((f, *start, *end));
        }
        Ok(file_handles_for_piece)
    }
}

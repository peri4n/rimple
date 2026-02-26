use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Mutex,
};

use log::{debug, info, trace};

use crate::file::{block_id::BlockId, page::Page};

pub struct FileManager {
    block_size: usize,
    path: PathBuf,
    is_new: bool,
    open_files: Mutex<HashMap<PathBuf, File>>,
}

impl FileManager {
    pub fn new(path: impl AsRef<Path>, block_size: usize) -> io::Result<Self> {
        debug!("Start to initialize file manager");
        let path_buf = path.as_ref().to_path_buf();
        let is_new = !path_buf.exists();

        if is_new {
            std::fs::create_dir_all(path)?;
        }

        trace!("Cleaning up temporary files in directory: {:?}", path_buf);
        for file in path_buf.read_dir()?.flatten() {
            let file_path = file.path();
            if file_path.to_str().is_some_and(|s| s.starts_with("temp")) {
                std::fs::remove_file(file_path)?;
            }
        }

        debug!("File manager initialization done");
        Ok(Self {
            block_size,
            path: path_buf,
            is_new,
            open_files: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn get_file(&self, file_path: &Path) -> io::Result<File> {
        debug!("Fetching file {:?}", file_path);
        let mut open_files = self
            .open_files
            .lock()
            .map_err(|_| io::Error::other("Failed to acquire open files lock"))?;

        if let Some(file) = open_files.get(file_path) {
            trace!("File was already in cache {:?}", file_path);
            return file.try_clone();
        }

        trace!("File not found in cache. Creating new: {:?}", file_path);
        let file = OpenOptions::new()
            .custom_flags(libc::O_SYNC)
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;

        open_files.insert(file_path.to_path_buf(), file.try_clone()?);
        Ok(file)
    }

    pub fn read(&self, block_id: &BlockId, page: &mut Page) -> io::Result<()> {
        let mut file = self.get_file(block_id.path())?;
        let offset = block_id.block_no() * self.block_size as u64;
        file.seek(std::io::SeekFrom::Start(offset))?;

        let buf = page.content_mut();
        file.read_exact(buf)?;
        Ok(())
    }

    pub fn write(&self, block_id: &BlockId, page: &Page) -> io::Result<()> {
        let mut file = self.get_file(block_id.path())?;
        let offset = block_id.block_no() * self.block_size as u64;
        file.seek(std::io::SeekFrom::Start(offset))?;

        let buf = page.content();
        file.write_all(buf)?;
        Ok(())
    }

    pub fn append_block(&self, path: &Path) -> io::Result<BlockId> {
        let new_block_id = BlockId::new(path.to_path_buf(), self.size(path));
        self.write(&new_block_id, &Page::with_size(self.block_size))?;

        Ok(new_block_id)
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn size(&self, path: &Path) -> u64 {
        self.get_file(path)
            .and_then(|f| f.metadata().map(|m| m.len() / self.block_size as u64))
            .unwrap_or(0)
    }

    pub fn has_blocks(&self, path: &Path) -> bool {
        self.size(path) > 0
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn create_a_new_database_directory() {
        let file_mgr = FileManager::new("test_db", 4096).expect("Failed to create FileManager");
        assert!(file_mgr.path().exists());
    }
}

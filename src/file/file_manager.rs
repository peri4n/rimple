use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Mutex,
};

use crate::file::{block_id::BlockId, page::Page};

pub struct FileManager {
    block_size: usize,
    path: PathBuf,
    is_new: bool,
    open_files: Mutex<HashMap<PathBuf, File>>,
}

impl FileManager {
    pub fn new(path: impl AsRef<Path>, block_size: usize) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let is_new = !path.exists();
        if is_new {
            std::fs::create_dir_all(&path).expect("Failed to create directory");
        }

        // clean up temporary files
        for file in path.read_dir().expect("Failed to read directory").flatten() {
            let file_path = file.file_name();
            if file_path.to_str().is_some_and(|s| s.starts_with("temp")) {
                std::fs::remove_file(file_path).expect("Failed to remove temporary file");
            }
        }
        Ok(Self {
            block_size,
            path,
            is_new,
            open_files: Mutex::new(HashMap::new()),
        })
    }

    fn get_file(&self, file_path: &Path) -> io::Result<File> {
        let open_files = self
            .open_files
            .lock()
            .map_err(|_| io::Error::other("Failed to acquire open files lock"))?;

        if let Some(file) = open_files.get(file_path) {
            return file.try_clone();
        }

        OpenOptions::new()
            .custom_flags(libc::O_SYNC)
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)
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

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
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

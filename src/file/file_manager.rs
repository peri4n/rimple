use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
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
    pub fn new(path: impl AsRef<Path>, block_size: usize) -> Self {
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
        Self {
            block_size,
            path,
            is_new,
            open_files: Mutex::new(HashMap::new()),
        }
    }

    fn get_file(&self, file_path: &Path) -> File {
        let mut open_files = self.open_files.lock().unwrap();
        open_files
            .entry(file_path.to_path_buf())
            .or_insert_with(|| {
                OpenOptions::new()
                    .custom_flags(libc::O_SYNC)
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(file_path)
                    .expect("Failed to open file")
            })
            .try_clone()
            .expect("Failed to clone file handle")
    }

    pub fn read(&self, block_id: &BlockId, page: &mut Page) {
        let mut file = self.get_file(block_id.path());
        let offset = block_id.block_no() * self.block_size as u64;
        file.seek(std::io::SeekFrom::Start(offset))
            .expect("Failed to seek in file");

        let buf = page.content_mut();
        file.read_exact(buf).expect("Failed to read from file");
    }

    pub fn write(&self, block_id: &BlockId, page: &Page) {
        let mut file = self.get_file(block_id.path());
        let offset = block_id.block_no() * self.block_size as u64;
        file.seek(std::io::SeekFrom::Start(offset))
            .expect("Failed to seek in file");

        let buf = page.content();
        file.write_all(buf).expect("Failed to write to file");
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
        let file_mgr = FileManager::new("test_db", 4096);
        assert!(file_mgr.path().exists());
    }

}

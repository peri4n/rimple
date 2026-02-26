use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Mutex,
};

use log::{debug, trace};

use crate::file::{BlockId, Page};

/// Manages file I/O operations with caching and block-based access.
///
/// The `FileManager` provides high-level operations for reading and writing
/// pages to disk files, with automatic file caching and synchronous I/O
/// to ensure data durability.
///
/// # Features
///
/// - **Block-based access**: All I/O operations work with fixed-size blocks
/// - **File caching**: Open files are cached to avoid repeated filesystem calls  
/// - **Synchronous I/O**: Uses `O_SYNC` flag to ensure data is written to disk
/// - **Automatic cleanup**: Removes temporary files on initialization
///
/// # Examples
///
/// ```
/// # use rimple::file::{FileManager, Page, BlockId};
/// # use std::path::PathBuf;
/// # use tempfile::tempdir;
/// # let tmp = tempdir().unwrap();
/// let fm = FileManager::new(&tmp, 4096).unwrap();
///
/// // Create and write a page
/// let mut page = Page::with_size(4096);
/// page.set_string(0, "Hello, world!").unwrap();
///
/// let block_id = fm.append_block(tmp.path().join("test.db").as_path()).unwrap();
/// fm.write(&block_id, &page).unwrap();
///
/// // Read the page back
/// let mut read_page = Page::with_size(4096);
/// fm.read(&block_id, &mut read_page).unwrap();
/// assert_eq!(read_page.get_string(0).unwrap(), "Hello, world!");
/// ```
pub struct FileManager {
    block_size: usize,
    open_files: Mutex<HashMap<PathBuf, File>>,
}

impl FileManager {
    /// Creates a new file manager for the specified directory and block size.
    ///
    /// # Arguments
    ///
    /// * `path` - The directory path where files will be managed
    /// * `block_size` - The fixed size of each block in bytes
    ///
    /// # Returns
    ///
    /// Returns a new `FileManager` instance on success.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be created or accessed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::FileManager;
    /// # use tempfile::tempdir;
    /// # let tmp = tempdir().unwrap();
    /// let fm = FileManager::new(&tmp, 4096).unwrap();
    /// assert_eq!(fm.block_size(), 4096);
    /// ```
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
            open_files: Mutex::new(HashMap::new()),
        })
    }

    /// Gets a file handle, using the cache or opening a new file if needed.
    ///
    /// Files are opened with `O_SYNC` flag for synchronous I/O to ensure
    /// data is immediately written to disk.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the file to open
    ///
    /// # Returns
    ///
    /// Returns a cloned file handle on success.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened or the cache lock fails.
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

    /// Reads a page from the specified block.
    ///
    /// # Arguments
    ///
    /// * `block_id` - The identifier of the block to read
    /// * `page` - The page buffer to read into (must be pre-allocated to block size)
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be accessed or read.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::{FileManager, Page, BlockId};
    /// # use std::path::PathBuf;
    /// # use tempfile::tempdir;
    /// # let tmp = tempdir().unwrap();
    /// # let fm = FileManager::new(&tmp, 4096).unwrap();
    /// # let block_id = BlockId::new(tmp.path().join("test.db"), 0);
    /// let mut page = Page::with_size(4096);
    /// // This would fail in practice since block doesn't exist yet
    /// // fm.read(&block_id, &mut page).unwrap();
    /// ```
    pub fn read(&self, block_id: &BlockId, page: &mut Page) -> io::Result<()> {
        let mut file = self.get_file(block_id.path())?;
        let offset = block_id.block_no() * self.block_size as u64;
        file.seek(std::io::SeekFrom::Start(offset))?;

        let buf = page.content_mut();
        file.read_exact(buf)?;
        Ok(())
    }

    /// Writes a page to the specified block.
    ///
    /// # Arguments
    ///
    /// * `block_id` - The identifier of the block to write
    /// * `page` - The page data to write
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be accessed or written.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::{FileManager, Page, BlockId};
    /// # use std::path::PathBuf;
    /// # use tempfile::tempdir;
    /// # let tmp = tempdir().unwrap();
    /// # let fm = FileManager::new(&tmp, 4096).unwrap();
    /// let mut page = Page::with_size(4096);
    /// page.set_string(0, "test data").unwrap();
    ///
    /// let block_id = BlockId::new(tmp.path().join("test.db"), 0);
    /// fm.write(&block_id, &page).unwrap();
    /// ```
    pub fn write(&self, block_id: &BlockId, page: &Page) -> io::Result<()> {
        let mut file = self.get_file(block_id.path())?;
        let offset = block_id.block_no() * self.block_size as u64;
        file.seek(std::io::SeekFrom::Start(offset))?;

        let buf = page.content();
        file.write_all(buf)?;
        Ok(())
    }

    /// Appends a new empty block to the specified file.
    ///
    /// # Arguments
    ///
    /// * `path` - The file to append to
    ///
    /// # Returns
    ///
    /// Returns the `BlockId` of the newly created block.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be accessed or extended.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::FileManager;
    /// # use tempfile::tempdir;
    /// # let tmp = tempdir().unwrap();
    /// # let fm = FileManager::new(&tmp, 4096).unwrap();
    /// let file_path = tmp.path().join("new_file.db");
    /// let block_id = fm.append_block(&file_path).unwrap();
    /// assert_eq!(block_id.block_no(), 0); // First block
    ///
    /// let second_block = fm.append_block(&file_path).unwrap();
    /// assert_eq!(second_block.block_no(), 1); // Second block
    /// ```
    pub fn append_block(&self, path: &Path) -> io::Result<BlockId> {
        let new_block_id = BlockId::new(path.to_path_buf(), self.size(path));
        self.write(&new_block_id, &Page::with_size(self.block_size))?;

        Ok(new_block_id)
    }

    /// Returns the configured block size.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::FileManager;
    /// # use tempfile::tempdir;
    /// # let tmp = tempdir().unwrap();
    /// let fm = FileManager::new(&tmp, 8192).unwrap();
    /// assert_eq!(fm.block_size(), 8192);
    /// ```
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Returns the number of blocks in the specified file.
    ///
    /// # Arguments
    ///
    /// * `path` - The file to measure
    ///
    /// # Returns
    ///
    /// The number of blocks, or 0 if the file cannot be accessed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::FileManager;
    /// # use tempfile::tempdir;
    /// # let tmp = tempdir().unwrap();
    /// # let fm = FileManager::new(&tmp, 4096).unwrap();
    /// let file_path = tmp.path().join("test.db");
    ///
    /// // New file has 0 blocks
    /// assert_eq!(fm.size(&file_path), 0);
    ///
    /// // After appending a block
    /// fm.append_block(&file_path).unwrap();
    /// assert_eq!(fm.size(&file_path), 1);
    /// ```
    pub fn size(&self, path: &Path) -> u64 {
        self.get_file(path)
            .and_then(|f| f.metadata().map(|m| m.len() / self.block_size as u64))
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_a_new_database_directory() {
        let tmp = tempfile::tempdir().expect("Failed to create temp dir");
        let fm = FileManager::new(&tmp, 4096);
        assert!(fm.is_ok());
    }
}

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Mutex,
};

use log::{debug, trace};

use crate::file::{PageId, Page};

/// Manages file I/O operations with caching and page-based access.
///
/// The `FileManager` provides high-level operations for reading and writing
/// pages to disk files, with automatic file caching and synchronous I/O
/// to ensure data durability.
///
/// # Features
///
/// - **Page-based access**: All I/O operations work with fixed-size pages
/// - **File caching**: Open files are cached to avoid repeated filesystem calls  
/// - **Synchronous I/O**: Uses `O_SYNC` flag to ensure data is written to disk
/// - **Automatic cleanup**: Removes temporary files on initialization
pub struct FileManager {
    page_size: usize,
    open_files: Mutex<HashMap<PathBuf, File>>,
}

impl FileManager {
    /// Creates a new file manager for the specified directory and page size.
    ///
    /// # Arguments
    ///
    /// * `path` - The directory path where files will be managed
    /// * `page_size` - The fixed size of each page in bytes
    ///
    /// # Returns
    ///
    /// Returns a new `FileManager` instance on success.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be created or accessed.
    pub fn new(path: impl AsRef<Path>, page_size: usize) -> io::Result<Self> {
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
            page_size,
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
    pub(crate) fn get_file(&self, file_path: &Path) -> anyhow::Result<File> {
        debug!("Fetching file {:?}", file_path);
        let mut open_files = self
            .open_files
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire file cache lock: {}", e))?;

        if let Some(file) = open_files.get(file_path) {
            trace!("File was already in cache {:?}", file_path);
            return Ok(file.try_clone()?);
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

    /// Reads a page from the specified page.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The identifier of the page to read
    /// * `page` - The page buffer to read into (must be pre-allocated to page size)
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be accessed or read.
    pub fn read(&self, page_id: &PageId, page: &mut Page) -> anyhow::Result<()> {
        let mut file = self.get_file(page_id.path())?;
        let offset = page_id.block_no() * self.page_size as u64;
        file.seek(std::io::SeekFrom::Start(offset))?;

        let buf = page.content_mut();
        file.read_exact(buf)?;
        Ok(())
    }

    /// Writes a page to the specified page.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The identifier of the page to write
    /// * `page` - The page data to write
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be accessed or written.
    pub fn write(&self, page_id: &PageId, page: &Page) -> anyhow::Result<()> {
        let mut file = self.get_file(page_id.path())?;
        let offset = page_id.block_no() * self.page_size as u64;
        file.seek(std::io::SeekFrom::Start(offset))?;

        let buf = page.content();
        file.write_all(buf)?;
        Ok(())
    }

    /// Appends a new empty page to the specified file.
    ///
    /// # Arguments
    ///
    /// * `path` - The file to append to
    ///
    /// # Returns
    ///
    /// Returns the `PageId` of the newly created page.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be accessed or extended.
    pub fn append_page(&self, path: &Path) -> anyhow::Result<PageId> {
        let new_page_id = PageId::new(path.to_path_buf(), self.size(path)?);
        self.write(&new_page_id, &Page::with_size(self.page_size))?;

        Ok(new_page_id)
    }

    /// Returns the configured page size.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Returns the number of pages in the specified file.
    ///
    /// # Arguments
    ///
    /// * `path` - The file to measure
    ///
    /// # Returns
    ///
    /// The number of pages, or 0 if the file cannot be accessed.
    pub fn size(&self, path: &Path) -> anyhow::Result<u64> {
        self.get_file(path).and_then(|f| {
            f.metadata()
                .map_err(|e| anyhow::anyhow!("Failed to get meta data of file: {}", e))
                .map(|m| m.len() / self.page_size as u64)
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use tempfile::TempDir;

    use super::*;

    pub(crate) fn temp_file_manager(page_size: usize) -> (FileManager, TempDir) {
        let tmp = tempfile::tempdir().expect("Failed to create temp dir");
        (
            FileManager::new(&tmp, page_size).expect("Failed to create FileManager"),
            tmp,
        )
    }

    #[test]
    fn create_a_new_database_directory() {
        // setup
        let (fm, _) = temp_file_manager(4096);

        // test + verify
        assert_eq!(fm.page_size(), 4096);
    }

    #[test]
    fn writing_a_single_page_and_reading_it_is_consistent() {
        // setup
        let (fm, tmp) = temp_file_manager(4096);
        let mut page = Page::with_size(4096);
        page.set_string(0, "Hello, world!").unwrap();
        page.set_integer(100, 42).unwrap();
        let path = tmp.path().join("pagefile");
        let page_id = PageId::new(path.clone(), 0);

        // verify
        assert!(fm.write(&page_id, &page).is_ok());
        assert_eq!(fm.size(&path).unwrap(), 1);

        // test
        let mut read_page = Page::with_size(4096);
        fm.read(&page_id, &mut read_page).unwrap();

        // verify
        assert_eq!(read_page.get_string(0).unwrap(), "Hello, world!");
        assert_eq!(read_page.get_integer(100).unwrap(), 42);
    }

    #[test]
    fn appending_pages_increases_file_size() {
        // setup
        let (fm, tmp) = temp_file_manager(4096);
        let page = Page::with_size(4096);
        let path = tmp.path().join("appendfile");
        fm.write(&PageId::new(path.clone(), 0), &page).unwrap();
        assert_eq!(fm.size(&path).unwrap(), 1);

        // test
        assert!(fm.append_page(&path).is_ok());

        // verify
        assert_eq!(fm.size(&path).unwrap(), 2);
    }

    #[test]
    fn reading_nonexistent_file_returns_error() {
        // setup
        let (fm, tmp) = temp_file_manager(4096);
        let path = tmp.path().join("nonexistent");
        let page_id = PageId::new(path, 0);
        let mut page = Page::with_size(4096);

        // test + verify
        assert!(fm.read(&page_id, &mut page).is_err());
    }
}

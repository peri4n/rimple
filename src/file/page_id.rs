use std::{
    fmt::Display,
    path::{Path, PathBuf},
};

/// Identifies a specific page within a file.
///
/// A `PageId` combines a file path with a page number to uniquely identify
/// a fixed-size page of data within the file system.
///
/// # Examples
///
/// ```
/// # use rimple::file::PageId;
/// # use std::path::PathBuf;
/// let page = PageId::new(PathBuf::from("/tmp/data.db"), 42);
/// assert_eq!(page.block_no(), 42);
/// assert_eq!(page.path(), std::path::Path::new("/tmp/data.db"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PageId {
    path: PathBuf,
    block_no: u64,
}

impl PageId {
    /// Creates a new page identifier.
    ///
    /// # Arguments
    ///
    /// * `path` - The file path containing the page
    /// * `block_no` - The zero-based page number within the file
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::PageId;
    /// # use std::path::PathBuf;
    /// let page = PageId::new(PathBuf::from("data.db"), 0);
    /// assert_eq!(page.block_no(), 0);
    /// ```
    pub fn new(path: PathBuf, block_no: u64) -> Self {
        Self { path, block_no }
    }

    /// Returns the file path for this page.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::PageId;
    /// # use std::path::PathBuf;
    /// let page = PageId::new(PathBuf::from("test.db"), 5);
    /// assert_eq!(page.path(), std::path::Path::new("test.db"));
    /// ```
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the page number within the file.
    ///
    /// Page numbers are zero-based, meaning the first page in a file is page 0.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::PageId;
    /// # use std::path::PathBuf;
    /// let page = PageId::new(PathBuf::from("test.db"), 10);
    /// assert_eq!(page.block_no(), 10);
    /// ```
    pub fn block_no(&self) -> u64 {
        self.block_no
    }
}

impl Display for PageId {
    /// Formats the page ID as "path/block_number" for debugging and logging.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::PageId;
    /// # use std::path::PathBuf;
    /// let page = PageId::new(PathBuf::from("data.db"), 42);
    /// assert_eq!(format!("{}", page), "\"data.db\"/42");
    /// ```
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}/{}", self.path(), self.block_no()))
    }
}

use std::{
    fmt::Display,
    path::{Path, PathBuf},
};

/// Identifies a specific block within a file.
///
/// A `BlockId` combines a file path with a block number to uniquely identify
/// a fixed-size block of data within the file system.
///
/// # Examples
///
/// ```
/// # use rimple::file::BlockId;
/// # use std::path::PathBuf;
/// let block = BlockId::new(PathBuf::from("/tmp/data.db"), 42);
/// assert_eq!(block.block_no(), 42);
/// assert_eq!(block.path(), std::path::Path::new("/tmp/data.db"));
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct BlockId {
    path: PathBuf,
    block_no: u64,
}

impl BlockId {
    /// Creates a new block identifier.
    ///
    /// # Arguments
    ///
    /// * `path` - The file path containing the block
    /// * `block_no` - The zero-based block number within the file
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::BlockId;
    /// # use std::path::PathBuf;
    /// let block = BlockId::new(PathBuf::from("data.db"), 0);
    /// assert_eq!(block.block_no(), 0);
    /// ```
    pub fn new(path: PathBuf, block_no: u64) -> Self {
        Self { path, block_no }
    }

    /// Returns the file path for this block.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::BlockId;
    /// # use std::path::PathBuf;
    /// let block = BlockId::new(PathBuf::from("test.db"), 5);
    /// assert_eq!(block.path(), std::path::Path::new("test.db"));
    /// ```
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the block number within the file.
    ///
    /// Block numbers are zero-based, meaning the first block in a file is block 0.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::BlockId;
    /// # use std::path::PathBuf;
    /// let block = BlockId::new(PathBuf::from("test.db"), 10);
    /// assert_eq!(block.block_no(), 10);
    /// ```
    pub fn block_no(&self) -> u64 {
        self.block_no
    }
}

impl Display for BlockId {
    /// Formats the block ID as "path/block_number" for debugging and logging.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::BlockId;
    /// # use std::path::PathBuf;
    /// let block = BlockId::new(PathBuf::from("data.db"), 42);
    /// assert_eq!(format!("{}", block), "\"data.db\"/42");
    /// ```
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}/{}", self.path(), self.block_no()))
    }
}

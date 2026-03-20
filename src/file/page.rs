use anyhow::Error;

/// A page represents a fixed-size block of bytes that can be read from or written to disk.
///
/// Pages provide methods to store and retrieve various data types in a binary format
/// suitable for persistence. All multi-byte values use big-endian byte ordering.
///
/// # Data Format
///
/// - **Integers**: Stored as 32-bit big-endian values
/// - **Byte arrays**: Stored with a 4-byte big-endian length prefix followed by the data
/// - **Strings**: Stored as byte arrays with UTF-8 encoding
///
/// # TODO
/// - Add support for other primitive types (e.g. i64, f32, f64, dates, etc.)
/// - Add support for null-terminated strings
///
/// # Examples
///
/// ```
/// # use rimple::file::Page;
/// let mut page = Page::with_size(1024);
/// page.set_string(0, "hello").unwrap();
/// assert_eq!(page.get_string(0).unwrap(), "hello");
/// ```
#[derive(Debug)]
pub struct Page {
    content: Vec<u8>,
}

/// Errors that can occur during page operations.
#[derive(thiserror::Error, Debug)]
pub enum PageError {
    /// Attempted to access data outside the bounds of the page.
    #[error("Attempted to access data outside the bounds of the page")]
    OutOfBounds,

    /// Data format is invalid for the requested operation.
    #[error("Data format is invalid for the requested operation")]
    InvalidData,

    /// Requested data size exceeds available page size.
    #[error("Requested data size exceeds available page size")]
    SizeExceeded {
        /// The requested size that caused the error.
        requested: usize,
        /// The available size in the page.
        available: usize,
    },
}

impl Page {
    /// Creates a new page with the provided byte content.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The byte content to initialize the page with
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::Page;
    /// let page = Page::with_bytes(&vec![1, 2, 3, 4]);
    /// assert_eq!(page.len(), 4);
    /// ```
    pub fn with_bytes(bytes: &[u8]) -> Self {
        Self {
            content: bytes.to_vec(),
        }
    }

    /// Creates a new page with the specified size, initialized with zeros.
    ///
    /// # Arguments
    ///
    /// * `size` - The size of the page in bytes
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::Page;
    /// let page = Page::with_size(1024);
    /// assert_eq!(page.len(), 1024);
    /// ```
    pub fn with_size(size: usize) -> Self {
        Self {
            content: vec![0; size],
        }
    }

    /// Reads a 32-bit signed integer from the page at the specified offset.
    ///
    /// The integer is stored in big-endian format.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset within the page to read from
    ///
    /// # Returns
    ///
    /// Returns the integer value on success.
    ///
    /// # Errors
    ///
    /// * `PageError::OutOfBounds` - If the offset + 4 bytes exceeds the page size
    /// * `PageError::InvalidData` - If the bytes cannot be converted to an integer
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::Page;
    /// let page = Page::with_bytes(&vec![0x00, 0x00, 0x00, 0x7F]);
    /// assert_eq!(page.get_integer(0).unwrap(), 127);
    /// ```
    pub fn get_integer(&self, offset: usize) -> anyhow::Result<i32> {
        self.assert_offset_within_bounds(offset, std::mem::size_of::<i32>())?;

        let bytes = &self.content[offset..offset + std::mem::size_of::<i32>()];
        bytes
            .try_into()
            .map(|arr: [u8; 4]| i32::from_be_bytes(arr))
            .map_err(|_| Error::new(PageError::InvalidData))
    }

    /// Writes a 32-bit signed integer to the page at the specified offset.
    ///
    /// The integer is stored in big-endian format.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset within the page to write to
    /// * `value` - The integer value to write
    ///
    /// # Errors
    ///
    /// * `PageError::OutOfBounds` - If the offset + 4 bytes exceeds the page size
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::Page;
    /// let mut page = Page::with_size(8);
    /// page.set_integer(0, 42).unwrap();
    /// assert_eq!(page.get_integer(0).unwrap(), 42);
    /// ```
    pub fn set_integer(&mut self, offset: usize, value: i32) -> anyhow::Result<()> {
        self.assert_offset_within_bounds(offset, std::mem::size_of::<i32>())?;

        self.content[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
        Ok(())
    }

    /// Reads a byte slice from the page at the specified offset.
    ///
    /// The byte data is stored with a 4-byte length prefix (big-endian) followed by the actual bytes.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset within the page to read from
    ///
    /// # Returns
    ///
    /// Returns a reference to the byte slice on success.
    ///
    /// # Errors
    ///
    /// * `PageError::OutOfBounds` - If the offset exceeds the page bounds
    /// * `PageError::InvalidData` - If the length prefix is negative or the total data exceeds page bounds
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::Page;
    /// let mut page = Page::with_size(16);
    /// page.set_bytes(0, b"hello").unwrap();
    /// assert_eq!(page.get_bytes(0).unwrap(), b"hello");
    /// ```
    pub fn get_bytes(&self, offset: usize) -> anyhow::Result<&[u8]> {
        self.assert_offset_within_bounds(offset, std::mem::size_of::<i32>())?;

        let length = self.get_integer(offset)?;
        let length = usize::try_from(length).map_err(|_| PageError::InvalidData)?;

        if offset + std::mem::size_of::<i32>() + length > self.content.len() {
            return Err(Error::new(PageError::InvalidData));
        }
        let start = offset + std::mem::size_of::<i32>();
        let end = start + length;
        Ok(&self.content[start..end])
    }

    /// Writes a byte slice to the page at the specified offset.
    ///
    /// The data is stored with a 4-byte length prefix (big-endian) followed by the actual bytes.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset within the page to write to
    /// * `bytes` - The byte slice to write
    ///
    /// # Errors
    ///
    /// * `PageError::OutOfBounds` - If the offset exceeds the page bounds
    /// * `PageError::SizeExceeded` - If the total data (length prefix + bytes) exceeds available space
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::Page;
    /// let mut page = Page::with_size(16);
    /// page.set_bytes(0, b"hello").unwrap();
    /// assert_eq!(page.get_bytes(0).unwrap(), b"hello");
    /// ```
    pub fn set_bytes(&mut self, offset: usize, bytes: &[u8]) -> anyhow::Result<()> {
        self.assert_offset_within_bounds(offset, std::mem::size_of::<i32>())?;

        let length = bytes.len();

        if offset + 4 + length > self.content.len() {
            return Err(Error::new(PageError::SizeExceeded {
                requested: offset + 4 + length,
                available: self.content.len(),
            }));
        }
        let _ = self.set_integer(offset, length as i32);
        self.content[offset + 4..offset + 4 + length].copy_from_slice(bytes);
        Ok(())
    }

    /// Reads a UTF-8 string from the page at the specified offset.
    ///
    /// The string data is stored with a 4-byte length prefix (big-endian) followed by UTF-8 bytes.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset within the page to read from
    ///
    /// # Returns
    ///
    /// Returns the decoded string on success.
    ///
    /// # Errors
    ///
    /// * `PageError::OutOfBounds` - If the offset exceeds the page bounds
    /// * `PageError::InvalidData` - If the length prefix is invalid or bytes are not valid UTF-8
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::Page;
    /// let mut page = Page::with_size(16);
    /// page.set_string(0, "hello").unwrap();
    /// assert_eq!(page.get_string(0).unwrap(), "hello");
    /// ```
    pub fn get_string(&self, offset: usize) -> anyhow::Result<String> {
        self.get_bytes(offset).and_then(|bytes| {
            std::str::from_utf8(bytes)
                .map(|s| s.to_string())
                .map_err(|_| Error::new(PageError::InvalidData))
        })
    }

    /// Writes a UTF-8 string to the page at the specified offset.
    ///
    /// The string is stored with a 4-byte length prefix (big-endian) followed by UTF-8 bytes.
    ///
    /// # Arguments
    ///
    /// * `offset` - The byte offset within the page to write to
    /// * `s` - The string to write
    ///
    /// # Errors
    ///
    /// * `PageError::OutOfBounds` - If the offset exceeds the page bounds
    /// * `PageError::SizeExceeded` - If the total data (length prefix + UTF-8 bytes) exceeds available space
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::Page;
    /// let mut page = Page::with_size(16);
    /// page.set_string(0, "hello").unwrap();
    /// assert_eq!(page.get_string(0).unwrap(), "hello");
    /// ```
    pub fn set_string(&mut self, offset: usize, s: &str) -> anyhow::Result<()> {
        self.set_bytes(offset, s.as_bytes())
    }

    /// Returns an immutable reference to the page's byte content.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::Page;
    /// let page = Page::with_bytes(&vec![1, 2, 3, 4]);
    /// assert_eq!(page.content(), &[1, 2, 3, 4]);
    /// ```
    pub fn content(&self) -> &[u8] {
        &self.content
    }

    /// Returns a mutable reference to the page's byte content.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::Page;
    /// let mut page = Page::with_size(4);
    /// page.content_mut().copy_from_slice(&[1, 2, 3, 4]);
    /// assert_eq!(page.content(), &[1, 2, 3, 4]);
    /// ```
    pub fn content_mut(&mut self) -> &mut [u8] {
        &mut self.content
    }

    /// Returns the size of the page in bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rimple::file::Page;
    /// let page = Page::with_size(1024);
    /// assert_eq!(page.len(), 1024);
    /// ```
    pub fn len(&self) -> usize {
        self.content.len()
    }

    /// Checks if the specified offset and size are within the page bounds.
    ///
    /// # Arguments
    ///
    /// * `offset` - The starting offset to check
    /// * `size` - The size of data to check
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the range is valid, otherwise `PageError::OutOfBounds`.
    fn assert_offset_within_bounds(&self, offset: usize, size: usize) -> anyhow::Result<()> {
        if offset + size > self.content.len() {
            Err(Error::new(PageError::OutOfBounds))
        } else {
            Ok(())
        }
    }

    /// Calculates the maximum storage space required for a string.
    ///
    /// This includes the 4-byte length prefix plus the string's byte length.
    ///
    /// # Arguments
    ///
    /// * `s` - The string to calculate space for
    ///
    /// # Returns
    ///
    /// The total bytes required to store the string with its length prefix.
    pub(crate) fn max_length(s: &str) -> usize {
        std::mem::size_of::<i32>() + s.len()
    }
}

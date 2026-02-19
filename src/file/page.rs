/// TODO
/// - Add support for other primitive types (e.g. i64, f32, f64, dates, etc.)
/// - Add support for null-terminated strings
///

/// A Page represents a fixed-size block of bytes that can be read from or written to disk.
#[derive(Debug)]
pub struct Page {
    content: Vec<u8>,
}

#[derive(thiserror::Error, Debug)]
pub enum PageError {
    #[error("Attempted to access data outside the bounds of the page")]
    OutOfBounds,

    #[error("Data format is invalid for the requested operation")]
    InvalidData,

    #[error("Requested data size exceeds available page size")]
    SizeExceeded { requested: usize, available: usize },
}

type PageResult<T> = Result<T, PageError>;

impl Page {
    pub fn with_bytes(bytes: Vec<u8>) -> Self {
        Self { content: bytes }
    }

    pub fn with_size(size: usize) -> Self {
        Self {
            content: vec![0; size],
        }
    }

    pub fn get_integer(&self, offset: usize) -> PageResult<i32> {
        self.assert_offset_within_bounds(offset, std::mem::size_of::<i32>())?;

        let bytes = &self.content[offset..offset + std::mem::size_of::<i32>()];
        bytes
            .try_into()
            .map(|arr: [u8; 4]| i32::from_be_bytes(arr))
            .map_err(|_| PageError::InvalidData)
    }

    // I don't believe this method can fail with InvalidData since we're just writing bytes, and
    // the bound check is covered by usize.
    pub fn set_integer(&mut self, offset: usize, value: i32) -> PageResult<()> {
        self.assert_offset_within_bounds(offset, std::mem::size_of::<i32>())?;

        self.content[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
        Ok(())
    }

    pub fn get_bytes(&self, offset: usize) -> PageResult<Vec<u8>> {
        self.assert_offset_within_bounds(offset, std::mem::size_of::<i32>())?;

        let length = self.get_integer(offset)?;
        let length = usize::try_from(length).map_err(|_| PageError::InvalidData)?;

        if offset + std::mem::size_of::<i32>() + length > self.content.len() {
            return Err(PageError::InvalidData);
        }
        let start = offset + std::mem::size_of::<i32>();
        let end = start + length;
        Ok(self.content[start..end].to_vec())
    }

    pub fn set_bytes(&mut self, offset: usize, bytes: &[u8]) -> PageResult<()> {
        self.assert_offset_within_bounds(offset, std::mem::size_of::<i32>())?;

        let length = bytes.len();

        if offset + 4 + length > self.content.len() {
            return Err(PageError::SizeExceeded {
                requested: offset + 4 + length,
                available: self.content.len(),
            });
        }
        let _ = self.set_integer(offset, length as i32);
        self.content[offset + 4..offset + 4 + length].copy_from_slice(bytes);
        Ok(())
    }

    pub fn get_string(&self, offset: usize) -> PageResult<String> {
        self.get_bytes(offset)
            .and_then(|bytes| String::from_utf8(bytes).map_err(|_| PageError::InvalidData))
    }

    pub fn set_string(&mut self, offset: usize, s: &str) -> PageResult<()> {
        self.set_bytes(offset, s.as_bytes())
    }

    pub fn content(&self) -> &[u8] {
        &self.content
    }

    pub fn content_mut(&mut self) -> &mut [u8] {
        &mut self.content
    }

    pub fn len(&self) -> usize {
        self.content.len()
    }

    fn assert_offset_within_bounds(&self, offset: usize, size: usize) -> PageResult<()> {
        if offset + size > self.content.len() {
            Err(PageError::OutOfBounds)
        } else {
            Ok(())
        }
    }

    pub(crate) fn max_length(str: &str) -> usize {
        std::mem::size_of::<i32>() + str.len()
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn with_size_initializes_zeroes_and_len() {
        let p = Page::with_size(8);
        assert_eq!(p.len(), 8);
        assert!(p.content().iter().all(|&b| b == 0));
    }

    #[test]
    fn with_bytes_get_integer_big_endian() {
        let p = Page::with_bytes(vec![0x00, 0x00, 0x00, 0x7F]);
        let res = p.get_integer(0);
        assert!(matches!(res, Ok(n) if n == 127));
    }

    #[test]
    fn set_get_integer_roundtrip_and_bytes() {
        let mut p = Page::with_size(8);
        let v: i32 = -123456;
        assert!(matches!(p.set_integer(0, v), Ok(())));
        assert!(matches!(p.get_integer(0), Ok(n) if n == v));
        assert_eq!(&p.content()[0..4], &v.to_be_bytes());
    }

    #[test]
    fn set_get_bytes_roundtrip() {
        let mut p = Page::with_size(16);
        assert!(matches!(p.set_bytes(0, b"abc"), Ok(())));
        let res = p.get_bytes(0);
        assert!(matches!(res, Ok(bytes) if bytes == b"abc".to_vec()));
        assert_eq!(&p.content()[0..4], &3i32.to_be_bytes());
        assert_eq!(&p.content()[4..7], b"abc");
    }

    #[test]
    fn set_get_string_roundtrip() {
        let mut p = Page::with_size(16);
        assert!(matches!(p.set_string(0, "hello"), Ok(())));
        assert!(matches!(p.get_string(0), Ok(s) if s == "hello"));
    }

    #[test]
    fn out_of_bounds_on_get_integer() {
        let p = Page::with_size(8);
        let res = p.get_integer(6);
        assert!(matches!(res, Err(PageError::OutOfBounds)));
    }

    #[test]
    fn out_of_bounds_on_set_integer() {
        let mut p = Page::with_size(8);
        let res = p.set_integer(6, 1);
        assert!(matches!(res, Err(PageError::OutOfBounds)));
    }

    #[test]
    fn out_of_bounds_on_get_bytes_offset() {
        let p = Page::with_size(8);
        let res = p.get_bytes(6);
        assert!(matches!(res, Err(PageError::OutOfBounds)));
    }

    #[test]
    fn size_exceeded_on_set_bytes() {
        let mut p = Page::with_size(5);
        let res = p.set_bytes(0, b"abcdef");
        match res {
            Err(PageError::SizeExceeded {
                requested,
                available,
            }) => {
                assert_eq!(requested, 10);
                assert_eq!(available, 5);
            }
            _ => panic!("expected SizeExceeded error"),
        }
    }

    #[test]
    fn invalid_utf8_get_string() {
        let mut p = Page::with_size(16);
        assert!(matches!(p.set_bytes(0, &[0xFF, 0xFE, 0xFA]), Ok(())));
        let res = p.get_string(0);
        assert!(matches!(res, Err(PageError::InvalidData)));
    }

    #[test]
    fn content_accessors_and_len() {
        let mut p = Page::with_size(4);
        let buf = p.content_mut();
        buf.copy_from_slice(&[1, 2, 3, 4]);
        assert_eq!(p.len(), 4);
        assert!(matches!(p.get_integer(0), Ok(n) if n == 0x01020304));
    }

    #[test]
    fn get_bytes_length_overflow_returns_error() {
        let mut p = Page::with_size(8);
        // Write a length larger than available space (10 > 8)
        assert!(matches!(p.set_integer(0, 10), Ok(())));
        let res = p.get_bytes(0);
        assert!(matches!(res, Err(PageError::InvalidData)));
    }

    #[test]
    fn get_bytes_negative_length_invalid_data() {
        let mut p = Page::with_size(8);
        // Negative stored length should be treated as invalid
        assert!(matches!(p.set_integer(0, -1), Ok(())));
        let res = p.get_bytes(0);
        assert!(matches!(res, Err(PageError::InvalidData)));
    }
}

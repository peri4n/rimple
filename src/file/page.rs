#[derive(Debug)]
pub struct Page {
    content: Vec<u8>,
}

impl Page {
    pub fn with_bytes(bytes: Vec<u8>) -> Self {
        Self { content: bytes }
    }

    pub fn with_size(size: usize) -> Self {
        Self {
            content: vec![0; size],
        }
    }

    pub fn get_integer(&self, offset: usize) -> i32 {
        let bytes = &self.content[offset..offset + 4];
        i32::from_be_bytes(bytes.try_into().expect("slice with incorrect length"))
    }

    pub fn set_integer(&mut self, offset: usize, value: i32) {
        self.content[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
    }

    pub fn get_bytes(&self, offset: usize) -> Vec<u8> {
        let length = i32::from_be_bytes(
            self.content[offset..offset + 4]
                .try_into()
                .expect("slice with incorrect length"),
        ) as usize;
        self.content[offset + 4..offset + 4 + length].to_vec()
    }

    pub fn set_bytes(&mut self, offset: usize, bytes: &[u8]) {
        let length = bytes.len();
        self.set_integer(offset, length as i32);
        self.content[offset + 4..offset + 4 + length].copy_from_slice(bytes);
    }

    pub fn get_string(&self, offset: usize) -> String {
        String::from_utf8(self.get_bytes(offset)).expect("unable to parse string")
    }

    pub fn set_string(&mut self, offset: usize, s: &str) {
        self.set_bytes(offset, s.as_bytes());
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
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn with_bytes_preserves_content() {
        let data = vec![1, 2, 3, 4, 5];
        let page = Page::with_bytes(data.clone());
        assert_eq!(page.len(), data.len());
        assert_eq!(page.content(), &data[..]);
    }

    #[test]
    fn with_size_initializes_zeros() {
        let page = Page::with_size(16);
        assert_eq!(page.len(), 16);
        assert!(page.content().iter().all(|&b| b == 0));
    }

    #[test]
    fn integer_roundtrip_at_offsets() {
        let mut page = Page::with_size(64);
        let cases = [(0usize, 0i32), (4, 42), (8, -12345678), (28, i32::MAX)];

        for (off, val) in cases {
            page.set_integer(off, val);
            assert_eq!(page.get_integer(off), val);
        }
    }

    #[test]
    fn integer_is_big_endian() {
        let mut page = Page::with_size(8);
        page.set_integer(0, 0x0102_0304);
        assert_eq!(&page.content()[0..4], &[0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    #[should_panic]
    fn integer_get_out_of_bounds_panics() {
        let page = Page::with_size(3);
        let _ = page.get_integer(0);
    }

    #[test]
    #[should_panic]
    fn integer_set_out_of_bounds_panics() {
        let mut page = Page::with_size(3);
        page.set_integer(0, 7);
    }

    #[test]
    fn bytes_roundtrip_length_prefixed_4byte() {
        let mut page = Page::with_size(64);
        let off = 10usize;
        let payload = b"hello"; // len = 5
        page.set_bytes(off, payload);

        let len_bytes = (payload.len() as i32).to_be_bytes();
        assert_eq!(&page.content()[off..off + 4], &len_bytes);
        assert_eq!(&page.content()[off + 4..off + 4 + payload.len()], payload);
        assert_eq!(page.get_bytes(off), payload);
    }

    #[test]
    fn bytes_zero_length() {
        let mut page = Page::with_size(16);
        let off = 2usize;
        let empty: &[u8] = b"";
        page.set_bytes(off, empty);
        assert_eq!(&page.content()[off..off + 4], &[0, 0, 0, 0]);
        assert!(page.get_bytes(off).is_empty());
    }

    #[test]
    #[should_panic]
    fn bytes_get_out_of_bounds_panics() {
        let mut page = Page::with_size(4);
        let buf = page.content_mut();
        buf[0..4].copy_from_slice(&(10i32).to_be_bytes());
        let _ = Page::with_bytes(buf.to_vec()).get_bytes(0);
    }

    #[test]
    #[should_panic]
    fn bytes_set_out_of_bounds_panics() {
        let mut page = Page::with_size(4);
        page.set_bytes(2, b"abcd");
    }

    #[test]
    fn string_roundtrip() {
        let mut page = Page::with_size(64);
        let off = 0usize;
        let s = "rimple";
        page.set_string(off, s);
        assert_eq!(page.get_string(off), s);
    }

    #[test]
    fn content_mut_allows_mutation() {
        let mut page = Page::with_size(4);
        {
            let buf = page.content_mut();
            buf[2] = 99;
        }
        assert_eq!(page.content()[2], 99);
    }
}

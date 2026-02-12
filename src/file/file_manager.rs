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
        for entry in path.read_dir().expect("Failed to read directory") {
            if let Ok(file) = entry {
                let file_path = file.path();
                if file_path.starts_with("temp") {
                    std::fs::remove_file(file_path).expect("Failed to remove temporary file");
                }
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

    pub fn read(&self, block_id: &BlockId, page: &mut Page) -> Vec<u8> {
        let mut file = self.get_file(block_id.path());
        let offset = block_id.id() * self.block_size as u64;
        file.seek(std::io::SeekFrom::Start(offset))
            .expect("Failed to seek in file");

        let buf = page.content_mut();
        file.read_exact(buf).expect("Failed to read from file");
        buf.to_vec()
    }

    pub fn write(&self, block_id: &BlockId, page: &Page) {
        let mut file = self.get_file(block_id.path());
        let offset = block_id.id() * self.block_size as u64;
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
    use std::fs;
    use std::io::SeekFrom;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir() -> PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("rimple_fm_test_{}", nanos));
        p
    }

    fn make_dir() -> PathBuf {
        let dir = unique_temp_dir();
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn make_file(path: &Path) {
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
    }

    #[test]
    fn new_creates_dir_and_sets_is_new() {
        let dir = unique_temp_dir();
        assert!(!dir.exists());
        let fm = FileManager::new(&dir, 4096);
        assert!(dir.exists());
        assert!(fm.is_new());
        assert_eq!(fm.block_size(), 4096);
        assert_eq!(fm.path(), &dir);
        // cleanup
        drop(fm);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn new_existing_dir_sets_is_new_false() {
        let dir = make_dir();
        let fm = FileManager::new(&dir, 1024);
        assert!(!fm.is_new());
        assert_eq!(fm.block_size(), 1024);
        assert_eq!(fm.path(), &dir);
        drop(fm);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_reads_expected_block_at_offset() {
        let dir = make_dir();
        let fm = FileManager::new(&dir, 128);
        let data_file = dir.join("data.bin");
        make_file(&data_file);

        // Prepare two blocks with distinct patterns at block 0 and block 2
        let block_size = fm.block_size();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&data_file)
            .unwrap();

        let mut block0 = vec![0u8; block_size];
        for (i, b) in block0.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(3);
        }
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&block0).unwrap();

        let mut block2 = vec![0u8; block_size];
        for (i, b) in block2.iter_mut().enumerate() {
            *b = (255u8).wrapping_sub((i as u8).wrapping_mul(2));
        }
        let offset_block2 = (2u64) * (block_size as u64);
        file.seek(SeekFrom::Start(offset_block2)).unwrap();
        file.write_all(&block2).unwrap();
        drop(file);

        // Read block 0
        let mut page0 = Page::with_size(block_size);
        let id0 = BlockId::new(data_file.clone(), 0);
        let buf0 = fm.read(&id0, &mut page0);
        assert_eq!(buf0, block0);
        assert_eq!(page0.content(), &block0[..]);

        // Read block 2
        let mut page2 = Page::with_size(block_size);
        let id2 = BlockId::new(data_file.clone(), 2);
        let buf2 = fm.read(&id2, &mut page2);
        assert_eq!(buf2, block2);
        assert_eq!(page2.content(), &block2[..]);

        drop(fm);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_and_read_roundtrip_multiple_blocks() {
        let dir = make_dir();
        let fm = FileManager::new(&dir, 64);
        let data_file = dir.join("wr.bin");
        make_file(&data_file);

        // Prepare two different payloads
        let bs = fm.block_size();
        let mut p0 = Page::with_size(bs);
        for (i, b) in p0.content_mut().iter_mut().enumerate() {
            *b = (i as u8).wrapping_add(7);
        }
        let mut p3 = Page::with_size(bs);
        for (i, b) in p3.content_mut().iter_mut().enumerate() {
            *b = (255u8).wrapping_sub(i as u8);
        }

        // Write block 0 and 3
        let id0 = BlockId::new(data_file.clone(), 0);
        fm.write(&id0, &p0);
        let id3 = BlockId::new(data_file.clone(), 3);
        fm.write(&id3, &p3);

        // Read back and verify roundtrip
        let mut r0 = Page::with_size(bs);
        let back0 = fm.read(&id0, &mut r0);
        assert_eq!(back0, p0.content());
        assert_eq!(r0.content(), p0.content());

        let mut r3 = Page::with_size(bs);
        let back3 = fm.read(&id3, &mut r3);
        assert_eq!(back3, p3.content());
        assert_eq!(r3.content(), p3.content());

        drop(fm);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_overwrites_existing_content() {
        let dir = make_dir();
        let fm = FileManager::new(&dir, 32);
        let data_file = dir.join("overwrite.bin");
        make_file(&data_file);

        let id = BlockId::new(data_file.clone(), 1);
        let mut first = Page::with_size(fm.block_size());
        first.content_mut().fill(0xAA);
        fm.write(&id, &first);

        let mut second = Page::with_size(fm.block_size());
        second.content_mut().fill(0x55);
        fm.write(&id, &second);

        let mut read_back = Page::with_size(fm.block_size());
        let buf = fm.read(&id, &mut read_back);
        assert_eq!(buf, second.content());
        assert_eq!(read_back.content(), second.content());

        drop(fm);
        fs::remove_dir_all(&dir).ok();
    }
}

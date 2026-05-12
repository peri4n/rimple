use std::sync::Arc;

use crate::file::{PageId, FileManager, Page};

pub(crate) struct LogIterator {
    file_manager: Arc<FileManager>,
    current_position: usize,
    blk: PageId,
    page: Page,
    boundary: i32,
}

impl LogIterator {
    pub fn new(file_manager: Arc<FileManager>, blk: PageId) -> anyhow::Result<Self> {
        let page_size = file_manager.page_size();
        let mut page = Page::with_size(page_size);
        file_manager.read(&blk, &mut page)?;
        let boundary = page.get_integer(0)?;
        let current_position = boundary as usize;

        Ok(Self {
            file_manager,
            page,
            blk,
            boundary,
            current_position,
        })
    }
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_position >= self.file_manager.page_size() && self.blk.block_no() == 0 {
            return None;
        }

        if self.current_position == self.file_manager.page_size() {
            self.blk = PageId::new(self.blk.path().to_path_buf(), self.blk.block_no() - 1);
            self.file_manager.read(&self.blk, &mut self.page).ok()?;
            self.boundary = self.page.get_integer(0).ok()?;
            self.current_position = self.boundary as usize;
        }

        let record = self.page.get_bytes(self.current_position).ok()?.to_vec();
        self.current_position += std::mem::size_of::<i32>() + record.len();
        Some(record)
    }
}

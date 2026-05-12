use std::{
    io,
    sync::{Arc, Mutex},
};

use crate::{
    file::{PageId, FileManager, Page},
    log::manager::LogManager,
};

/// A buffer is a page of main memory that can hold the contents of a disk page.
pub struct Buffer {
    file_manager: Arc<FileManager>,

    log_manager: Arc<Mutex<LogManager>>,

    /// The contents of the buffer, represented as a Page.
    page: Page,

    /// The identifier of the disk page currently stored in this buffer, if any.
    page_id: Option<PageId>,

    /// Number of times this buffer has been pinned (i.e., how many clients are currently using it).
    pins: usize,

    /// The transaction number that last modified this buffer, if any.
    txnum: i32,

    /// The log sequence number (LSN) of the most recent log record that modified this buffer, if any.
    lsn: i32,
}

impl Buffer {
    pub fn new(file_manager: Arc<FileManager>, log_manager: Arc<Mutex<LogManager>>) -> Self {
        Self {
            file_manager: file_manager.clone(),
            log_manager: log_manager.clone(),
            page: Page::with_size(file_manager.page_size()),
            page_id: None,
            pins: 0,
            txnum: -1,
            lsn: -1,
        }
    }

    pub fn contents(&self) -> &Page {
        &self.page
    }

    // For in-place modifications
    pub fn contents_mut(&mut self) -> &mut Page {
        &mut self.page
    }

    pub fn page_id(&self) -> Option<&PageId> {
        self.page_id.as_ref()
    }

    pub fn set_modified(&mut self, txnum: i32, lsn: i32) {
        self.txnum = txnum;
        if lsn >= 0 {
            self.lsn = lsn;
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_txn(&self) -> i32 {
        self.txnum
    }

    pub fn pin(&mut self) {
        self.pins += 1;
    }

    pub fn unpin(&mut self) {
        if self.pins > 0 {
            self.pins -= 1;
        }
    }

    pub(crate) fn assign_to_page(&mut self, page_id: &PageId) -> anyhow::Result<()> {
        self.flush()?;
        self.page_id = Some(page_id.clone());
        self.file_manager.read(page_id, &mut self.page)?;
        self.pins = 0;

        Ok(())
    }

    pub(crate) fn flush(&mut self) -> anyhow::Result<()> {
        if self.txnum >= 0 {
            let mut log_manager = self
                .log_manager
                .lock()
                .map_err(|_| io::Error::other("Failed to acquire log manager lock"))?;
            log_manager.flush(self.lsn as usize)?;
            self.file_manager
                .write(self.page_id.as_ref().unwrap(), &self.page)?;
        }

        Ok(())
    }
}

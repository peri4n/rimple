use std::{
    io,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::bail;
use log::{debug, trace};

use crate::{
    buffer::buffer::Buffer,
    file::{PageId, FileManager},
    log::manager::LogManager,
};

#[derive(thiserror::Error, Debug)]
pub enum BufferError {
    #[error("Buffer pinning failed: {0}")]
    Timeout(String),
}

pub struct BufferManager {
    file_manager: Arc<FileManager>,
    log_manager: Arc<Mutex<LogManager>>,
    pool: Vec<Arc<Mutex<Buffer>>>,
    available: usize,
    max_time: u64,
}

impl BufferManager {
    pub fn new(
        file_manager: Arc<FileManager>,
        log_manager: Arc<Mutex<LogManager>>,
        num_buffers: usize,
    ) -> Self {
        debug!(
            "Start to initialize buffer manager with {} buffers",
            num_buffers
        );
        let buffers = (0..num_buffers)
            .map(|_| {
                Arc::new(Mutex::new(Buffer::new(
                    file_manager.clone(),
                    log_manager.clone(),
                )))
            })
            .collect();

        debug!("Buffer manager initialization done");
        Self {
            file_manager,
            log_manager,
            pool: buffers,
            available: num_buffers,
            max_time: 1000, // Default max time to wait for a buffer (in milliseconds)
        }
    }

    pub fn pin(&mut self, page_id: &PageId) -> anyhow::Result<Arc<Mutex<Buffer>>, BufferError> {
        debug!("Trying to pin page: {}", page_id);
        let deadline = Instant::now() + Duration::from_millis(self.max_time);

        while Instant::now() < deadline {
            if let Ok(buffer) = self.try_to_pin(page_id.clone()) {
                trace!("Pinned page: {}", page_id);
                return Ok(buffer);
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        Err(BufferError::Timeout("Could not pin buffer: timeout".into()))
    }

    pub fn unpin(&mut self, buffer: Arc<Mutex<Buffer>>) -> anyhow::Result<()> {
        let mut buffer = buffer.lock().unwrap();
        debug!("Trying to unpin page: {:?}", buffer.page_id());

        buffer.unpin();
        if !buffer.is_pinned() {
            self.available += 1;
        }

        Ok(())
    }

    pub fn available(&self) -> usize {
        self.available
    }

    pub fn flush_all(&mut self, txn: i32) -> anyhow::Result<()> {
        for buffer in &mut self.pool {
            let mut buffer = buffer
                .lock()
                .map_err(|_| io::Error::other("Failed to acquire buffer lock"))?;
            if buffer.modifying_txn() == txn {
                buffer.flush()?;
            }
        }

        Ok(())
    }

    fn try_to_pin(&mut self, page_id: PageId) -> anyhow::Result<Arc<Mutex<Buffer>>> {
        if let Some(buffer) = self.find_existing_buffer(&page_id) {
            let mut locked_buffer = buffer
                .lock()
                .map_err(|_| io::Error::other("Failed to acquire buffer lock"))?;
            if !locked_buffer.is_pinned() {
                self.available -= 1;
            }
            locked_buffer.pin();
            Ok(buffer.clone())
        } else if let Some(buffer) = self.choose_unpinned_buffer() {
            let mut locked_buffer = buffer
                .lock()
                .map_err(|_| io::Error::other("Failed to acquire buffer lock"))?;
            locked_buffer.assign_to_page(&page_id)?;
            self.available -= 1;
            locked_buffer.pin();
            Ok(buffer.clone())
        } else {
            bail!("No available buffers to pin")
        }
    }

    fn find_existing_buffer(&self, page_id: &PageId) -> Option<Arc<Mutex<Buffer>>> {
        for buffer in &self.pool {
            if let Ok(locked_buffer) = buffer.lock()
                && locked_buffer.page_id() == Some(page_id)
            {
                return Some(buffer.clone());
            }
        }
        None
    }

    fn choose_unpinned_buffer(&mut self) -> Option<Arc<Mutex<Buffer>>> {
        for buffer in &self.pool {
            if let Ok(locked_buffer) = buffer.lock()
                && !locked_buffer.is_pinned()
            {
                return Some(buffer.clone());
            }
        }
        None
    }
}

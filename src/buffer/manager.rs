use std::{
    error::Error,
    io,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    buffer::buffer::Buffer,
    file::{block_id::BlockId, manager::FileManager},
    log::manager::LogManager,
};

enum BufferError {
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
        let buffers = (0..num_buffers)
            .map(|_| {
                Arc::new(Mutex::new(Buffer::new(
                    file_manager.clone(),
                    log_manager.clone(),
                )))
            })
            .collect();
        Self {
            file_manager,
            log_manager,
            pool: buffers,
            available: num_buffers,
            max_time: 1000, // Default max time to wait for a buffer (in milliseconds)
        }
    }

    pub fn pin(&mut self, block: &BlockId) -> Result<Arc<Mutex<Buffer>>, BufferError> {
        let deadline = Instant::now() + Duration::from_millis(self.max_time);

        while Instant::now() < deadline {
            if let Ok(buffer) = self.try_to_pin(block.clone()) {
                return Ok(buffer);
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        Err(BufferError::Timeout("Could not pin buffer: timeout".into()))
    }

    pub fn unpin(&mut self, buffer: &mut Buffer) {
        buffer.unpin();
        if !buffer.is_pinned() {
            self.available += 1;
        }
    }

    pub fn available(&self) -> usize {
        self.available
    }

    pub fn flush_all(&mut self, txn: i32) -> io::Result<()> {
        for buffer in &mut self.pool {
            let mut buffer = buffer.lock().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "Failed to acquire buffer lock")
            })?;
            if buffer.modifying_txn() == txn {
                buffer.flush()?;
            }
        }

        Ok(())
    }

    fn try_to_pin(&mut self, block: BlockId) -> io::Result<Arc<Mutex<Buffer>>> {
        if let Some(buffer) = self.find_existing_buffer(&block) {
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
            locked_buffer.assign_to_block(&block)?;
            self.available -= 1;
            locked_buffer.pin();
            Ok(buffer.clone())
        } else {
            Err(io::Error::other("No available buffers to pin"))
        }
    }

    fn find_existing_buffer(&self, block: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        for buffer in &self.pool {
            if let Ok(locked_buffer) = buffer.lock()
                && locked_buffer.block_id() == Some(block)
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

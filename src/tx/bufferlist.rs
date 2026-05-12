use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::{buffer::Buffer, manager::BufferManager},
    file::PageId,
};

pub struct BufferList {
    buffers: HashMap<PageId, Arc<Mutex<Buffer>>>,
    pins: Vec<PageId>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl BufferList {
    pub fn new(buffer_manager: Arc<Mutex<BufferManager>>) -> Self {
        BufferList {
            buffers: HashMap::new(),
            pins: Vec::new(),
            buffer_manager,
        }
    }

    pub fn get_buffer(&self, page_id: &PageId) -> Option<&Arc<Mutex<Buffer>>> {
        self.buffers.get(page_id)
    }

    pub fn pin(&mut self, page_id: &PageId) -> anyhow::Result<()> {
        let buffer = self.buffer_manager.lock().unwrap().pin(page_id)?;
        self.buffers.insert(page_id.clone(), buffer);
        self.pins.push(page_id.clone());

        Ok(())
    }

    pub fn unpin(&mut self, page_id: &PageId) -> anyhow::Result<()> {
        if let Some(buffer) = self.buffers.get(page_id) {
            self.buffer_manager.lock().unwrap().unpin(buffer.clone())?;
            self.pins.retain(|b| b != page_id);
            if self.pins.iter().filter(|b| *b == page_id).count() == 0 {
                self.buffers.remove(page_id);
            }
        }

        Ok(())
    }

    pub fn unpin_all(&mut self) -> anyhow::Result<()> {
        for page_id in &self.pins {
            if let Some(buffer) = self.buffers.get(page_id) {
                self.buffer_manager.lock().unwrap().unpin(buffer.clone())?;
            }
        }
        self.buffers.clear();
        self.pins.clear();

        Ok(())
    }
}

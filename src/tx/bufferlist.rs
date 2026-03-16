use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::{buffer::Buffer, manager::BufferManager},
    file::BlockId,
};

pub struct BufferList {
    buffers: HashMap<BlockId, Arc<Mutex<Buffer>>>,
    pins: Vec<BlockId>,
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

    pub fn get_buffer(&self, block_id: &BlockId) -> Option<&Arc<Mutex<Buffer>>> {
        self.buffers.get(block_id)
    }

    pub fn pin(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        let buffer = self.buffer_manager.lock().unwrap().pin(block_id)?;
        self.buffers.insert(block_id.clone(), buffer);
        self.pins.push(block_id.clone());

        Ok(())
    }

    pub fn unpin(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        if let Some(buffer) = self.buffers.get(block_id) {
            self.buffer_manager.lock().unwrap().unpin(buffer.clone())?;
            self.pins.retain(|b| b != block_id);
            if self.pins.iter().filter(|b| *b == block_id).count() == 0 {
                self.buffers.remove(block_id);
            }
        }

        Ok(())
    }

    pub fn unpin_all(&mut self) -> anyhow::Result<()> {
        for block_id in &self.pins {
            if let Some(buffer) = self.buffers.get(block_id) {
                self.buffer_manager.lock().unwrap().unpin(buffer.clone())?;
            }
        }
        self.buffers.clear();
        self.pins.clear();

        Ok(())
    }
}

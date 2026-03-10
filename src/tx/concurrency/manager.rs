use crate::file::BlockId;

pub struct ConcurrencyManager {}

impl ConcurrencyManager {
    pub fn new() -> Self {
        ConcurrencyManager {}
    }

    fn sLock(&mut self, block_id: &BlockId) {
        todo!()
    }

    fn xLock(&mut self, block_id: &BlockId) {
        todo!()
    }

    fn release(&mut self) {
        todo!()
    }

    fn has_x_lock(&self, block_id: &BlockId) -> bool {
        todo!()
    }
}

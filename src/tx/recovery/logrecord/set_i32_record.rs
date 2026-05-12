use std::{
    mem,
    sync::{Arc, Mutex},
};

use crate::{
    file::{PageId, Page},
    log::manager::LogManager,
    tx::recovery::logrecord::{LogRecord, UndoContext},
};

pub struct SetI32Record {
    tx_num: i32,
    page_id: PageId,
    offset: i32,
    value: i32,
}

impl SetI32Record {
    pub fn new(page: Page) -> anyhow::Result<Self> {
        let tpos = mem::size_of::<i32>();
        let tx_num = page.get_integer(tpos)?;
        let fpos = tpos + mem::size_of::<i32>();
        let file_name = page.get_string(fpos)?;
        let bpos = fpos + Page::max_length(&file_name);
        let block_num = page.get_integer(bpos)? as u64;
        let opos = bpos + mem::size_of::<i32>();
        let offset = page.get_integer(opos)?;
        let vpos = opos + mem::size_of::<i32>();
        let value = page.get_integer(vpos)?;
        Ok(SetI32Record {
            tx_num,
            page_id: PageId::new(file_name.into(), block_num),
            offset,
            value,
        })
    }

    pub(crate) fn write_to_log(
        log_manager: Arc<Mutex<LogManager>>,
        tx_num: i32,
        page_id: &PageId,
        offset: usize,
        value: i32,
    ) -> anyhow::Result<usize> {
        let tpos = mem::size_of::<i32>();
        let fpos = tpos + mem::size_of::<i32>();
        let bpos = fpos + Page::max_length(page_id.path().to_str().unwrap()); // the unwrap seams odd
        let opos = bpos + mem::size_of::<i32>();
        let vpos = opos + mem::size_of::<i32>();
        let record_size = vpos + mem::size_of::<i32>();

        let mut page = Page::with_size(record_size);
        page.set_integer(0, crate::tx::recovery::logrecord::TxOp::SetI32 as i32)?;
        page.set_integer(tpos, tx_num)?;
        page.set_string(fpos, page_id.path().to_str().unwrap())?;
        page.set_integer(bpos, page_id.block_no() as i32)?;
        page.set_integer(opos, offset as i32)?;
        page.set_integer(vpos, value)?;

        log_manager.lock().unwrap().append(&page.content())
    }
}

impl LogRecord for SetI32Record {
    fn op(&self) -> crate::tx::recovery::logrecord::TxOp {
        crate::tx::recovery::logrecord::TxOp::SetI32
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, ctx: &mut UndoContext) -> anyhow::Result<()> {
        let buf_arc = ctx.buffer_manager.lock().unwrap().pin(&self.page_id)?;
        {
            let mut buf = buf_arc.lock().unwrap();
            let p = buf.contents_mut();
            p.set_integer(self.offset as usize, self.value)?;
            buf.set_modified(self.tx_num, -1);
        }
        ctx.buffer_manager.lock().unwrap().unpin(buf_arc)?;
        Ok(())
    }
}

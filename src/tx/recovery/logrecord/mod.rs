pub mod checkpoint_record;
pub mod commit_record;
pub mod rollback_record;
pub mod set_i32_record;
pub mod set_string_record;
pub mod start_record;

use self::set_string_record::SetStringRecord;
use anyhow::bail;

use crate::{
    file::Page,
    tx::{
        recovery::logrecord::{checkpoint_record::CheckpointRecord, start_record::StartRecord},
        transaction::Transaction,
    },
};

#[derive(Debug)]
pub enum TxOp {
    Checkpoint,
    Start,
    Commit,
    Rollback,
    SetI32,
    SetString,
}

pub trait LogRecord {
    fn op(&self) -> TxOp;

    // TODO: this should retun an Option, since some log records don't have a tx_num (e.g. checkpoint).
    fn tx_num(&self) -> i32;

    fn undo(&self, tx: &mut Transaction);
}

fn from_page(page: Page) -> anyhow::Result<Box<dyn LogRecord>> {
    let op_code = page.get_integer(0)?;
    let op = match op_code {
        0 => TxOp::Checkpoint,
        1 => TxOp::Start,
        2 => TxOp::Commit,
        3 => TxOp::Rollback,
        4 => TxOp::SetI32,
        5 => TxOp::SetString,
        _ => return bail!("Unknown log record type: {}", op_code),
    };
    match op {
        TxOp::Checkpoint => Ok(Box::new(CheckpointRecord::new())),
        TxOp::Start => Ok(Box::new(StartRecord::new(page)?)),
        // TxOp::Commit => Ok(Box::new(CommitRecord::new(page)?)),
        // TxOp::Rollback => Ok(Box::new(RollbackRecord::new(page)?)),
        // TxOp::SetI32 => Ok(Box::new(SetI32Record::new(page)?)),
        TxOp::SetString => Ok(Box::new(SetStringRecord::new(page)?)),
        _ => bail!("Unsupported log record type: {:?}", op),
    }
}

use std::{io, path::PathBuf, sync::Arc};

use crate::{
    file::{block_id::BlockId, manager::FileManager, page::Page},
    log::iterator::LogIterator,
};

pub struct LogManager {
    file_manager: Arc<FileManager>,
    log_file: PathBuf,
    log_page: Page,
    current_block: BlockId,
    latest_lsn: usize,
    latest_saved_lsn: usize,
}

impl LogManager {
    pub fn new(file_manager: Arc<FileManager>, log_file: impl Into<PathBuf>) -> io::Result<Self> {
        let log_file = log_file.into();
        println!("Initializing LogManager with log file: {:?}", log_file);
        let block_size = file_manager.block_size();
        let mut log_page = Page::with_size(block_size);
        let log_size = file_manager.size(log_file.as_path());

        println!("Checking if log file exists and has blocks...");
        let current_block = if log_size == 0 {
            println!("Log file doesn't exist with blocks");
            // append new block and initialize log page boundary
            let blk = file_manager.append_block(log_file.as_path())?;
            log_page
                .set_integer(0, file_manager.block_size() as i32)
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to reset log page boundary: {e}"),
                    )
                })?;
            file_manager.write(&blk, &log_page)?;
            blk
        } else {
            println!("Log file exists with blocks");
            let block = BlockId::new(log_file.clone(), log_size - 1);
            file_manager.read(&block, &mut log_page)?;
            block
        };

        println!(
            "LogManager initialized with current block: {:?}",
            current_block
        );
        Ok(Self {
            file_manager,
            log_file,
            log_page,
            current_block,
            latest_lsn: 0,
            latest_saved_lsn: 0,
        })
    }

    pub fn append(&mut self, record: &[u8]) -> io::Result<usize> {
        let mut boundary = self.log_page.get_integer(0).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to read log page boundary: {e}"),
            )
        })? as usize;

        let record_size = record.len();
        let bytes_needed = record_size + std::mem::size_of::<i32>();

        if boundary < std::mem::size_of::<i32>() + bytes_needed {
            // Not enough space for the record and its size
            self.flush_internal()?;
            self.current_block = self.append_new_block()?;
            boundary = self.log_page.get_integer(0).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to read log page boundary after appending new block: {e}"),
                )
            })? as usize;
        }

        let rec_pos = boundary - bytes_needed;
        self.log_page.set_bytes(rec_pos, record).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to write log record to log page: {e}"),
            )
        })?;
        self.log_page.set_integer(0, rec_pos as i32).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to update log page boundary: {e}"),
            )
        })?;
        self.latest_lsn += 1;
        Ok(self.latest_lsn)
    }

    pub fn flush(&mut self, lsn: usize) -> io::Result<()> {
        if lsn >= self.latest_saved_lsn {
            return self.flush_internal();
        }
        Ok(())
    }

    fn flush_internal(&mut self) -> io::Result<()> {
        self.file_manager
            .write(&self.current_block, &self.log_page)?;
        self.latest_saved_lsn = self.latest_lsn;
        Ok(())
    }

    pub fn iter(&self) -> io::Result<LogIterator> {
        LogIterator::new(self.file_manager.clone(), self.current_block.clone())
    }

    fn append_new_block(&mut self) -> io::Result<BlockId> {
        let blk = self.file_manager.append_block(self.log_file.as_path())?;
        self.log_page
            .set_integer(0, self.file_manager.block_size() as i32)
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to reset log page boundary: {e}"),
                )
            })?;
        self.file_manager.write(&blk, &self.log_page)?;
        Ok(blk)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::{db::SimpleDB, file::page::Page};

    #[test]
    fn example_run() {
        let db = SimpleDB::new("mydb", 400).expect("Failed to create database");
        let mut log_manager = db
            .log_manager()
            .try_lock()
            .expect("Failed to acquire lock on LogManager");
        append_log_records(&mut log_manager, 1, 35);
        print_log_records(&log_manager);
        append_log_records(&mut log_manager, 36, 70);
        log_manager.flush(65).expect("Failed to flush log records");
        print_log_records(&log_manager);
    }

    fn print_log_records(log_manager: &LogManager) {
        println!("The log files contains the following records:");
        for entry in log_manager.iter().expect("Failed to create log iterator") {
            let page = Page::with_bytes(entry);
            let s = page
                .get_string(0)
                .expect("Failed to read string from log record");
            let i = page
                .get_integer(Page::max_length(&s))
                .expect("Failed to read integer from log record");
            println!("{}: {}", s, i);
        }
        println!()
    }

    fn append_log_records(log_manager: &mut LogManager, start: usize, end: usize) {
        println!("Appending log records from {} to {}", start, end);
        for i in start..=end {
            let record = create_log_record(format!("record{}", i), (i + 1000) as i32);
            let isn = log_manager
                .append(&record)
                .expect("Failed to append log record");
            println!("Appended log record {} with ISN {}", i, isn);
        }
        println!()
    }

    fn create_log_record(s: String, i: i32) -> Vec<u8> {
        let npos = Page::max_length(&s);
        let mut page = Page::with_size(npos + std::mem::size_of::<i32>());
        page.set_string(0, &s)
            .expect("Failed to write string to log record");
        page.set_integer(npos, i)
            .expect("Failed to write integer to log record");
        page.content().to_vec()
    }
}

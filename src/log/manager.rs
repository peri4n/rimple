use std::{path::PathBuf, sync::Arc};

use log::{debug, trace};

use crate::{
    file::{PageId, FileManager, Page},
    log::iterator::LogIterator,
};

pub struct LogManager {
    file_manager: Arc<FileManager>,
    log_file: PathBuf,
    log_page: Page,
    current_page: PageId,
    latest_lsn: usize,
    latest_saved_lsn: usize,
}

impl LogManager {
    pub fn new(
        file_manager: Arc<FileManager>,
        log_file: impl Into<PathBuf>,
    ) -> anyhow::Result<Self> {
        debug!("Start to initialize log manager");
        let log_file = log_file.into();
        let page_size = file_manager.page_size();
        let mut log_page = Page::with_size(page_size);
        let log_size = file_manager.size(log_file.as_path())?;

        let current_page = if log_size == 0 {
            trace!("Log file at {:?} is empty. Allocating page.", log_file);
            let blk = file_manager.append_page(log_file.as_path())?;
            log_page.set_integer(0, file_manager.page_size() as i32)?;
            file_manager.write(&blk, &log_page)?;
            blk
        } else {
            trace!("Log file at {:?} already exists.", log_file);
            let page = PageId::new(log_file.clone(), log_size - 1);
            file_manager.read(&page, &mut log_page)?;
            page
        };

        debug!("Log manager initialization done");
        Ok(Self {
            file_manager,
            log_file,
            log_page,
            current_page,
            latest_lsn: 0,
            latest_saved_lsn: 0,
        })
    }

    pub fn append(&mut self, record: &[u8]) -> anyhow::Result<usize> {
        let mut boundary = self.log_page.get_integer(0)? as usize;

        let record_size = record.len();
        let bytes_needed = record_size + std::mem::size_of::<i32>();

        if boundary < std::mem::size_of::<i32>() + bytes_needed {
            // Not enough space for the record and its size
            self.flush_internal()?;
            self.current_page = self.append_new_page()?;
            boundary = self.log_page.get_integer(0)? as usize;
        }

        let rec_pos = boundary - bytes_needed;
        self.log_page.set_bytes(rec_pos, record)?;
        self.log_page.set_integer(0, rec_pos as i32)?;
        self.latest_lsn += 1;
        Ok(self.latest_lsn)
    }

    pub fn flush(&mut self, lsn: usize) -> anyhow::Result<()> {
        if lsn >= self.latest_saved_lsn {
            return self.flush_internal();
        }
        Ok(())
    }

    fn flush_internal(&mut self) -> anyhow::Result<()> {
        self.file_manager
            .write(&self.current_page, &self.log_page)?;
        self.latest_saved_lsn = self.latest_lsn;
        Ok(())
    }

    pub(crate) fn iter(&self) -> anyhow::Result<LogIterator> {
        LogIterator::new(self.file_manager.clone(), self.current_page.clone())
    }

    fn append_new_page(&mut self) -> anyhow::Result<PageId> {
        let blk = self.file_manager.append_page(self.log_file.as_path())?;
        self.log_page
            .set_integer(0, self.file_manager.page_size() as i32)?;
        self.file_manager.write(&blk, &self.log_page)?;
        Ok(blk)
    }
}

#[cfg(test)]
mod test {

    use tempfile::TempDir;

    use super::*;

    fn temp_log_manager(page_size: usize) -> (LogManager, TempDir) {
        let (fm, tmp) = crate::file::manager::test::temp_file_manager(page_size);
        (
            LogManager::new(Arc::new(fm), tmp.path().join("logfile"))
                .expect("Failed to create LogManager"),
            tmp,
        )
    }

    fn mk_record(s: &str, i: i32) -> Vec<u8> {
        let npos = Page::max_length(s);
        let mut page = Page::with_size(npos + std::mem::size_of::<i32>());
        page.set_string(0, s).unwrap();
        page.set_integer(npos, i).unwrap();
        page.content().to_vec()
    }

    fn parse_entry(bytes: &[u8]) -> (String, i32) {
        let page = Page::with_bytes(bytes);
        let s = page.get_string(0).unwrap();
        let i = page.get_integer(Page::max_length(&s)).unwrap();
        (s, i)
    }

    #[test]
    fn empty_log_iterates_nothing() {
        let (log, _tmp) = temp_log_manager(4096);
        let items: Vec<_> = log.iter().unwrap().collect();
        assert!(items.is_empty());
    }

    #[test]
    fn append_and_iter_single_page() {
        let (mut lm, _) = temp_log_manager(4096);
        let mut last_lsn = 0;
        for i in 1..=5 {
            last_lsn = lm
                .append(&mk_record(&format!("rec{:03}", i), 1000 + i))
                .unwrap();
            assert_eq!(last_lsn, i as usize);
        }
        lm.flush(last_lsn).unwrap();

        let got: Vec<_> = lm
            .iter()
            .unwrap()
            .map(|e| parse_entry(&e))
            .collect();

        let exp: Vec<_> = (1..=5)
            .rev()
            .map(|i| (format!("rec{:03}", i), 1000 + i))
            .collect();
        assert_eq!(got, exp);
    }

    #[test]
    fn append_across_pages_iterates_newest_page_first() {
        let (mut lm, _) = temp_log_manager(128);

        // Each record ~18 bytes in page; 6 fit in 128 -> force 2 pages with 12 records
        let mut last_lsn = 0;
        for i in 1..=12 {
            last_lsn = lm
                .append(&mk_record(&format!("rec{:03}", i), 2000 + i))
                .unwrap();
        }
        lm.flush(last_lsn).unwrap();

        let got: Vec<_> = lm
            .iter()
            .unwrap()
            .map(|e| parse_entry(&e).0)
            .collect();

        // Expect reverse chronological: 12..7 then 6..1
        let mut exp: Vec<String> = (7..=12).rev().map(|i| format!("rec{:03}", i)).collect();
        exp.extend((1..=6).rev().map(|i| format!("rec{:03}", i)));
        assert_eq!(got, exp);
    }

    #[test]
    fn flush_persists_records_across_reopen() {
        let (mut lm, tmp) = temp_log_manager(4096);
        let mut last_lsn = 0;
        for i in 1..=3 {
            last_lsn = lm
                .append(&mk_record(&format!("rec{:03}", i), 3000 + i))
                .unwrap();
        }
        lm.flush(last_lsn).unwrap();

        let lm2 = LogManager::new(Arc::new(FileManager::new(tmp.path(), 4096).unwrap()), tmp.path().join("logfile")).unwrap();
        let got: Vec<_> = lm2
            .iter()
            .unwrap()
            .map(|e| parse_entry(&e))
            .collect();
        let exp: Vec<_> = (1..=3)
            .rev()
            .map(|i| (format!("rec{:03}", i), 3000 + i))
            .collect();
        assert_eq!(got, exp);
    }
}

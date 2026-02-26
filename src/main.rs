use std::io;

use ::log::info;

mod buffer;
mod db;
mod file;
mod log;

fn main() -> Result<(), io::Error> {
    env_logger::init();

    info!("Starting Simple DB");
    let _ = db::SimpleDB::new("mydb", 4096)?;

    info!("Listening to requests");
    Ok(())
}

#[cfg(test)]
mod test {

    use crate::db::SimpleDB;

    fn new_test_db(block: usize) -> (tempfile::TempDir, SimpleDB) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = SimpleDB::new(tmp.path(), block).unwrap_or_else(|e| {
            panic!("Failed to create test database in {} because {}", tmp.path().display(), e)
        });
        (tmp, db)
    }

    #[test]
    fn create_a_new_database() {
        let (_tmp, _db) = new_test_db(4096);
    }
}

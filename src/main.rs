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

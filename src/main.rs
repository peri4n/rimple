mod file;
mod db;

fn main() {
    let db = db::SimpleDB::new("mydb", 4096).expect("Failed to create database");
    println!("Database created at: {}", db.file_manager().path().display());
}

//! File management and page-based storage abstraction.
//!
//! This module provides core abstractions for file-based storage including:
//! - Block identification and addressing
//! - Page-based data storage with type-safe serialization  
//! - File management with caching and synchronous I/O

// Private modules - not exposed in public API
mod block_id;
mod manager;
mod page;

// Public re-exports with inlined documentation
#[doc(inline)]
pub use self::block_id::BlockId;
#[doc(inline)]
pub use self::manager::FileManager;
#[doc(inline)]
pub use self::page::{Page, PageError, PageResult};

//! File management and page-based storage abstraction.
//!
//! This module provides core abstractions for file-based storage including:
//! - Page identification and addressing
//! - Page-based data storage with type-safe serialization  
//! - File management with caching and synchronous I/O

// Private modules - not exposed in public API
pub mod page_id;
pub mod manager;
pub mod page;

// Public re-exports with inlined documentation
#[doc(inline)]
pub use self::page_id::PageId;
#[doc(inline)]
pub use self::manager::FileManager;
#[doc(inline)]
pub use self::page::Page;

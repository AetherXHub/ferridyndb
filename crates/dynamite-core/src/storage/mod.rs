//! Storage engine: pages, file I/O, headers, memory-mapped reads, locking.

pub mod file;
pub mod freelist;
pub mod header;
pub mod lock;
pub mod mmap;
pub mod page;
pub mod pending_free;
pub mod slotted;
pub mod snapshot;

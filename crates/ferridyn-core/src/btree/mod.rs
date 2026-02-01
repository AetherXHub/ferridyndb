//! B+Tree index with copy-on-write pages, overflow chains, and range scans.

pub mod node;
pub mod ops;
pub mod overflow;

use std::collections::HashMap;

use crate::error::StorageError;
use crate::storage::page::{Page, PageType};
use crate::types::{PAGE_SIZE, PageId};

/// Abstraction over page read/write/allocate for the B+Tree.
///
/// The B+Tree operates on in-memory `Page` objects and never does file I/O
/// directly. The caller is responsible for reading pages from disk and writing
/// dirty pages back.
pub trait PageStore {
    /// Read a page by ID.
    fn read_page(&self, page_id: PageId) -> Result<Page, StorageError>;
    /// Write (or buffer) a modified page.
    fn write_page(&mut self, page: Page) -> Result<(), StorageError>;
    /// Allocate a new page of the given type.
    fn allocate_page(&mut self, page_type: PageType) -> Result<Page, StorageError>;
    /// Free a page, returning it to the pool.
    fn free_page(&mut self, page_id: PageId) -> Result<(), StorageError>;
}

/// In-memory page store backed by a `HashMap`. Used for testing.
pub struct InMemoryPageStore {
    pages: HashMap<PageId, [u8; PAGE_SIZE]>,
    next_page_id: PageId,
}

impl InMemoryPageStore {
    /// Create a new empty in-memory page store.
    pub fn new() -> Self {
        Self {
            pages: HashMap::new(),
            next_page_id: 1,
        }
    }
}

impl Default for InMemoryPageStore {
    fn default() -> Self {
        Self::new()
    }
}

impl PageStore for InMemoryPageStore {
    fn read_page(&self, page_id: PageId) -> Result<Page, StorageError> {
        let buf = self
            .pages
            .get(&page_id)
            .ok_or(StorageError::PageOutOfBounds {
                page_id,
                total_pages: self.next_page_id,
            })?;
        Ok(Page::from_bytes(*buf, page_id))
    }

    fn write_page(&mut self, page: Page) -> Result<(), StorageError> {
        let page_id = page.page_id();
        self.pages.insert(page_id, *page.data());
        Ok(())
    }

    fn allocate_page(&mut self, page_type: PageType) -> Result<Page, StorageError> {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        let page = Page::new(page_id, page_type);
        self.pages.insert(page_id, *page.data());
        Ok(Page::new(page_id, page_type))
    }

    fn free_page(&mut self, page_id: PageId) -> Result<(), StorageError> {
        self.pages.remove(&page_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_store_allocate_and_read() {
        let mut store = InMemoryPageStore::new();
        let page = store.allocate_page(PageType::BTreeLeaf).unwrap();
        let page_id = page.page_id();
        assert_eq!(page.page_type().unwrap(), PageType::BTreeLeaf);

        let read_back = store.read_page(page_id).unwrap();
        assert_eq!(read_back.page_type().unwrap(), PageType::BTreeLeaf);
        assert_eq!(read_back.page_id(), page_id);
    }

    #[test]
    fn test_in_memory_store_write_and_read() {
        let mut store = InMemoryPageStore::new();
        let mut page = store.allocate_page(PageType::BTreeLeaf).unwrap();
        let page_id = page.page_id();

        // Modify the page
        page.set_entry_count(42);
        store.write_page(page).unwrap();

        let read_back = store.read_page(page_id).unwrap();
        assert_eq!(read_back.entry_count(), 42);
    }

    #[test]
    fn test_in_memory_store_free_page() {
        let mut store = InMemoryPageStore::new();
        let page = store.allocate_page(PageType::BTreeLeaf).unwrap();
        let page_id = page.page_id();

        store.free_page(page_id).unwrap();
        assert!(store.read_page(page_id).is_err());
    }

    #[test]
    fn test_in_memory_store_read_nonexistent() {
        let store = InMemoryPageStore::new();
        assert!(store.read_page(999).is_err());
    }

    #[test]
    fn test_in_memory_store_sequential_ids() {
        let mut store = InMemoryPageStore::new();
        let p1 = store.allocate_page(PageType::BTreeLeaf).unwrap();
        let p2 = store.allocate_page(PageType::BTreeInternal).unwrap();
        let p3 = store.allocate_page(PageType::Overflow).unwrap();

        assert_eq!(p1.page_id(), 1);
        assert_eq!(p2.page_id(), 2);
        assert_eq!(p3.page_id(), 3);
    }
}

use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::FileExt;

use crate::btree::PageStore;
use crate::error::StorageError;
use crate::storage::page::{Page, PageType, verify_page_integrity};
use crate::types::{PAGE_SIZE, PageId};

/// A read-only [`PageStore`] backed by a file descriptor.
///
/// Uses `pread` (`FileExt::read_exact_at`) so it is safe to share the
/// underlying file handle across threads (no seek-based state).
pub struct FilePageStore {
    file: File,
    total_page_count: u64,
}

impl FilePageStore {
    /// Wrap an already-cloned file descriptor for read-only page access.
    pub fn new(file: File, total_page_count: u64) -> Self {
        Self {
            file,
            total_page_count,
        }
    }
}

impl PageStore for FilePageStore {
    fn read_page(&self, page_id: PageId) -> Result<Page, StorageError> {
        if page_id >= self.total_page_count {
            return Err(StorageError::PageOutOfBounds {
                page_id,
                total_pages: self.total_page_count,
            });
        }
        let mut buf = [0u8; PAGE_SIZE];
        let offset = page_id * PAGE_SIZE as u64;
        self.file
            .read_exact_at(&mut buf, offset)
            .map_err(StorageError::Io)?;
        // Verify page integrity for data pages (skip header pages 0 and 1).
        if page_id >= 2 {
            verify_page_integrity(&buf, page_id)?;
        }
        Ok(Page::from_bytes(buf, page_id))
    }

    fn write_page(&mut self, _page: Page) -> Result<(), StorageError> {
        Err(StorageError::CorruptedPage(
            "FilePageStore is read-only".to_string(),
        ))
    }

    fn allocate_page(&mut self, _page_type: PageType) -> Result<Page, StorageError> {
        Err(StorageError::CorruptedPage(
            "FilePageStore is read-only".to_string(),
        ))
    }

    fn free_page(&mut self, _page_id: PageId) -> Result<(), StorageError> {
        Err(StorageError::CorruptedPage(
            "FilePageStore is read-only".to_string(),
        ))
    }
}

/// A buffered [`PageStore`] that overlays in-memory writes on top of
/// file-backed reads.
///
/// Used by write transactions: all mutations accumulate in the overlay
/// `HashMap` and are flushed to disk only on commit.
pub struct BufferedPageStore {
    file: File,
    overlay: HashMap<PageId, [u8; PAGE_SIZE]>,
    next_page_id: PageId,
    freed_pages: Vec<PageId>,
    file_total_pages: u64,
}

impl BufferedPageStore {
    /// Create a new buffered store over the given file descriptor.
    ///
    /// `total_page_count` is the number of pages currently on disk; new
    /// allocations start from that offset.
    pub fn new(file: File, total_page_count: u64) -> Self {
        Self {
            file,
            overlay: HashMap::new(),
            next_page_id: total_page_count,
            freed_pages: Vec::new(),
            file_total_pages: total_page_count,
        }
    }

    /// Return a reference to the in-memory overlay pages.
    pub fn overlay(&self) -> &HashMap<PageId, [u8; PAGE_SIZE]> {
        &self.overlay
    }

    /// Return the next page id that would be allocated.
    pub fn next_page_id(&self) -> PageId {
        self.next_page_id
    }
}

impl PageStore for BufferedPageStore {
    fn read_page(&self, page_id: PageId) -> Result<Page, StorageError> {
        // Check overlay first.
        if let Some(buf) = self.overlay.get(&page_id) {
            return Ok(Page::from_bytes(*buf, page_id));
        }
        // Fall through to disk read.
        if page_id >= self.file_total_pages {
            return Err(StorageError::PageOutOfBounds {
                page_id,
                total_pages: self.file_total_pages,
            });
        }
        let mut buf = [0u8; PAGE_SIZE];
        let offset = page_id * PAGE_SIZE as u64;
        self.file
            .read_exact_at(&mut buf, offset)
            .map_err(StorageError::Io)?;
        // Verify page integrity for data pages (skip header pages 0 and 1).
        if page_id >= 2 {
            verify_page_integrity(&buf, page_id)?;
        }
        Ok(Page::from_bytes(buf, page_id))
    }

    fn write_page(&mut self, page: Page) -> Result<(), StorageError> {
        let page_id = page.page_id();
        self.overlay.insert(page_id, *page.data());
        Ok(())
    }

    fn allocate_page(&mut self, page_type: PageType) -> Result<Page, StorageError> {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        let page = Page::new(page_id, page_type);
        self.overlay.insert(page_id, *page.data());
        Ok(Page::new(page_id, page_type))
    }

    fn free_page(&mut self, page_id: PageId) -> Result<(), StorageError> {
        self.overlay.remove(&page_id);
        self.freed_pages.push(page_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Create a temp file with two pages of data, return the file handle and path.
    fn create_test_file() -> (NamedTempFile, u64) {
        let mut tmp = NamedTempFile::new().unwrap();
        // Write 2 pages.
        let page0 = [0xAAu8; PAGE_SIZE];
        let page1 = [0xBBu8; PAGE_SIZE];
        tmp.write_all(&page0).unwrap();
        tmp.write_all(&page1).unwrap();
        tmp.flush().unwrap();
        (tmp, 2)
    }

    #[test]
    fn test_file_page_store_read() {
        let (tmp, count) = create_test_file();
        let file = tmp.as_file().try_clone().unwrap();
        let store = FilePageStore::new(file, count);

        let page0 = store.read_page(0).unwrap();
        assert_eq!(page0.data()[100], 0xAA);

        let page1 = store.read_page(1).unwrap();
        assert_eq!(page1.data()[100], 0xBB);
    }

    #[test]
    fn test_file_page_store_out_of_bounds() {
        let (tmp, count) = create_test_file();
        let file = tmp.as_file().try_clone().unwrap();
        let store = FilePageStore::new(file, count);

        assert!(store.read_page(5).is_err());
    }

    #[test]
    fn test_file_page_store_write_returns_error() {
        let (tmp, count) = create_test_file();
        let file = tmp.as_file().try_clone().unwrap();
        let mut store = FilePageStore::new(file, count);

        let page = Page::new(0, PageType::BTreeLeaf);
        assert!(store.write_page(page).is_err());
    }

    #[test]
    fn test_buffered_page_store_read_through() {
        let (tmp, count) = create_test_file();
        let file = tmp.as_file().try_clone().unwrap();
        let store = BufferedPageStore::new(file, count);

        // Should read from disk.
        let page0 = store.read_page(0).unwrap();
        assert_eq!(page0.data()[100], 0xAA);
    }

    #[test]
    fn test_buffered_page_store_overlay() {
        let (tmp, count) = create_test_file();
        let file = tmp.as_file().try_clone().unwrap();
        let mut store = BufferedPageStore::new(file, count);

        // Write an overlay for page 0.
        let mut page = Page::new(0, PageType::BTreeLeaf);
        page.data_mut()[100] = 0xCC;
        store.write_page(page).unwrap();

        // Now reading page 0 should return the overlay.
        let read_back = store.read_page(0).unwrap();
        assert_eq!(read_back.data()[100], 0xCC);

        // Page 1 still reads from disk.
        let page1 = store.read_page(1).unwrap();
        assert_eq!(page1.data()[100], 0xBB);
    }

    #[test]
    fn test_buffered_page_store_allocate() {
        let (tmp, count) = create_test_file();
        let file = tmp.as_file().try_clone().unwrap();
        let mut store = BufferedPageStore::new(file, count);

        assert_eq!(store.next_page_id(), 2);

        let page = store.allocate_page(PageType::BTreeLeaf).unwrap();
        assert_eq!(page.page_id(), 2);
        assert_eq!(store.next_page_id(), 3);

        // The allocated page should be readable from overlay.
        let read_back = store.read_page(2).unwrap();
        assert_eq!(read_back.page_id(), 2);
    }

    #[test]
    fn test_buffered_page_store_free() {
        let (tmp, count) = create_test_file();
        let file = tmp.as_file().try_clone().unwrap();
        let mut store = BufferedPageStore::new(file, count);

        // Allocate and then free.
        let page = store.allocate_page(PageType::BTreeLeaf).unwrap();
        let pid = page.page_id();
        store.free_page(pid).unwrap();

        // The page should no longer be in the overlay; reading it
        // should fail since it's beyond file_total_pages.
        assert!(store.read_page(pid).is_err());
    }
}

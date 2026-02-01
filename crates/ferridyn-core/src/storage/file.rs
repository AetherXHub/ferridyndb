use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::error::StorageError;
use crate::types::{PAGE_SIZE, PageId};

use super::header::FileHeader;

/// Low-level file I/O manager for the database file.
///
/// Provides page-granularity reads and writes using `pread`/`pwrite`
/// (via `FileExt::read_at` / `write_at`).
pub struct FileManager {
    file: File,
    total_page_count: u64,
}

impl FileManager {
    /// Create a new database file at `path`.
    ///
    /// Writes two header pages (both with the same fresh header), fsyncs,
    /// and returns the manager.
    pub fn create(path: &Path) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        let header = FileHeader::new();
        let mut buf = [0u8; PAGE_SIZE];

        // Write header page 0
        header.write_to_page(&mut buf);
        file.write_all_at(&buf, 0)?;

        // Write header page 1 (identical)
        file.write_all_at(&buf, PAGE_SIZE as u64)?;

        file.sync_all()?;

        Ok(Self {
            file,
            total_page_count: header.total_page_count,
        })
    }

    /// Open an existing database file at `path`.
    ///
    /// Reads both header pages, selects the current valid one, and returns
    /// the manager together with the active header and its slot index.
    pub fn open(path: &Path) -> Result<(Self, FileHeader, u8), StorageError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        let mut buf_a = [0u8; PAGE_SIZE];
        let mut buf_b = [0u8; PAGE_SIZE];

        file.read_exact_at(&mut buf_a, 0)?;
        file.read_exact_at(&mut buf_b, PAGE_SIZE as u64)?;

        let (header, slot) = FileHeader::select_current(&buf_a, &buf_b)?;

        Ok((
            Self {
                file,
                total_page_count: header.total_page_count,
            },
            header,
            slot,
        ))
    }

    /// Read a full page from disk. Bounds-checked against `total_page_count`.
    pub fn read_page(&self, page_id: PageId) -> Result<[u8; PAGE_SIZE], StorageError> {
        if page_id >= self.total_page_count {
            return Err(StorageError::PageOutOfBounds {
                page_id,
                total_pages: self.total_page_count,
            });
        }
        let mut buf = [0u8; PAGE_SIZE];
        let offset = page_id * PAGE_SIZE as u64;
        self.file.read_exact_at(&mut buf, offset)?;
        Ok(buf)
    }

    /// Write a full page to disk. Allows writing at `page_id == total_page_count`
    /// (i.e. one past the end) to support growth, but not further.
    pub fn write_page(&self, page_id: PageId, data: &[u8; PAGE_SIZE]) -> Result<(), StorageError> {
        if page_id > self.total_page_count {
            return Err(StorageError::PageOutOfBounds {
                page_id,
                total_pages: self.total_page_count,
            });
        }
        let offset = page_id * PAGE_SIZE as u64;
        self.file.write_all_at(data, offset)?;
        Ok(())
    }

    /// Grow the file to accommodate `new_total` pages.
    pub fn grow(&mut self, new_total: u64) -> Result<(), StorageError> {
        if new_total <= self.total_page_count {
            return Ok(());
        }
        let new_len = new_total * PAGE_SIZE as u64;
        self.file.set_len(new_len)?;
        self.total_page_count = new_total;
        Ok(())
    }

    /// Fsync the underlying file.
    pub fn sync(&self) -> Result<(), StorageError> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Return the current total page count.
    pub fn total_page_count(&self) -> u64 {
        self.total_page_count
    }

    /// Return a reference to the underlying file handle.
    pub fn file(&self) -> &File {
        &self.file
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_and_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create
        {
            let fm = FileManager::create(&path).unwrap();
            assert_eq!(fm.total_page_count(), 2);
        }

        // Reopen
        {
            let (fm, header, _slot) = FileManager::open(&path).unwrap();
            assert_eq!(fm.total_page_count(), 2);
            assert_eq!(header.txn_counter, 0);
            assert_eq!(header.total_page_count, 2);
        }
    }

    #[test]
    fn test_write_and_read_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut fm = FileManager::create(&path).unwrap();
        fm.grow(3).unwrap();

        let mut page_data = [0u8; PAGE_SIZE];
        page_data[0..5].copy_from_slice(b"hello");

        fm.write_page(2, &page_data).unwrap();
        let read_back = fm.read_page(2).unwrap();
        assert_eq!(&read_back[0..5], b"hello");
    }

    #[test]
    fn test_grow() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut fm = FileManager::create(&path).unwrap();
        assert_eq!(fm.total_page_count(), 2);

        fm.grow(10).unwrap();
        assert_eq!(fm.total_page_count(), 10);

        // New pages should be readable (zero-filled by OS)
        let buf = fm.read_page(9).unwrap();
        assert_eq!(buf, [0u8; PAGE_SIZE]);
    }

    #[test]
    fn test_page_out_of_bounds() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let fm = FileManager::create(&path).unwrap();
        // total_page_count is 2, so page 5 is out of bounds
        match fm.read_page(5) {
            Err(StorageError::PageOutOfBounds {
                page_id: 5,
                total_pages: 2,
            }) => {}
            other => panic!("expected PageOutOfBounds, got {other:?}"),
        }
    }

    #[test]
    fn test_write_at_boundary() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut fm = FileManager::create(&path).unwrap();
        fm.grow(3).unwrap();

        // Writing at total_page_count (3) should succeed (allows growth write)
        // but writing beyond should fail
        let data = [0u8; PAGE_SIZE];
        assert!(fm.write_page(3, &data).is_ok());

        match fm.write_page(5, &data) {
            Err(StorageError::PageOutOfBounds { .. }) => {}
            other => panic!("expected PageOutOfBounds, got {other:?}"),
        }
    }
}

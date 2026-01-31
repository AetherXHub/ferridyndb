use std::fs::File;

use memmap2::Mmap;

use crate::error::StorageError;
use crate::types::{PAGE_SIZE, PageId};

/// Read-only memory-mapped reader for efficient page reads.
///
/// # Safety invariant
///
/// CoW guarantees no page visible to readers is ever modified in place.
/// Readers only access pages reachable from the committed header pointer.
pub struct MmapReader {
    mmap: Option<Mmap>,
    file: File,
    page_count: u64,
}

impl MmapReader {
    /// Create a new read-only mmap over the given file.
    ///
    /// If `page_count` is 0, no mapping is created (the mmap field is `None`).
    pub fn new(file: &File, page_count: u64) -> Result<Self, StorageError> {
        let owned_file = file.try_clone()?;

        let mmap = if page_count > 0 {
            let len = page_count * PAGE_SIZE as u64;
            // SAFETY: The file is opened read-only for this mapping and the
            // CoW invariant ensures mapped pages are never modified in place.
            let m = unsafe { Mmap::map(&owned_file) }?;
            if (m.len() as u64) < len {
                return Err(StorageError::CorruptedPage(format!(
                    "mmap size {} is smaller than expected {}",
                    m.len(),
                    len
                )));
            }
            Some(m)
        } else {
            None
        };

        Ok(Self {
            mmap,
            file: owned_file,
            page_count,
        })
    }

    /// Return a slice for the given page from the memory-mapped region.
    pub fn read_page(&self, page_id: PageId) -> Result<&[u8], StorageError> {
        if page_id >= self.page_count {
            return Err(StorageError::PageOutOfBounds {
                page_id,
                total_pages: self.page_count,
            });
        }

        let mmap = self.mmap.as_ref().ok_or_else(|| {
            StorageError::CorruptedPage("mmap not initialised (page_count was 0)".to_string())
        })?;

        let offset = (page_id as usize) * PAGE_SIZE;
        Ok(&mmap[offset..offset + PAGE_SIZE])
    }

    /// Drop the current mapping and create a new one with an updated page count.
    pub fn remap(&mut self, new_page_count: u64) -> Result<(), StorageError> {
        // Drop old mmap
        self.mmap = None;

        if new_page_count > 0 {
            // SAFETY: Same invariant as in `new`.
            let m = unsafe { Mmap::map(&self.file) }?;
            let expected_len = new_page_count * PAGE_SIZE as u64;
            if (m.len() as u64) < expected_len {
                return Err(StorageError::CorruptedPage(format!(
                    "mmap size {} is smaller than expected {}",
                    m.len(),
                    expected_len
                )));
            }
            self.mmap = Some(m);
        }

        self.page_count = new_page_count;
        Ok(())
    }

    /// Return the current page count.
    pub fn page_count(&self) -> u64 {
        self.page_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Helper: create a temp file with `n` pages of data, where each page
    /// starts with its page number as a little-endian u64.
    fn make_temp_file(n: u64) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        for i in 0..n {
            let mut page = [0u8; PAGE_SIZE];
            page[0..8].copy_from_slice(&i.to_le_bytes());
            f.write_all(&page).unwrap();
        }
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_mmap_read_page() {
        let f = make_temp_file(4);
        let reader = MmapReader::new(f.as_file(), 4).unwrap();

        for i in 0u64..4 {
            let page = reader.read_page(i).unwrap();
            let stored_id = u64::from_le_bytes(page[0..8].try_into().unwrap());
            assert_eq!(stored_id, i);
        }
    }

    #[test]
    fn test_mmap_remap() {
        let mut f = make_temp_file(2);
        let mut reader = MmapReader::new(f.as_file(), 2).unwrap();
        assert_eq!(reader.page_count(), 2);

        // Grow the file by adding 2 more pages
        for i in 2u64..4 {
            let mut page = [0u8; PAGE_SIZE];
            page[0..8].copy_from_slice(&i.to_le_bytes());
            f.write_all(&page).unwrap();
        }
        f.flush().unwrap();

        // Remap
        reader.remap(4).unwrap();
        assert_eq!(reader.page_count(), 4);

        // Read new pages
        let page = reader.read_page(3).unwrap();
        let stored_id = u64::from_le_bytes(page[0..8].try_into().unwrap());
        assert_eq!(stored_id, 3);
    }

    #[test]
    fn test_mmap_bounds_check() {
        let f = make_temp_file(2);
        let reader = MmapReader::new(f.as_file(), 2).unwrap();

        match reader.read_page(5) {
            Err(StorageError::PageOutOfBounds {
                page_id: 5,
                total_pages: 2,
            }) => {}
            other => panic!("expected PageOutOfBounds, got {other:?}"),
        }
    }

    #[test]
    fn test_mmap_zero_pages() {
        let f = NamedTempFile::new().unwrap();
        let reader = MmapReader::new(f.as_file(), 0).unwrap();
        assert_eq!(reader.page_count(), 0);
        assert!(reader.read_page(0).is_err());
    }
}

//! Version chain storage for old versions of MVCC documents.
//!
//! Reuses the overflow page format (header [0..40] common, [40..48] next_page
//! u64 LE, [48..4096] data) for storing serialized `VersionedDocument` bytes of
//! old versions.

use crate::btree::PageStore;
use crate::error::StorageError;
use crate::storage::page::PageType;
use crate::types::{PAGE_SIZE, PageId};

const CHAIN_HEADER_SIZE: usize = 48;
const CHAIN_DATA_PER_PAGE: usize = PAGE_SIZE - CHAIN_HEADER_SIZE;

/// Read the next-page pointer from a chain (overflow) page.
fn chain_next(data: &[u8; PAGE_SIZE]) -> PageId {
    u64::from_le_bytes(data[40..48].try_into().unwrap())
}

/// Set the next-page pointer on a chain (overflow) page.
fn chain_set_next(data: &mut [u8; PAGE_SIZE], next: PageId) {
    data[40..48].copy_from_slice(&next.to_le_bytes());
}

/// Write data to a version chain of Overflow pages.
///
/// Returns `(first_page_id, total_length)` so the caller can store a pointer
/// to the chain.
pub fn write_version_chain(
    store: &mut impl PageStore,
    data: &[u8],
) -> Result<(PageId, u32), StorageError> {
    let total_len = data.len();
    if total_len == 0 {
        // Even empty data gets a page so we have a valid page id.
        let page = store.allocate_page(PageType::Overflow)?;
        let page_id = page.page_id();
        store.write_page(page)?;
        return Ok((page_id, 0));
    }

    let num_pages = total_len.div_ceil(CHAIN_DATA_PER_PAGE);
    let mut pages = Vec::with_capacity(num_pages);
    for _ in 0..num_pages {
        pages.push(store.allocate_page(PageType::Overflow)?);
    }

    let page_ids: Vec<PageId> = pages.iter().map(|p| p.page_id()).collect();

    let mut offset = 0usize;
    for (i, page) in pages.iter_mut().enumerate() {
        let chunk_size = (total_len - offset).min(CHAIN_DATA_PER_PAGE);
        let page_data = page.data_mut();
        page_data[CHAIN_HEADER_SIZE..CHAIN_HEADER_SIZE + chunk_size]
            .copy_from_slice(&data[offset..offset + chunk_size]);
        offset += chunk_size;

        let next_id = if i + 1 < num_pages {
            page_ids[i + 1]
        } else {
            0
        };
        chain_set_next(page_data, next_id);
    }

    let first_page_id = page_ids[0];

    for page in pages {
        store.write_page(page)?;
    }

    Ok((first_page_id, total_len as u32))
}

/// Read data from a version chain of Overflow pages.
///
/// Walks the chain collecting data bytes up to `total_len`.
pub fn read_version_chain(
    store: &impl PageStore,
    first_page: PageId,
    total_len: u32,
) -> Result<Vec<u8>, StorageError> {
    let total_len = total_len as usize;
    if total_len == 0 {
        return Ok(Vec::new());
    }

    let mut result = Vec::with_capacity(total_len);
    let mut current_page_id = first_page;

    while current_page_id != 0 && result.len() < total_len {
        let page = store.read_page(current_page_id)?;
        let page_data = page.data();
        let remaining = total_len - result.len();
        let chunk_size = remaining.min(CHAIN_DATA_PER_PAGE);
        result.extend_from_slice(&page_data[CHAIN_HEADER_SIZE..CHAIN_HEADER_SIZE + chunk_size]);
        current_page_id = chain_next(page_data);
    }

    if result.len() != total_len {
        return Err(StorageError::CorruptedPage(format!(
            "version chain incomplete: expected {} bytes, got {}",
            total_len,
            result.len()
        )));
    }

    Ok(result)
}

/// Free all pages in a version chain.
pub fn free_version_chain(
    store: &mut impl PageStore,
    first_page: PageId,
) -> Result<(), StorageError> {
    let mut current_page_id = first_page;

    while current_page_id != 0 {
        let page = store.read_page(current_page_id)?;
        let next = chain_next(page.data());
        store.free_page(current_page_id)?;
        current_page_id = next;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::InMemoryPageStore;

    #[test]
    fn test_write_read_roundtrip() {
        let mut store = InMemoryPageStore::new();
        let data = b"hello version chain";
        let (page_id, len) = write_version_chain(&mut store, data).unwrap();
        let recovered = read_version_chain(&store, page_id, len).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_write_read_large() {
        let mut store = InMemoryPageStore::new();
        // Data spanning multiple pages.
        let data = vec![0xAB; CHAIN_DATA_PER_PAGE * 3 + 100];
        let (page_id, len) = write_version_chain(&mut store, &data).unwrap();
        let recovered = read_version_chain(&store, page_id, len).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_free_chain() {
        let mut store = InMemoryPageStore::new();
        let data = vec![0xCD; CHAIN_DATA_PER_PAGE * 2 + 50];
        let (page_id, len) = write_version_chain(&mut store, &data).unwrap();

        // Verify data is readable.
        assert_eq!(read_version_chain(&store, page_id, len).unwrap(), data);

        // Free and verify pages are gone.
        free_version_chain(&mut store, page_id).unwrap();
        assert!(read_version_chain(&store, page_id, len).is_err());
    }

    #[test]
    fn test_empty_data() {
        let mut store = InMemoryPageStore::new();
        let (page_id, len) = write_version_chain(&mut store, &[]).unwrap();
        assert!(page_id > 0);
        assert_eq!(len, 0);
        let recovered = read_version_chain(&store, page_id, len).unwrap();
        assert!(recovered.is_empty());
    }
}

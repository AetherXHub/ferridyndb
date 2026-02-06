//! Overflow page management for large values that exceed the inline threshold.
//!
//! ## Overflow page layout
//!
//! ```text
//! [0..40]    common page header (PageType::Overflow)
//! [40..48]   next_overflow: u64 LE (PageId of next page in chain, 0 if last)
//! [48..4096] data (up to OVERFLOW_DATA_PER_PAGE bytes)
//! ```
//!
//! ## Value encoding in leaf cells
//!
//! Inline value: `[0x00][data bytes]`
//! Overflow pointer: `[0x01][page_id: u64 LE][total_length: u32 LE]` (13 bytes)

use crate::error::StorageError;
use crate::storage::page::{Page, PageType};
use crate::types::{PAGE_SIZE, PageId, SLOT_SIZE};

use super::PageStore;
use super::node::LEAF_DATA_OFFSET;

/// Size of the overflow-specific header region (common header + next pointer).
pub const OVERFLOW_HEADER_SIZE: usize = 48;

/// Maximum data bytes stored per overflow page.
pub const OVERFLOW_DATA_PER_PAGE: usize = PAGE_SIZE - OVERFLOW_HEADER_SIZE;

/// Marker byte for inline values.
pub const INLINE_MARKER: u8 = 0x00;

/// Marker byte for overflow pointer values.
pub const OVERFLOW_MARKER: u8 = 0x01;

/// Legacy fixed overflow threshold, kept for backward compatibility in tests.
pub const OVERFLOW_THRESHOLD: usize = 1500;

/// Compute the maximum inline value size for a given encoded key length.
///
/// Returns the largest value that can be stored inline such that the resulting
/// leaf cell (key_len_prefix + key + inline_marker + value) plus its slot entry
/// fits in a single empty leaf page.
pub fn max_inline_value_size(encoded_key_len: usize) -> usize {
    // Available space in an empty leaf page for a single cell + slot:
    //   PAGE_SIZE - LEAF_DATA_OFFSET - SLOT_SIZE
    // Cell layout: [key_len: u16][key bytes][marker: u8][value bytes]
    let max_cell_size = PAGE_SIZE - LEAF_DATA_OFFSET - SLOT_SIZE;
    let cell_overhead = 2 + encoded_key_len + 1; // key_len prefix + key + inline marker
    max_cell_size.saturating_sub(cell_overhead)
}

/// Read the next-overflow pointer from an overflow page.
fn overflow_next(page: &Page) -> PageId {
    let data = page.data();
    u64::from_le_bytes(data[40..48].try_into().unwrap())
}

/// Set the next-overflow pointer on an overflow page.
fn overflow_set_next(page: &mut Page, next: PageId) {
    let data = page.data_mut();
    data[40..48].copy_from_slice(&next.to_le_bytes());
}

/// Read a value that may be inline or stored across overflow pages.
///
/// `raw_value` is the value portion extracted from the leaf cell.
pub fn read_value(store: &impl PageStore, raw_value: &[u8]) -> Result<Vec<u8>, StorageError> {
    if raw_value.is_empty() {
        return Ok(Vec::new());
    }

    match raw_value[0] {
        INLINE_MARKER => Ok(raw_value[1..].to_vec()),
        OVERFLOW_MARKER => {
            if raw_value.len() < 13 {
                return Err(StorageError::CorruptedPage(
                    "overflow pointer too short".to_string(),
                ));
            }
            let first_page_id = u64::from_le_bytes(raw_value[1..9].try_into().unwrap());
            let total_length = u32::from_le_bytes(raw_value[9..13].try_into().unwrap()) as usize;

            let mut result = Vec::with_capacity(total_length);
            let mut current_page_id = first_page_id;

            while current_page_id != 0 && result.len() < total_length {
                let page = store.read_page(current_page_id)?;
                let remaining = total_length - result.len();
                let chunk_size = remaining.min(OVERFLOW_DATA_PER_PAGE);
                result.extend_from_slice(
                    &page.data()[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk_size],
                );
                current_page_id = overflow_next(&page);
            }

            if result.len() != total_length {
                return Err(StorageError::CorruptedPage(format!(
                    "overflow chain incomplete: expected {} bytes, got {}",
                    total_length,
                    result.len()
                )));
            }

            Ok(result)
        }
        other => Err(StorageError::CorruptedPage(format!(
            "invalid value marker byte: {other:#04x}"
        ))),
    }
}

/// Write a value, returning the raw bytes to store in the leaf cell.
///
/// If the value is small enough to fit inline in a leaf page alongside the
/// given key, it is stored inline. Otherwise, it is written across one or
/// more overflow pages. The `encoded_key_len` parameter is the byte length
/// of the already-encoded key.
pub fn write_value(
    store: &mut impl PageStore,
    data: &[u8],
    encoded_key_len: usize,
) -> Result<Vec<u8>, StorageError> {
    if data.len() <= max_inline_value_size(encoded_key_len) {
        // Inline: marker + data
        let mut result = Vec::with_capacity(1 + data.len());
        result.push(INLINE_MARKER);
        result.extend_from_slice(data);
        return Ok(result);
    }

    // Overflow: write data across pages, then return the pointer.
    let total_length = data.len();
    let mut offset = 0usize;

    // Allocate all needed pages first, then link them.
    let num_pages = total_length.div_ceil(OVERFLOW_DATA_PER_PAGE);
    let mut pages: Vec<Page> = Vec::with_capacity(num_pages);
    for _ in 0..num_pages {
        pages.push(store.allocate_page(PageType::Overflow)?);
    }

    // Collect page IDs upfront to avoid borrow conflicts.
    let page_ids: Vec<PageId> = pages.iter().map(|p| p.page_id()).collect();

    // Fill pages with data and set next pointers.
    for (i, page) in pages.iter_mut().enumerate() {
        let chunk_size = (total_length - offset).min(OVERFLOW_DATA_PER_PAGE);
        page.data_mut()[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk_size]
            .copy_from_slice(&data[offset..offset + chunk_size]);
        offset += chunk_size;

        // Link to next page (or 0 if last).
        let next_id = if i + 1 < num_pages {
            page_ids[i + 1]
        } else {
            0
        };
        overflow_set_next(page, next_id);
    }

    let first_page_id = page_ids[0];

    // Write all pages to the store.
    for page in pages {
        store.write_page(page)?;
    }

    // Build the overflow pointer: marker + page_id + total_length
    let mut result = Vec::with_capacity(13);
    result.push(OVERFLOW_MARKER);
    result.extend_from_slice(&first_page_id.to_le_bytes());
    result.extend_from_slice(&(total_length as u32).to_le_bytes());
    Ok(result)
}

/// Free all overflow pages in a chain starting at the page referenced by the
/// raw value bytes. If the value is inline, this is a no-op.
pub fn free_overflow_chain(
    store: &mut impl PageStore,
    raw_value: &[u8],
) -> Result<(), StorageError> {
    if raw_value.is_empty() || raw_value[0] != OVERFLOW_MARKER {
        return Ok(());
    }
    if raw_value.len() < 13 {
        return Err(StorageError::CorruptedPage(
            "overflow pointer too short".to_string(),
        ));
    }

    let mut current_page_id = u64::from_le_bytes(raw_value[1..9].try_into().unwrap());

    while current_page_id != 0 {
        let page = store.read_page(current_page_id)?;
        let next = overflow_next(&page);
        store.free_page(current_page_id)?;
        current_page_id = next;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::InMemoryPageStore;

    /// Typical encoded key length used by most tests.
    const TEST_KEY_LEN: usize = 20;

    #[test]
    fn test_inline_roundtrip() {
        let mut store = InMemoryPageStore::new();
        let data = b"hello world";
        let raw = write_value(&mut store, data, TEST_KEY_LEN).unwrap();
        assert_eq!(raw[0], INLINE_MARKER);
        let recovered = read_value(&store, &raw).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_inline_empty() {
        let mut store = InMemoryPageStore::new();
        let raw = write_value(&mut store, b"", TEST_KEY_LEN).unwrap();
        assert_eq!(raw[0], INLINE_MARKER);
        let recovered = read_value(&store, &raw).unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_overflow_roundtrip() {
        let mut store = InMemoryPageStore::new();
        // Use a value larger than any possible inline threshold
        let data = vec![0xAB; PAGE_SIZE];
        let raw = write_value(&mut store, &data, TEST_KEY_LEN).unwrap();
        assert_eq!(raw[0], OVERFLOW_MARKER);
        assert_eq!(raw.len(), 13);
        let recovered = read_value(&store, &raw).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_overflow_large_multi_page() {
        let mut store = InMemoryPageStore::new();
        // Fill more than one overflow page
        let data = vec![0xCD; OVERFLOW_DATA_PER_PAGE * 3 + 100];
        let raw = write_value(&mut store, &data, TEST_KEY_LEN).unwrap();
        assert_eq!(raw[0], OVERFLOW_MARKER);
        let recovered = read_value(&store, &raw).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_overflow_free_chain() {
        let mut store = InMemoryPageStore::new();
        let data = vec![0xEF; 5000];
        let raw = write_value(&mut store, &data, TEST_KEY_LEN).unwrap();

        // Verify we can read before freeing
        assert_eq!(read_value(&store, &raw).unwrap(), data);

        // Free the chain
        free_overflow_chain(&mut store, &raw).unwrap();

        // After freeing, reading should fail
        assert!(read_value(&store, &raw).is_err());
    }

    #[test]
    fn test_free_inline_noop() {
        let mut store = InMemoryPageStore::new();
        let raw = write_value(&mut store, b"small", TEST_KEY_LEN).unwrap();
        // Freeing an inline value is a no-op
        free_overflow_chain(&mut store, &raw).unwrap();
    }

    #[test]
    fn test_read_empty_value() {
        let store = InMemoryPageStore::new();
        let result = read_value(&store, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_dynamic_overflow_threshold() {
        let mut store = InMemoryPageStore::new();
        let key_len = TEST_KEY_LEN;
        let threshold = max_inline_value_size(key_len);

        // Exactly at threshold: should be inline
        let data_at = vec![0x01; threshold];
        let raw = write_value(&mut store, &data_at, key_len).unwrap();
        assert_eq!(raw[0], INLINE_MARKER);

        // One byte over: should be overflow
        let data_over = vec![0x02; threshold + 1];
        let raw = write_value(&mut store, &data_over, key_len).unwrap();
        assert_eq!(raw[0], OVERFLOW_MARKER);
        let recovered = read_value(&store, &raw).unwrap();
        assert_eq!(recovered, data_over);
    }

    #[test]
    fn test_small_key_allows_larger_inline() {
        // A small key should allow a larger inline value than a big key.
        let small_key_threshold = max_inline_value_size(5);
        let large_key_threshold = max_inline_value_size(500);
        assert!(small_key_threshold > large_key_threshold);
    }

    #[test]
    fn test_max_inline_value_size_huge_key() {
        // A key so large it fills the entire page should yield 0 inline capacity.
        let threshold = max_inline_value_size(PAGE_SIZE);
        assert_eq!(threshold, 0);
    }
}

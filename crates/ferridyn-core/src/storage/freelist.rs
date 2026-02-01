use crate::types::{PAGE_SIZE, PageId};

use super::page::Page;

/// Byte offset where free list-specific data begins (after the 32-byte common header).
///
/// Layout of a free list trunk page:
/// ```text
/// [0..32]    common page header (page_type=FreeList, entry_count=N)
/// [32..40]   next_trunk: u64 LE — PageId of the next trunk page, or 0 if none
/// [40..4096] array of PageId values (up to 507 entries, u64 LE each)
/// ```
const NEXT_TRUNK_OFFSET: usize = 32;

/// Byte offset where the PageId array begins.
pub const FREELIST_DATA_OFFSET: usize = 40;

/// Maximum number of PageId entries that fit in one free list trunk page.
pub const FREELIST_MAX_ENTRIES: usize = (PAGE_SIZE - FREELIST_DATA_OFFSET) / 8;

/// Read the `next_trunk` pointer from a free list page.
///
/// Returns the `PageId` of the next trunk page, or `0` if there is no next trunk.
pub fn freelist_next_trunk(page: &Page) -> PageId {
    let data = page.data();
    u64::from_le_bytes(
        data[NEXT_TRUNK_OFFSET..NEXT_TRUNK_OFFSET + 8]
            .try_into()
            .unwrap(),
    )
}

/// Set the `next_trunk` pointer on a free list page.
pub fn freelist_set_next_trunk(page: &mut Page, next: PageId) {
    let data = page.data_mut();
    data[NEXT_TRUNK_OFFSET..NEXT_TRUNK_OFFSET + 8].copy_from_slice(&next.to_le_bytes());
}

/// Read the entry at the given 0-based `index` from the free list page.
///
/// # Panics
///
/// Panics if `index >= FREELIST_MAX_ENTRIES`.
pub fn freelist_entry(page: &Page, index: usize) -> PageId {
    assert!(index < FREELIST_MAX_ENTRIES, "freelist index out of bounds");
    let offset = FREELIST_DATA_OFFSET + index * 8;
    let data = page.data();
    u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
}

/// Set the entry at the given 0-based `index` on the free list page.
///
/// # Panics
///
/// Panics if `index >= FREELIST_MAX_ENTRIES`.
pub fn freelist_set_entry(page: &mut Page, index: usize, page_id: PageId) {
    assert!(index < FREELIST_MAX_ENTRIES, "freelist index out of bounds");
    let offset = FREELIST_DATA_OFFSET + index * 8;
    let data = page.data_mut();
    data[offset..offset + 8].copy_from_slice(&page_id.to_le_bytes());
}

/// Push a `PageId` onto this trunk page (stack-style, appended at `entry_count`).
///
/// Returns `true` if the push succeeded, or `false` if the page is full.
pub fn freelist_push(page: &mut Page, page_id: PageId) -> bool {
    let count = page.entry_count() as usize;
    if count >= FREELIST_MAX_ENTRIES {
        return false;
    }
    freelist_set_entry(page, count, page_id);
    page.set_entry_count((count + 1) as u32);
    true
}

/// Pop a `PageId` from this trunk page (LIFO order).
///
/// Returns `None` if the page is empty.
pub fn freelist_pop(page: &mut Page) -> Option<PageId> {
    let count = page.entry_count() as usize;
    if count == 0 {
        return None;
    }
    let page_id = freelist_entry(page, count - 1);
    page.set_entry_count((count - 1) as u32);
    Some(page_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::PageType;

    fn new_freelist_page() -> Page {
        Page::new(100, PageType::FreeList)
    }

    #[test]
    fn test_push_and_pop() {
        let mut page = new_freelist_page();

        assert!(freelist_push(&mut page, 10));
        assert!(freelist_push(&mut page, 20));
        assert!(freelist_push(&mut page, 30));
        assert_eq!(page.entry_count(), 3);

        // LIFO order
        assert_eq!(freelist_pop(&mut page), Some(30));
        assert_eq!(freelist_pop(&mut page), Some(20));
        assert_eq!(freelist_pop(&mut page), Some(10));
        assert_eq!(freelist_pop(&mut page), None);
        assert_eq!(page.entry_count(), 0);
    }

    #[test]
    fn test_push_full() {
        let mut page = new_freelist_page();

        for i in 0..FREELIST_MAX_ENTRIES {
            assert!(freelist_push(&mut page, i as u64));
        }
        assert_eq!(page.entry_count(), FREELIST_MAX_ENTRIES as u32);

        // One more should fail
        assert!(!freelist_push(&mut page, 9999));
        assert_eq!(page.entry_count(), FREELIST_MAX_ENTRIES as u32);
    }

    #[test]
    fn test_next_trunk() {
        let mut page = new_freelist_page();

        // Default is 0
        assert_eq!(freelist_next_trunk(&page), 0);

        freelist_set_next_trunk(&mut page, 42);
        assert_eq!(freelist_next_trunk(&page), 42);

        freelist_set_next_trunk(&mut page, 0);
        assert_eq!(freelist_next_trunk(&page), 0);
    }

    #[test]
    fn test_push_pop_many() {
        let mut page = new_freelist_page();

        // Push exactly FREELIST_MAX_ENTRIES (507) entries
        for i in 0..FREELIST_MAX_ENTRIES {
            assert!(freelist_push(&mut page, (i + 1) as u64));
        }
        assert_eq!(page.entry_count(), FREELIST_MAX_ENTRIES as u32);

        // Pop all 507 in reverse order
        for i in (0..FREELIST_MAX_ENTRIES).rev() {
            assert_eq!(freelist_pop(&mut page), Some((i + 1) as u64));
        }
        assert_eq!(page.entry_count(), 0);
        assert_eq!(freelist_pop(&mut page), None);
    }
}

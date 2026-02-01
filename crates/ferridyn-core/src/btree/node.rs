//! Helper functions for reading and writing B+Tree internal and leaf node pages.
//!
//! ## Leaf node layout
//!
//! ```text
//! [0..32]   common page header (PageType::BTreeLeaf)
//! [32..40]  next_leaf: u64 LE (PageId of next sibling, 0 if none)
//! [40..]    slotted page region (slot array grows forward, cells grow backward)
//! ```
//!
//! Each leaf cell: `[key_len: u16 LE][key bytes][value bytes]`
//!
//! ## Internal node layout
//!
//! ```text
//! [0..32]   common page header (PageType::BTreeInternal)
//! [32..40]  rightmost_child: u64 LE (child for keys >= all stored keys)
//! [40..]    slotted page region
//! ```
//!
//! Each internal cell: `[key_len: u16 LE][key bytes][child_page_id: u64 LE]`
//!
//! The child in each cell is the pointer for keys LESS THAN that cell's key.
//! The rightmost child handles keys >= the last key.

use std::cmp::Ordering;

use crate::storage::page::Page;
use crate::storage::slotted::SlottedPageRef;
use crate::types::PageId;

/// Byte offset where leaf/internal node data begins (after the 8-byte
/// next_leaf or rightmost_child field following the 32-byte common header).
pub const BTREE_DATA_OFFSET: usize = 40;

// ---------------------------------------------------------------------------
// Leaf node helpers
// ---------------------------------------------------------------------------

/// Read the next-leaf pointer from a leaf page.
pub fn leaf_next_leaf(page: &Page) -> PageId {
    let data = page.data();
    u64::from_le_bytes(data[32..40].try_into().unwrap())
}

/// Set the next-leaf pointer on a leaf page.
pub fn leaf_set_next_leaf(page: &mut Page, next: PageId) {
    let data = page.data_mut();
    data[32..40].copy_from_slice(&next.to_le_bytes());
}

/// Extract the key portion from a leaf cell.
///
/// Cell format: `[key_len: u16 LE][key bytes][value bytes]`
pub fn leaf_cell_key(cell: &[u8]) -> &[u8] {
    let key_len = u16::from_le_bytes([cell[0], cell[1]]) as usize;
    &cell[2..2 + key_len]
}

/// Extract the value portion from a leaf cell.
pub fn leaf_cell_value(cell: &[u8]) -> &[u8] {
    let key_len = u16::from_le_bytes([cell[0], cell[1]]) as usize;
    &cell[2 + key_len..]
}

/// Build a leaf cell from a key and value.
pub fn make_leaf_cell(key: &[u8], value: &[u8]) -> Vec<u8> {
    let key_len = key.len() as u16;
    let mut cell = Vec::with_capacity(2 + key.len() + value.len());
    cell.extend_from_slice(&key_len.to_le_bytes());
    cell.extend_from_slice(key);
    cell.extend_from_slice(value);
    cell
}

/// Find the slot index where `key` should be inserted in a leaf page
/// (first slot whose key >= target). Uses binary search.
pub fn leaf_search_slot(page: &Page, key: &[u8]) -> usize {
    let sp = SlottedPageRef::new(page, BTREE_DATA_OFFSET);
    let count = sp.slot_count();
    if count == 0 {
        return 0;
    }

    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        // Skip dead slots during search — dead slots are logically absent.
        // However, binary search requires monotonic ordering; dead slots break
        // that assumption. For correctness we do a linear fallback when we hit
        // a dead slot.
        if sp.is_dead(mid) {
            // Fall back to linear scan from `lo`.
            return leaf_search_slot_linear(page, key, lo, count);
        }
        let cell = sp.cell(mid).unwrap();
        let cell_key = leaf_cell_key(cell);
        match cell_key.cmp(key) {
            Ordering::Less => lo = mid + 1,
            Ordering::Equal | Ordering::Greater => hi = mid,
        }
    }
    lo
}

/// Linear scan fallback for leaf search when dead slots are present.
fn leaf_search_slot_linear(page: &Page, key: &[u8], start: usize, count: usize) -> usize {
    let sp = SlottedPageRef::new(page, BTREE_DATA_OFFSET);
    for i in start..count {
        if sp.is_dead(i) {
            continue;
        }
        let cell = sp.cell(i).unwrap();
        let cell_key = leaf_cell_key(cell);
        if cell_key >= key {
            return i;
        }
    }
    count
}

/// Find the exact slot index of `key` in a leaf page, or `None`.
pub fn leaf_find_exact(page: &Page, key: &[u8]) -> Option<usize> {
    let sp = SlottedPageRef::new(page, BTREE_DATA_OFFSET);
    let idx = leaf_search_slot(page, key);
    if idx < sp.slot_count() && !sp.is_dead(idx) {
        let cell = sp.cell(idx).unwrap();
        if leaf_cell_key(cell) == key {
            return Some(idx);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Internal node helpers
// ---------------------------------------------------------------------------

/// Read the rightmost child pointer from an internal page.
pub fn internal_rightmost_child(page: &Page) -> PageId {
    let data = page.data();
    u64::from_le_bytes(data[32..40].try_into().unwrap())
}

/// Set the rightmost child pointer on an internal page.
pub fn internal_set_rightmost_child(page: &mut Page, child: PageId) {
    let data = page.data_mut();
    data[32..40].copy_from_slice(&child.to_le_bytes());
}

/// Extract the key portion from an internal cell.
///
/// Cell format: `[key_len: u16 LE][key bytes][child_page_id: u64 LE]`
pub fn internal_cell_key(cell: &[u8]) -> &[u8] {
    let key_len = u16::from_le_bytes([cell[0], cell[1]]) as usize;
    &cell[2..2 + key_len]
}

/// Extract the child pointer from an internal cell.
pub fn internal_cell_child(cell: &[u8]) -> PageId {
    let key_len = u16::from_le_bytes([cell[0], cell[1]]) as usize;
    let offset = 2 + key_len;
    u64::from_le_bytes(cell[offset..offset + 8].try_into().unwrap())
}

/// Build an internal cell from a key and child pointer.
pub fn make_internal_cell(key: &[u8], child: PageId) -> Vec<u8> {
    let key_len = key.len() as u16;
    let mut cell = Vec::with_capacity(2 + key.len() + 8);
    cell.extend_from_slice(&key_len.to_le_bytes());
    cell.extend_from_slice(key);
    cell.extend_from_slice(&child.to_le_bytes());
    cell
}

/// Find which child to follow for `key` in an internal page.
///
/// The internal node stores N keys and N+1 children:
///   child_in_cell[0] < key[0] <= child_in_cell[1] < key[1] <= ... <= rightmost
///
/// Each cell's child is for keys LESS THAN that cell's key. The rightmost
/// child handles keys >= all stored keys.
pub fn internal_find_child(page: &Page, key: &[u8]) -> PageId {
    let sp = SlottedPageRef::new(page, BTREE_DATA_OFFSET);
    let count = sp.slot_count();

    // Binary search for the first cell whose key > target key.
    // If target < cell[i].key, follow cell[i].child (left child of that key).
    // If target >= all keys, follow rightmost child.
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let cell = sp.cell(mid).unwrap();
        let cell_key = internal_cell_key(cell);
        if key < cell_key {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }

    // lo is the index of the first key > target. If lo == count, use rightmost.
    if lo < count {
        let cell = sp.cell(lo).unwrap();
        internal_cell_child(cell)
    } else {
        internal_rightmost_child(page)
    }
}

/// Binary search for the slot index of the first key > target in an internal
/// node. Used during insert to determine where to place a split key.
pub fn internal_search_slot(page: &Page, key: &[u8]) -> usize {
    let sp = SlottedPageRef::new(page, BTREE_DATA_OFFSET);
    let count = sp.slot_count();
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let cell = sp.cell(mid).unwrap();
        let cell_key = internal_cell_key(cell);
        if cell_key < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::PageType;
    use crate::storage::slotted::SlottedPage;

    #[test]
    fn test_leaf_next_leaf() {
        let mut page = Page::new(1, PageType::BTreeLeaf);
        assert_eq!(leaf_next_leaf(&page), 0);
        leaf_set_next_leaf(&mut page, 42);
        assert_eq!(leaf_next_leaf(&page), 42);
    }

    #[test]
    fn test_leaf_cell_roundtrip() {
        let cell = make_leaf_cell(b"hello", b"world");
        assert_eq!(leaf_cell_key(&cell), b"hello");
        assert_eq!(leaf_cell_value(&cell), b"world");
    }

    #[test]
    fn test_leaf_cell_empty_value() {
        let cell = make_leaf_cell(b"key", b"");
        assert_eq!(leaf_cell_key(&cell), b"key");
        assert_eq!(leaf_cell_value(&cell), b"");
    }

    #[test]
    fn test_leaf_search_slot_empty() {
        let page = Page::new(1, PageType::BTreeLeaf);
        assert_eq!(leaf_search_slot(&page, b"anything"), 0);
    }

    #[test]
    fn test_leaf_search_slot_ordered() {
        let mut page = Page::new(1, PageType::BTreeLeaf);
        {
            let mut sp = SlottedPage::new(&mut page, BTREE_DATA_OFFSET);
            sp.insert(&make_leaf_cell(b"apple", b"1")).unwrap();
            sp.insert(&make_leaf_cell(b"banana", b"2")).unwrap();
            sp.insert(&make_leaf_cell(b"cherry", b"3")).unwrap();
        }

        assert_eq!(leaf_search_slot(&page, b"apple"), 0);
        assert_eq!(leaf_search_slot(&page, b"banana"), 1);
        assert_eq!(leaf_search_slot(&page, b"cherry"), 2);
        // Before first
        assert_eq!(leaf_search_slot(&page, b"aaa"), 0);
        // After last
        assert_eq!(leaf_search_slot(&page, b"zzz"), 3);
        // Between
        assert_eq!(leaf_search_slot(&page, b"blueberry"), 2);
    }

    #[test]
    fn test_leaf_find_exact() {
        let mut page = Page::new(1, PageType::BTreeLeaf);
        {
            let mut sp = SlottedPage::new(&mut page, BTREE_DATA_OFFSET);
            sp.insert(&make_leaf_cell(b"alpha", b"v1")).unwrap();
            sp.insert(&make_leaf_cell(b"beta", b"v2")).unwrap();
        }

        assert_eq!(leaf_find_exact(&page, b"alpha"), Some(0));
        assert_eq!(leaf_find_exact(&page, b"beta"), Some(1));
        assert_eq!(leaf_find_exact(&page, b"gamma"), None);
    }

    #[test]
    fn test_internal_rightmost_child() {
        let mut page = Page::new(1, PageType::BTreeInternal);
        assert_eq!(internal_rightmost_child(&page), 0);
        internal_set_rightmost_child(&mut page, 99);
        assert_eq!(internal_rightmost_child(&page), 99);
    }

    #[test]
    fn test_internal_cell_roundtrip() {
        let cell = make_internal_cell(b"key", 42);
        assert_eq!(internal_cell_key(&cell), b"key");
        assert_eq!(internal_cell_child(&cell), 42);
    }

    #[test]
    fn test_internal_find_child_single_key() {
        let mut page = Page::new(1, PageType::BTreeInternal);
        internal_set_rightmost_child(&mut page, 20);
        {
            let mut sp = SlottedPage::new(&mut page, BTREE_DATA_OFFSET);
            // Key "M" with left child 10
            sp.insert(&make_internal_cell(b"M", 10)).unwrap();
        }

        // Key < "M" -> child 10 (cell[0].child)
        assert_eq!(internal_find_child(&page, b"A"), 10);
        // Key == "M" -> rightmost child (20), because >= all keys
        assert_eq!(internal_find_child(&page, b"M"), 20);
        // Key > "M" -> rightmost child (20)
        assert_eq!(internal_find_child(&page, b"Z"), 20);
    }

    #[test]
    fn test_internal_find_child_multiple_keys() {
        let mut page = Page::new(1, PageType::BTreeInternal);
        internal_set_rightmost_child(&mut page, 30);
        {
            let mut sp = SlottedPage::new(&mut page, BTREE_DATA_OFFSET);
            sp.insert(&make_internal_cell(b"D", 10)).unwrap();
            sp.insert(&make_internal_cell(b"H", 20)).unwrap();
        }

        // key < "D" -> child 10
        assert_eq!(internal_find_child(&page, b"A"), 10);
        // key == "D" -> child 20 (>= D, < H)
        assert_eq!(internal_find_child(&page, b"D"), 20);
        // key between D and H -> child 20
        assert_eq!(internal_find_child(&page, b"F"), 20);
        // key == "H" -> rightmost (30)
        assert_eq!(internal_find_child(&page, b"H"), 30);
        // key > "H" -> rightmost (30)
        assert_eq!(internal_find_child(&page, b"Z"), 30);
    }

    #[test]
    fn test_internal_search_slot() {
        let mut page = Page::new(1, PageType::BTreeInternal);
        internal_set_rightmost_child(&mut page, 30);
        {
            let mut sp = SlottedPage::new(&mut page, BTREE_DATA_OFFSET);
            sp.insert(&make_internal_cell(b"D", 10)).unwrap();
            sp.insert(&make_internal_cell(b"H", 20)).unwrap();
        }

        // "A" < "D" -> slot 0
        assert_eq!(internal_search_slot(&page, b"A"), 0);
        // "D" == "D" -> slot 0
        assert_eq!(internal_search_slot(&page, b"D"), 0);
        // "F" between D and H -> slot 1
        assert_eq!(internal_search_slot(&page, b"F"), 1);
        // "H" == "H" -> slot 1
        assert_eq!(internal_search_slot(&page, b"H"), 1);
        // "Z" > "H" -> slot 2 (past end)
        assert_eq!(internal_search_slot(&page, b"Z"), 2);
    }
}

//! Core B+Tree operations: search, insert, delete, and range scan.

use crate::error::StorageError;
use crate::storage::page::{Page, PageType};
use crate::storage::slotted::{SlottedPage, SlottedPageRef};
use crate::types::PageId;

use super::PageStore;
use super::node::{
    INTERNAL_DATA_OFFSET, LEAF_DATA_OFFSET, internal_cell_child, internal_cell_child_checksum,
    internal_cell_key, internal_find_child, internal_rightmost_child,
    internal_rightmost_child_checksum, internal_search_slot, internal_set_rightmost_child,
    internal_set_rightmost_child_checksum, leaf_cell_key, leaf_cell_value, leaf_find_exact,
    leaf_next_leaf, leaf_search_slot, leaf_set_next_leaf, make_internal_cell, make_leaf_cell,
};
use super::overflow::{free_overflow_chain, read_value, write_value};

/// A key-value pair returned by range scans.
pub type KeyValuePair = (Vec<u8>, Vec<u8>);

/// Result of inserting into a node. If the node split, the caller must
/// propagate the median key and the new page to the parent.
enum InsertResult {
    /// Insertion completed without splitting. The page ID may have changed
    /// due to copy-on-write allocation.
    Done(PageId),
    /// The node split. `median_key` is the separator key to insert into the
    /// parent, `left_page_id` is the (possibly COW'd) original page, and
    /// `right_page_id` is the new right sibling.
    Split {
        median_key: Vec<u8>,
        left_page_id: PageId,
        right_page_id: PageId,
    },
}

/// Compute the Merkle checksum of a page by reading it from the store.
///
/// This returns the same value that will be stored in a parent's internal
/// node cell for Merkle verification.
fn page_checksum(store: &impl PageStore, page_id: PageId) -> Result<u128, StorageError> {
    let page = store.read_page(page_id)?;
    Ok(page.compute_checksum())
}

/// Verify a child page's checksum against the expected value from its parent.
fn verify_child(
    store: &impl PageStore,
    child_id: PageId,
    expected_checksum: u128,
) -> Result<(), StorageError> {
    let child_page = store.read_page(child_id)?;
    let actual = child_page.compute_checksum();
    if actual != expected_checksum {
        return Err(StorageError::CorruptedPage(format!(
            "Merkle checksum mismatch on child page {child_id}: \
             expected={expected_checksum:#034x}, actual={actual:#034x}"
        )));
    }
    Ok(())
}

/// Search for a key in the B+Tree. Returns the decoded value if found.
///
/// Verifies Merkle checksums on every parent-to-child traversal.
pub fn search(
    store: &impl PageStore,
    root_page: PageId,
    key: &[u8],
) -> Result<Option<Vec<u8>>, StorageError> {
    let mut current_page = store.read_page(root_page)?;

    loop {
        match current_page.page_type()? {
            PageType::BTreeInternal => {
                let (child_id, expected_cs) = internal_find_child(&current_page, key);
                current_page = store.read_page(child_id)?;
                let actual_cs = current_page.compute_checksum();
                if actual_cs != expected_cs {
                    return Err(StorageError::CorruptedPage(format!(
                        "Merkle checksum mismatch on child page {child_id}: \
                         expected={expected_cs:#034x}, actual={actual_cs:#034x}"
                    )));
                }
            }
            PageType::BTreeLeaf => {
                if let Some(slot_idx) = leaf_find_exact(&current_page, key) {
                    let sp = SlottedPageRef::new(&current_page, LEAF_DATA_OFFSET);
                    let cell = sp.cell(slot_idx).unwrap();
                    let raw_value = leaf_cell_value(cell);
                    let value = read_value(store, raw_value)?;
                    return Ok(Some(value));
                } else {
                    return Ok(None);
                }
            }
            _ => {
                return Err(StorageError::CorruptedPage(
                    "unexpected page type during search".to_string(),
                ));
            }
        }
    }
}

/// Insert a key-value pair into the B+Tree. Returns the (possibly new) root
/// page ID if the root had to be split and a new root was created.
pub fn insert(
    store: &mut impl PageStore,
    root_page: PageId,
    key: &[u8],
    value: &[u8],
) -> Result<PageId, StorageError> {
    let result = insert_recursive(store, root_page, key, value)?;
    match result {
        InsertResult::Done(new_root) => Ok(new_root),
        InsertResult::Split {
            median_key,
            left_page_id,
            right_page_id,
        } => {
            // Root split: create a new root with one key.
            let left_cs = page_checksum(store, left_page_id)?;
            let right_cs = page_checksum(store, right_page_id)?;

            let mut new_root = store.allocate_page(PageType::BTreeInternal)?;
            let new_root_id = new_root.page_id();

            // The (possibly COW'd) old root becomes the left child.
            // The new page becomes the rightmost child.
            let cell = make_internal_cell(&median_key, left_page_id, left_cs);
            {
                let mut sp = SlottedPage::new(&mut new_root, INTERNAL_DATA_OFFSET);
                sp.insert(&cell)?;
            }

            internal_set_rightmost_child(&mut new_root, right_page_id);
            internal_set_rightmost_child_checksum(&mut new_root, right_cs);
            store.write_page(new_root)?;

            Ok(new_root_id)
        }
    }
}

fn insert_recursive(
    store: &mut impl PageStore,
    page_id: PageId,
    key: &[u8],
    value: &[u8],
) -> Result<InsertResult, StorageError> {
    let page = store.read_page(page_id)?;
    match page.page_type()? {
        PageType::BTreeInternal => insert_into_internal(store, page, key, value),
        PageType::BTreeLeaf => insert_into_leaf(store, page, key, value),
        _ => Err(StorageError::CorruptedPage(
            "unexpected page type during insert".to_string(),
        )),
    }
}

fn insert_into_leaf(
    store: &mut impl PageStore,
    mut page: Page,
    key: &[u8],
    value: &[u8],
) -> Result<InsertResult, StorageError> {
    // Encode value (may be inline or overflow).
    let encoded_value = write_value(store, value, key.len())?;
    let new_cell = make_leaf_cell(key, &encoded_value);

    // Check if key already exists.
    if let Some(slot_idx) = leaf_find_exact(&page, key) {
        // Replace: free old overflow if any, then update the cell.
        let old_raw_value = {
            let sp_ref = SlottedPageRef::new(&page, LEAF_DATA_OFFSET);
            let old_cell = sp_ref.cell(slot_idx).unwrap();
            leaf_cell_value(old_cell).to_vec()
        };

        free_overflow_chain(store, &old_raw_value)?;

        // Mark old slot dead, compact, then insert replacement in order.
        {
            let mut sp = SlottedPage::new(&mut page, LEAF_DATA_OFFSET);
            sp.mark_dead(slot_idx);
            sp.compact();
        }

        // Re-find insertion point after compaction.
        let insert_idx = leaf_search_slot(&page, key);
        {
            let mut sp = SlottedPage::new(&mut page, LEAF_DATA_OFFSET);
            sp.insert_at(insert_idx, &new_cell)?;
        }

        let new_page_id = store.cow_write_page(page)?;
        return Ok(InsertResult::Done(new_page_id));
    }

    // Try to insert directly.
    let insert_idx = leaf_search_slot(&page, key);
    let space_needed = new_cell.len() + 4; // cell + slot entry
    let free = {
        let sp_ref = SlottedPageRef::new(&page, LEAF_DATA_OFFSET);
        sp_ref.free_space()
    };

    if free >= space_needed {
        {
            let mut sp = SlottedPage::new(&mut page, LEAF_DATA_OFFSET);
            sp.insert_at(insert_idx, &new_cell)?;
        }
        let new_page_id = store.cow_write_page(page)?;
        return Ok(InsertResult::Done(new_page_id));
    }

    // Not enough space -- split the leaf.
    split_leaf(store, page, &new_cell)
}

fn split_leaf(
    store: &mut impl PageStore,
    mut old_page: Page,
    new_cell: &[u8],
) -> Result<InsertResult, StorageError> {
    // Collect all live cells + the new cell in sorted order.
    let mut all_cells: Vec<Vec<u8>> = {
        let sp_ref = SlottedPageRef::new(&old_page, LEAF_DATA_OFFSET);
        let count = sp_ref.slot_count();
        let mut cells = Vec::with_capacity(count + 1);
        for i in 0..count {
            if !sp_ref.is_dead(i) {
                cells.push(sp_ref.cell(i).unwrap().to_vec());
            }
        }
        cells
    };

    // Insert new cell at the correct position among live cells.
    let live_insert_idx = find_live_insert_idx(&all_cells, new_cell);
    all_cells.insert(live_insert_idx, new_cell.to_vec());

    let total = all_cells.len();
    let mid = total / 2;

    // The median key (first key of the right half) goes to the parent.
    let median_key = leaf_cell_key(&all_cells[mid]).to_vec();

    // Allocate new right leaf.
    let mut new_page = store.allocate_page(PageType::BTreeLeaf)?;
    let new_page_id = new_page.page_id();

    // Link: new_page.next_leaf = old_page.next_leaf; old_page.next_leaf = new_page
    let old_next = leaf_next_leaf(&old_page);
    leaf_set_next_leaf(&mut new_page, old_next);
    leaf_set_next_leaf(&mut old_page, new_page_id);

    // Clear old page slots and rebuild with left half.
    clear_page_slots(&mut old_page, LEAF_DATA_OFFSET);
    {
        let mut sp = SlottedPage::new(&mut old_page, LEAF_DATA_OFFSET);
        for cell in &all_cells[..mid] {
            sp.insert(cell)?;
        }
    }

    // Fill new page with right half.
    {
        let mut sp = SlottedPage::new(&mut new_page, LEAF_DATA_OFFSET);
        for cell in &all_cells[mid..] {
            sp.insert(cell)?;
        }
    }

    let left_page_id = store.cow_write_page(old_page)?;
    store.write_page(new_page)?;

    Ok(InsertResult::Split {
        median_key,
        left_page_id,
        right_page_id: new_page_id,
    })
}

/// Find the correct insert position for `new_cell` among `live_cells` by
/// comparing keys.
fn find_live_insert_idx(live_cells: &[Vec<u8>], new_cell: &[u8]) -> usize {
    let new_key = leaf_cell_key(new_cell);
    for (i, cell) in live_cells.iter().enumerate() {
        let cell_key = leaf_cell_key(cell);
        if cell_key >= new_key {
            return i;
        }
    }
    live_cells.len()
}

fn insert_into_internal(
    store: &mut impl PageStore,
    page: Page,
    key: &[u8],
    value: &[u8],
) -> Result<InsertResult, StorageError> {
    let (child_id, _expected_cs) = internal_find_child(&page, key);
    let page_id = page.page_id();
    // Let the page go out of scope before the recursive call; we re-read it
    // afterwards if we need to insert a split key.
    let page_type = page.page_type()?;
    debug_assert_eq!(page_type, PageType::BTreeInternal);
    // Explicitly consume `page` by moving it into a block that ends.
    {
        let _consumed = page;
    }

    let child_result = insert_recursive(store, child_id, key, value)?;

    match child_result {
        InsertResult::Done(new_child_id) => {
            if new_child_id != child_id {
                // Child was COW'd — update our child pointer + checksum and COW ourselves.
                let new_child_cs = page_checksum(store, new_child_id)?;
                let mut page = store.read_page(page_id)?;
                update_internal_child_by_value(&mut page, child_id, new_child_id, new_child_cs);
                let new_page_id = store.cow_write_page(page)?;
                Ok(InsertResult::Done(new_page_id))
            } else {
                Ok(InsertResult::Done(page_id))
            }
        }
        InsertResult::Split {
            median_key,
            left_page_id,
            right_page_id,
        } => {
            let left_cs = page_checksum(store, left_page_id)?;
            let right_cs = page_checksum(store, right_page_id)?;

            // Re-read the internal page to insert the split key.
            let mut page = store.read_page(page_id)?;
            // Use left_page_id (possibly COW'd child) as the left child of the median.
            let new_cell = make_internal_cell(&median_key, left_page_id, left_cs);

            // Find where to insert the median_key.
            let insert_idx = internal_search_slot(&page, &median_key);
            let (count, free) = {
                let sp_ref = SlottedPageRef::new(&page, INTERNAL_DATA_OFFSET);
                (sp_ref.slot_count(), sp_ref.free_space())
            };

            let space_needed = new_cell.len() + 4;

            if free >= space_needed {
                {
                    let mut sp = SlottedPage::new(&mut page, INTERNAL_DATA_OFFSET);
                    sp.insert_at(insert_idx, &new_cell)?;
                }

                // Update the child pointer on the RIGHT side of the median
                // to point to right_page_id (the new right sibling).
                if insert_idx + 1 < count + 1 {
                    update_internal_cell_child(&mut page, insert_idx + 1, right_page_id, right_cs);
                } else {
                    internal_set_rightmost_child(&mut page, right_page_id);
                    internal_set_rightmost_child_checksum(&mut page, right_cs);
                }

                let new_page_id = store.cow_write_page(page)?;
                Ok(InsertResult::Done(new_page_id))
            } else {
                // Internal node is full -- must split it.
                split_internal(
                    store,
                    page,
                    &median_key,
                    left_page_id,
                    left_cs,
                    right_page_id,
                    right_cs,
                )
            }
        }
    }
}

/// Update a child pointer in an internal node by searching for the old child
/// value and replacing it with the new one. Handles both cell children and
/// the rightmost child pointer.
fn update_internal_child_by_value(
    page: &mut Page,
    old_child: PageId,
    new_child: PageId,
    new_child_checksum: u128,
) {
    // Check rightmost child first.
    if internal_rightmost_child(page) == old_child {
        internal_set_rightmost_child(page, new_child);
        internal_set_rightmost_child_checksum(page, new_child_checksum);
        return;
    }
    // Search cells for the old child pointer.
    let slot_idx = {
        let sp_ref = SlottedPageRef::new(page, INTERNAL_DATA_OFFSET);
        let count = sp_ref.slot_count();
        let mut found = None;
        for i in 0..count {
            let cell = sp_ref.cell(i).unwrap();
            if internal_cell_child(cell) == old_child {
                found = Some(i);
                break;
            }
        }
        found
    };
    if let Some(idx) = slot_idx {
        update_internal_cell_child(page, idx, new_child, new_child_checksum);
    }
}

/// Update the child pointer and checksum in an internal cell at the given slot index.
fn update_internal_cell_child(
    page: &mut Page,
    slot_index: usize,
    new_child: PageId,
    new_child_checksum: u128,
) {
    let cell = {
        let sp_ref = SlottedPageRef::new(page, INTERNAL_DATA_OFFSET);
        sp_ref.cell(slot_index).unwrap().to_vec()
    };

    let cell_key = internal_cell_key(&cell).to_vec();
    let new_cell = make_internal_cell(&cell_key, new_child, new_child_checksum);

    {
        let mut sp = SlottedPage::new(page, INTERNAL_DATA_OFFSET);
        sp.mark_dead(slot_index);
        sp.compact();
    }

    // Re-find position after compact.
    let insert_pos = internal_search_slot(page, &cell_key);
    let mut sp = SlottedPage::new(page, INTERNAL_DATA_OFFSET);
    sp.insert_at(insert_pos, &new_cell).unwrap();
}

fn split_internal(
    store: &mut impl PageStore,
    mut old_page: Page,
    median_key: &[u8],
    left_child: PageId,
    left_child_checksum: u128,
    right_child: PageId,
    right_child_checksum: u128,
) -> Result<InsertResult, StorageError> {
    // Collect all cells + the new cell.
    let old_rightmost = internal_rightmost_child(&old_page);
    let old_rightmost_cs = internal_rightmost_child_checksum(&old_page);

    let mut all_cells: Vec<Vec<u8>> = {
        let sp_ref = SlottedPageRef::new(&old_page, INTERNAL_DATA_OFFSET);
        let count = sp_ref.slot_count();
        let mut cells = Vec::with_capacity(count + 1);
        for i in 0..count {
            cells.push(sp_ref.cell(i).unwrap().to_vec());
        }
        cells
    };

    // Build the new cell for the median key from the child split.
    let new_cell = make_internal_cell(median_key, left_child, left_child_checksum);

    // Find insertion position.
    let insert_idx = find_internal_insert_idx(&all_cells, median_key);
    all_cells.insert(insert_idx, new_cell);

    // Extract all children and checksums in order:
    let mut children: Vec<PageId> = Vec::with_capacity(all_cells.len() + 1);
    let mut checksums: Vec<u128> = Vec::with_capacity(all_cells.len() + 1);
    for cell in &all_cells {
        children.push(internal_cell_child(cell));
        checksums.push(internal_cell_child_checksum(cell));
    }
    children.push(old_rightmost);
    checksums.push(old_rightmost_cs);

    // The slot we just inserted at `insert_idx` has child = left_child.
    // The slot/rightmost at insert_idx+1 should become right_child.
    children[insert_idx + 1] = right_child;
    checksums[insert_idx + 1] = right_child_checksum;

    // Now split at the midpoint. The middle key gets promoted to parent.
    let total = all_cells.len();
    let mid = total / 2;

    // Promoted key.
    let promoted_key = internal_cell_key(&all_cells[mid]).to_vec();

    // Left half: keys 0..mid with children 0..mid, rightmost = children[mid]
    // Right half: keys mid+1..total with children mid+1..total, rightmost = children[total]

    // Rebuild old page as left half.
    clear_page_slots(&mut old_page, INTERNAL_DATA_OFFSET);
    {
        let mut sp = SlottedPage::new(&mut old_page, INTERNAL_DATA_OFFSET);
        for i in 0..mid {
            let key = internal_cell_key(&all_cells[i]).to_vec();
            let cell = make_internal_cell(&key, children[i], checksums[i]);
            sp.insert(&cell)?;
        }
    }
    internal_set_rightmost_child(&mut old_page, children[mid]);
    internal_set_rightmost_child_checksum(&mut old_page, checksums[mid]);

    // Allocate new right page.
    let mut new_page = store.allocate_page(PageType::BTreeInternal)?;
    let new_page_id = new_page.page_id();
    {
        let mut sp = SlottedPage::new(&mut new_page, INTERNAL_DATA_OFFSET);
        for i in (mid + 1)..total {
            let key = internal_cell_key(&all_cells[i]).to_vec();
            let cell = make_internal_cell(&key, children[i], checksums[i]);
            sp.insert(&cell)?;
        }
    }
    internal_set_rightmost_child(&mut new_page, children[total]);
    internal_set_rightmost_child_checksum(&mut new_page, checksums[total]);

    let left_page_id = store.cow_write_page(old_page)?;
    store.write_page(new_page)?;

    Ok(InsertResult::Split {
        median_key: promoted_key,
        left_page_id,
        right_page_id: new_page_id,
    })
}

fn find_internal_insert_idx(cells: &[Vec<u8>], key: &[u8]) -> usize {
    for (i, cell) in cells.iter().enumerate() {
        let cell_key = internal_cell_key(cell);
        if cell_key >= key {
            return i;
        }
    }
    cells.len()
}

/// Delete a key from the B+Tree. Returns the (possibly new) root page ID.
///
/// This is a simplified v1 delete: it finds the leaf containing the key and
/// marks the slot as dead. No rebalancing or merging is performed.
/// With COW, modified pages get new page IDs that propagate to the root.
pub fn delete(
    store: &mut impl PageStore,
    root_page: PageId,
    key: &[u8],
) -> Result<PageId, StorageError> {
    let (_found, new_root) = delete_recursive(store, root_page, key)?;
    Ok(new_root)
}

/// Returns `(key_was_found, possibly_new_page_id)`.
fn delete_recursive(
    store: &mut impl PageStore,
    page_id: PageId,
    key: &[u8],
) -> Result<(bool, PageId), StorageError> {
    let page = store.read_page(page_id)?;
    match page.page_type()? {
        PageType::BTreeInternal => {
            let (child_id, _expected_cs) = internal_find_child(&page, key);
            // Let `page` go out of scope before the recursive call.
            {
                let _consumed = page;
            }
            let (found, new_child_id) = delete_recursive(store, child_id, key)?;
            if found && new_child_id != child_id {
                // Child was COW'd — update our child pointer + checksum and COW ourselves.
                let new_child_cs = page_checksum(store, new_child_id)?;
                let mut page = store.read_page(page_id)?;
                update_internal_child_by_value(&mut page, child_id, new_child_id, new_child_cs);
                let new_page_id = store.cow_write_page(page)?;
                Ok((true, new_page_id))
            } else {
                Ok((found, page_id))
            }
        }
        PageType::BTreeLeaf => {
            if let Some(slot_idx) = leaf_find_exact(&page, key) {
                // Free overflow pages if value is overflow.
                let raw_value = {
                    let sp_ref = SlottedPageRef::new(&page, LEAF_DATA_OFFSET);
                    let cell = sp_ref.cell(slot_idx).unwrap();
                    leaf_cell_value(cell).to_vec()
                };

                free_overflow_chain(store, &raw_value)?;

                let mut page = page;
                {
                    let mut sp = SlottedPage::new(&mut page, LEAF_DATA_OFFSET);
                    sp.mark_dead(slot_idx);
                }
                let new_page_id = store.cow_write_page(page)?;
                Ok((true, new_page_id))
            } else {
                Ok((false, page_id))
            }
        }
        _ => Err(StorageError::CorruptedPage(
            "unexpected page type during delete".to_string(),
        )),
    }
}

/// Range scan: returns all key-value pairs where start_key <= key < end_key.
///
/// If `start_key` is `None`, scanning starts from the very first leaf entry.
/// If `end_key` is `None`, scanning continues to the end of the tree.
///
/// Uses tree-structure traversal (not `next_leaf` pointers) so that scans are
/// correct even after copy-on-write page allocation reassigns page IDs.
/// Verifies Merkle checksums on every parent-to-child traversal.
pub fn range_scan(
    store: &impl PageStore,
    root_page: PageId,
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
) -> Result<Vec<KeyValuePair>, StorageError> {
    let mut results = Vec::new();
    tree_range_scan(store, root_page, start_key, end_key, &mut results)?;
    Ok(results)
}

/// Recursive tree-structure range scan. Returns `false` when `end_key` is
/// reached and the caller should stop visiting further children.
fn tree_range_scan(
    store: &impl PageStore,
    page_id: PageId,
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
    results: &mut Vec<KeyValuePair>,
) -> Result<bool, StorageError> {
    let page = store.read_page(page_id)?;
    match page.page_type()? {
        PageType::BTreeLeaf => {
            let sp = SlottedPageRef::new(&page, LEAF_DATA_OFFSET);
            let count = sp.slot_count();
            for i in 0..count {
                if sp.is_dead(i) {
                    continue;
                }
                let cell = sp.cell(i).unwrap();
                let cell_key = leaf_cell_key(cell);

                if let Some(sk) = start_key
                    && cell_key < sk
                {
                    continue;
                }
                if let Some(ek) = end_key
                    && cell_key >= ek
                {
                    return Ok(false);
                }

                let raw_value = leaf_cell_value(cell);
                let value = read_value(store, raw_value)?;
                results.push((cell_key.to_vec(), value));
            }
            Ok(true)
        }
        PageType::BTreeInternal => {
            let sp_ref = SlottedPageRef::new(&page, INTERNAL_DATA_OFFSET);
            let count = sp_ref.slot_count();

            // Determine which child to start scanning from.
            let start_child = if let Some(sk) = start_key {
                let mut idx = 0;
                for i in 0..count {
                    let cell = sp_ref.cell(i).unwrap();
                    let sep = internal_cell_key(cell);
                    if sep <= sk {
                        idx = i + 1;
                    } else {
                        break;
                    }
                }
                idx
            } else {
                0
            };

            for i in start_child..=count {
                // If this child's lower bound >= end_key, all remaining children
                // are past the scan range.
                if let Some(ek) = end_key
                    && i > 0
                {
                    let prev_cell = sp_ref.cell(i - 1).unwrap();
                    let lower = internal_cell_key(prev_cell);
                    if lower >= ek {
                        return Ok(false);
                    }
                }

                let (child_id, expected_cs) = if i < count {
                    let cell = sp_ref.cell(i).unwrap();
                    (
                        internal_cell_child(cell),
                        internal_cell_child_checksum(cell),
                    )
                } else {
                    (
                        internal_rightmost_child(&page),
                        internal_rightmost_child_checksum(&page),
                    )
                };

                // Verify Merkle checksum before descending.
                verify_child(store, child_id, expected_cs)?;

                if !tree_range_scan(store, child_id, start_key, end_key, results)? {
                    return Ok(false);
                }
            }

            Ok(true)
        }
        _ => Err(StorageError::CorruptedPage(
            "unexpected page type during scan".to_string(),
        )),
    }
}

/// Clear all slot data from a page, resetting it to an empty slotted state
/// while preserving the page type, page ID, and fields before `data_offset`.
fn clear_page_slots(page: &mut Page, data_offset: usize) {
    // Reset entry count to 0 and free space offset to end of page.
    page.set_entry_count(0);
    page.set_free_space_offset(crate::types::PAGE_SIZE as u32);

    // Zero out the slot array and cell data regions.
    let data = page.data_mut();
    for byte in &mut data[data_offset..] {
        *byte = 0;
    }
    page.mark_dirty();
}

/// Recursively free all pages in a B+Tree (internal nodes, leaf nodes).
///
/// Walks the tree depth-first, freeing each page. After this call the
/// tree's pages are returned to the free list and the root page ID is
/// no longer valid.
pub fn free_tree(store: &mut impl PageStore, page_id: PageId) -> Result<(), StorageError> {
    let page = store.read_page(page_id)?;
    match page.page_type()? {
        PageType::BTreeInternal => {
            let sp = SlottedPageRef::new(&page, INTERNAL_DATA_OFFSET);
            // Free each child referenced by cells.
            for i in 0..sp.slot_count() {
                let cell = sp.cell(i).expect("cell missing in internal node");
                let child_id = internal_cell_child(cell);
                free_tree(store, child_id)?;
            }
            // Free the rightmost child.
            let rightmost = internal_rightmost_child(&page);
            if rightmost != 0 {
                free_tree(store, rightmost)?;
            }
            // Free this internal page.
            store.free_page(page_id)?;
        }
        PageType::BTreeLeaf => {
            // Leaf node — just free it. No overflow pages in index trees
            // (index values are empty).
            store.free_page(page_id)?;
        }
        other => {
            return Err(StorageError::CorruptedPage(format!(
                "unexpected page type {:?} in tree walk",
                other
            )));
        }
    }
    Ok(())
}

/// Create an initial empty B+Tree. Returns the root page ID.
pub fn create_tree(store: &mut impl PageStore) -> Result<PageId, StorageError> {
    let page = store.allocate_page(PageType::BTreeLeaf)?;
    let page_id = page.page_id();
    store.write_page(page)?;
    Ok(page_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::InMemoryPageStore;

    /// Helper to create a fresh tree and return (store, root_id).
    fn setup() -> (InMemoryPageStore, PageId) {
        let mut store = InMemoryPageStore::new();
        let root = create_tree(&mut store).unwrap();
        (store, root)
    }

    #[test]
    fn test_insert_and_search() {
        let (mut store, mut root) = setup();
        root = insert(&mut store, root, b"key1", b"value1").unwrap();
        let result = search(&store, root, b"key1").unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_insert_multiple() {
        let (mut store, mut root) = setup();
        for i in 0..10u32 {
            let key = format!("key{i:03}");
            let val = format!("val{i:03}");
            root = insert(&mut store, root, key.as_bytes(), val.as_bytes()).unwrap();
        }

        for i in 0..10u32 {
            let key = format!("key{i:03}");
            let val = format!("val{i:03}");
            let result = search(&store, root, key.as_bytes()).unwrap();
            assert_eq!(result, Some(val.into_bytes()));
        }
    }

    #[test]
    fn test_insert_overwrites() {
        let (mut store, mut root) = setup();
        root = insert(&mut store, root, b"key", b"old_value").unwrap();
        root = insert(&mut store, root, b"key", b"new_value").unwrap();
        let result = search(&store, root, b"key").unwrap();
        assert_eq!(result, Some(b"new_value".to_vec()));
    }

    #[test]
    fn test_search_missing() {
        let (mut store, mut root) = setup();
        root = insert(&mut store, root, b"exists", b"yes").unwrap();
        let result = search(&store, root, b"missing").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_leaf_split() {
        let (mut store, mut root) = setup();
        // Insert enough items to force at least one leaf split.
        for i in 0..100u32 {
            let key = format!("key_{i:05}");
            let val = format!("value_{i:05}_padding_to_make_it_bigger");
            root = insert(&mut store, root, key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Verify all items are still searchable.
        for i in 0..100u32 {
            let key = format!("key_{i:05}");
            let val = format!("value_{i:05}_padding_to_make_it_bigger");
            let result = search(&store, root, key.as_bytes()).unwrap();
            assert_eq!(result, Some(val.into_bytes()), "failed to find key {key}");
        }

        // Verify the root is now an internal node (split happened).
        let root_page = store.read_page(root).unwrap();
        assert_eq!(
            root_page.page_type().unwrap(),
            PageType::BTreeInternal,
            "root should be internal after splits"
        );
    }

    #[test]
    fn test_multi_level_split() {
        let (mut store, mut root) = setup();
        // Use large values (~1000 bytes) so each leaf holds only ~3-4 entries.
        let padding = "x".repeat(1000);
        for i in 0..1000u32 {
            let key = format!("k{i:06}");
            let val = format!("v{i:06}_{padding}");
            root = insert(&mut store, root, key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Verify all items.
        for i in 0..1000u32 {
            let key = format!("k{i:06}");
            let val = format!("v{i:06}_{padding}");
            let result = search(&store, root, key.as_bytes()).unwrap();
            assert_eq!(result, Some(val.into_bytes()), "failed at key k{i:06}");
        }

        // Verify tree depth >= 3 by counting levels.
        let depth = measure_depth(&store, root);
        assert!(depth >= 3, "expected depth >= 3, got {depth}");
    }

    fn measure_depth(store: &InMemoryPageStore, page_id: PageId) -> usize {
        let page = store.read_page(page_id).unwrap();
        match page.page_type().unwrap() {
            PageType::BTreeLeaf => 1,
            PageType::BTreeInternal => {
                let child = internal_rightmost_child(&page);
                1 + measure_depth(store, child)
            }
            _ => panic!("unexpected page type"),
        }
    }

    #[test]
    fn test_delete_basic() {
        let (mut store, mut root) = setup();
        root = insert(&mut store, root, b"key1", b"val1").unwrap();
        root = insert(&mut store, root, b"key2", b"val2").unwrap();

        root = delete(&mut store, root, b"key1").unwrap();
        assert_eq!(search(&store, root, b"key1").unwrap(), None);
        assert_eq!(
            search(&store, root, b"key2").unwrap(),
            Some(b"val2".to_vec())
        );
    }

    #[test]
    fn test_delete_nonexistent() {
        let (mut store, mut root) = setup();
        root = insert(&mut store, root, b"key1", b"val1").unwrap();
        // Deleting a non-existent key should not error.
        root = delete(&mut store, root, b"nope").unwrap();
        assert_eq!(
            search(&store, root, b"key1").unwrap(),
            Some(b"val1".to_vec())
        );
    }

    #[test]
    fn test_range_scan() {
        let (mut store, mut root) = setup();
        for i in 0..20u32 {
            let key = format!("k{i:03}");
            let val = format!("v{i:03}");
            root = insert(&mut store, root, key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Scan k005..k010 (should include k005,k006,k007,k008,k009)
        let results = range_scan(&store, root, Some(b"k005"), Some(b"k010")).unwrap();

        assert_eq!(results.len(), 5);
        for (i, (key, val)) in results.iter().enumerate() {
            let expected_key = format!("k{:03}", i + 5);
            let expected_val = format!("v{:03}", i + 5);
            assert_eq!(key, expected_key.as_bytes());
            assert_eq!(val, expected_val.as_bytes());
        }
    }

    #[test]
    fn test_range_scan_full() {
        let (mut store, mut root) = setup();
        for i in 0..10u32 {
            let key = format!("k{i:03}");
            let val = format!("v{i:03}");
            root = insert(&mut store, root, key.as_bytes(), val.as_bytes()).unwrap();
        }

        let results = range_scan(&store, root, None, None).unwrap();
        assert_eq!(results.len(), 10);
    }

    #[test]
    fn test_range_scan_skips_dead() {
        let (mut store, mut root) = setup();
        for i in 0..10u32 {
            let key = format!("k{i:03}");
            let val = format!("v{i:03}");
            root = insert(&mut store, root, key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Delete some
        root = delete(&mut store, root, b"k003").unwrap();
        root = delete(&mut store, root, b"k007").unwrap();

        let results = range_scan(&store, root, None, None).unwrap();
        assert_eq!(results.len(), 8);

        let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();
        assert!(!keys.contains(&b"k003".to_vec()));
        assert!(!keys.contains(&b"k007".to_vec()));
    }

    #[test]
    fn test_overflow_value() {
        let (mut store, mut root) = setup();
        let big_value = vec![0xAB; 2000]; // > OVERFLOW_THRESHOLD
        root = insert(&mut store, root, b"bigkey", &big_value).unwrap();

        let result = search(&store, root, b"bigkey").unwrap();
        assert_eq!(result, Some(big_value));
    }

    #[test]
    fn test_stress_1000() {
        let (mut store, mut root) = setup();

        // Insert 1000 items.
        for i in 0..1000u32 {
            let key = format!("stress_{i:06}");
            let val = format!("data_{i:06}");
            root = insert(&mut store, root, key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Verify all 1000 are searchable.
        for i in 0..1000u32 {
            let key = format!("stress_{i:06}");
            let val = format!("data_{i:06}");
            let result = search(&store, root, key.as_bytes()).unwrap();
            assert_eq!(
                result,
                Some(val.into_bytes()),
                "missing key after insert: stress_{i:06}"
            );
        }

        // Delete 500 (even-numbered).
        for i in (0..1000u32).step_by(2) {
            let key = format!("stress_{i:06}");
            root = delete(&mut store, root, key.as_bytes()).unwrap();
        }

        // Verify the 500 deleted are gone.
        for i in (0..1000u32).step_by(2) {
            let key = format!("stress_{i:06}");
            let result = search(&store, root, key.as_bytes()).unwrap();
            assert_eq!(result, None, "key should be deleted: stress_{i:06}");
        }

        // Verify the 500 remaining are still present.
        for i in (1..1000u32).step_by(2) {
            let key = format!("stress_{i:06}");
            let val = format!("data_{i:06}");
            let result = search(&store, root, key.as_bytes()).unwrap();
            assert_eq!(
                result,
                Some(val.into_bytes()),
                "missing key after delete: stress_{i:06}"
            );
        }
    }

    #[test]
    fn test_empty_tree_search() {
        let (store, root) = setup();
        assert_eq!(search(&store, root, b"anything").unwrap(), None);
    }

    #[test]
    fn test_empty_tree_range_scan() {
        let (store, root) = setup();
        let results = range_scan(&store, root, None, None).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_insert_reverse_order() {
        let (mut store, mut root) = setup();
        // Insert in reverse order to stress the insert_at logic.
        for i in (0..100u32).rev() {
            let key = format!("rev_{i:05}");
            let val = format!("val_{i:05}");
            root = insert(&mut store, root, key.as_bytes(), val.as_bytes()).unwrap();
        }

        for i in 0..100u32 {
            let key = format!("rev_{i:05}");
            let val = format!("val_{i:05}");
            let result = search(&store, root, key.as_bytes()).unwrap();
            assert_eq!(result, Some(val.into_bytes()));
        }
    }

    #[test]
    fn test_range_scan_with_splits() {
        let (mut store, mut root) = setup();
        // Insert enough to force multiple leaf pages.
        for i in 0..200u32 {
            let key = format!("rs_{i:05}");
            let val = format!("v_{i:05}");
            root = insert(&mut store, root, key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Full range scan should return all 200 items in order.
        let results = range_scan(&store, root, None, None).unwrap();
        assert_eq!(results.len(), 200);

        // Verify ordering.
        for i in 0..results.len() - 1 {
            assert!(results[i].0 < results[i + 1].0, "results not in order");
        }
    }

    #[test]
    fn test_range_scan_open_ended_start() {
        let (mut store, mut root) = setup();
        for i in 0..10u32 {
            let key = format!("k{i:03}");
            let val = format!("v{i:03}");
            root = insert(&mut store, root, key.as_bytes(), val.as_bytes()).unwrap();
        }

        // No start key, end at k005 -> k000..k004 (5 items)
        let results = range_scan(&store, root, None, Some(b"k005")).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_range_scan_open_ended_end() {
        let (mut store, mut root) = setup();
        for i in 0..10u32 {
            let key = format!("k{i:03}");
            let val = format!("v{i:03}");
            root = insert(&mut store, root, key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Start at k005, no end key -> k005..k009 (5 items)
        let results = range_scan(&store, root, Some(b"k005"), None).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_overwrite_with_overflow() {
        let (mut store, mut root) = setup();
        let small_value = b"small";
        let big_value = vec![0xCD; 2000];

        root = insert(&mut store, root, b"key", small_value).unwrap();
        assert_eq!(
            search(&store, root, b"key").unwrap(),
            Some(small_value.to_vec())
        );

        // Overwrite with big value.
        root = insert(&mut store, root, b"key", &big_value).unwrap();
        assert_eq!(
            search(&store, root, b"key").unwrap(),
            Some(big_value.clone())
        );

        // Overwrite back to small.
        root = insert(&mut store, root, b"key", small_value).unwrap();
        assert_eq!(
            search(&store, root, b"key").unwrap(),
            Some(small_value.to_vec())
        );
    }

    #[test]
    fn test_delete_overflow_value() {
        let (mut store, mut root) = setup();
        let big_value = vec![0xEF; 3000];
        root = insert(&mut store, root, b"big", &big_value).unwrap();
        root = delete(&mut store, root, b"big").unwrap();
        assert_eq!(search(&store, root, b"big").unwrap(), None);
    }

    #[test]
    fn test_create_tree() {
        let mut store = InMemoryPageStore::new();
        let root = create_tree(&mut store).unwrap();
        let page = store.read_page(root).unwrap();
        assert_eq!(page.page_type().unwrap(), PageType::BTreeLeaf);
        assert_eq!(page.entry_count(), 0);
    }

    #[test]
    fn test_merkle_checksum_detects_corruption() {
        let (mut store, mut root) = setup();
        // Insert enough to create an internal node.
        for i in 0..100u32 {
            let key = format!("mk_{i:05}");
            let val = format!("v_{i:05}_padding_to_fill_page_faster_xxxxxxxxxxx");
            root = insert(&mut store, root, key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Verify the root is an internal node.
        let root_page = store.read_page(root).unwrap();
        assert_eq!(
            root_page.page_type().unwrap(),
            PageType::BTreeInternal,
            "root should be internal"
        );

        // Find a child page via the root.
        let (child_id, _expected_cs) = internal_find_child(&root_page, b"mk_00050");

        // Corrupt the child page by flipping a byte in the data region.
        let mut child_buf = *store.read_page(child_id).unwrap().data();
        child_buf[200] ^= 0xFF;
        store.pages.insert(child_id, child_buf);

        // Searching should now detect the Merkle checksum mismatch.
        let result = search(&store, root, b"mk_00050");
        assert!(
            result.is_err(),
            "search should fail due to Merkle checksum mismatch"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Merkle checksum mismatch"),
            "error should mention Merkle: {err_msg}"
        );
    }
}

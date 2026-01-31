use crate::error::StorageError;
use crate::types::{PAGE_SIZE, SLOT_SIZE};

use super::page::Page;

/// Read-only view of a slotted page.
///
/// Slot array starts at `data_offset` and grows forward. Each slot is 4 bytes:
/// `(cell_offset: u16, cell_length: u16)` in little-endian.
///
/// Cell data starts at the end of the page and grows backward. The page header's
/// `free_space_offset` tracks where the next cell can be written (the lowest
/// occupied byte).
pub struct SlottedPageRef<'a> {
    page: &'a Page,
    data_offset: usize,
}

impl<'a> SlottedPageRef<'a> {
    /// Create a read-only slotted page view.
    pub fn new(page: &'a Page, data_offset: usize) -> Self {
        Self { page, data_offset }
    }

    /// Number of slots (from page entry_count).
    pub fn slot_count(&self) -> usize {
        self.page.entry_count() as usize
    }

    /// Bytes available between the end of the slot array and the start of cell data.
    pub fn free_space(&self) -> usize {
        let slot_end = self.data_offset + self.slot_count() * SLOT_SIZE;
        let cell_start = self.page.free_space_offset() as usize;
        cell_start.saturating_sub(slot_end)
    }

    /// Read the slot entry at `slot_index`. Returns `(offset, length)`.
    fn read_slot(&self, slot_index: usize) -> (u16, u16) {
        let base = self.data_offset + slot_index * SLOT_SIZE;
        let buf = self.page.data();
        let offset = u16::from_le_bytes([buf[base], buf[base + 1]]);
        let length = u16::from_le_bytes([buf[base + 2], buf[base + 3]]);
        (offset, length)
    }

    /// Return the cell data for `slot_index`, or `None` if the slot is dead
    /// (offset == 0).
    pub fn cell(&self, slot_index: usize) -> Option<&[u8]> {
        if slot_index >= self.slot_count() {
            return None;
        }
        let (offset, length) = self.read_slot(slot_index);
        if offset == 0 {
            return None; // dead slot
        }
        let start = offset as usize;
        let end = start + length as usize;
        Some(&self.page.data()[start..end])
    }

    /// Check whether a slot is dead (offset == 0).
    pub fn is_dead(&self, slot_index: usize) -> bool {
        if slot_index >= self.slot_count() {
            return false;
        }
        let (offset, _) = self.read_slot(slot_index);
        offset == 0
    }
}

/// Mutable slotted page operations.
pub struct SlottedPage<'a> {
    page: &'a mut Page,
    data_offset: usize,
}

impl<'a> SlottedPage<'a> {
    /// Create a mutable slotted page view.
    pub fn new(page: &'a mut Page, data_offset: usize) -> Self {
        Self { page, data_offset }
    }

    /// Number of slots (from page entry_count).
    pub fn slot_count(&self) -> usize {
        self.page.entry_count() as usize
    }

    /// Bytes available between the end of the slot array and the start of cell data.
    pub fn free_space(&self) -> usize {
        let slot_end = self.data_offset + self.slot_count() * SLOT_SIZE;
        let cell_start = self.page.free_space_offset() as usize;
        cell_start.saturating_sub(slot_end)
    }

    /// Read the slot entry at `slot_index`. Returns `(offset, length)`.
    fn read_slot(&self, slot_index: usize) -> (u16, u16) {
        let base = self.data_offset + slot_index * SLOT_SIZE;
        let buf = self.page.data();
        let offset = u16::from_le_bytes([buf[base], buf[base + 1]]);
        let length = u16::from_le_bytes([buf[base + 2], buf[base + 3]]);
        (offset, length)
    }

    /// Write a slot entry at `slot_index`.
    fn write_slot(&mut self, slot_index: usize, offset: u16, length: u16) {
        let base = self.data_offset + slot_index * SLOT_SIZE;
        let buf = self.page.data_mut();
        buf[base..base + 2].copy_from_slice(&offset.to_le_bytes());
        buf[base + 2..base + 4].copy_from_slice(&length.to_le_bytes());
    }

    /// Return the cell data for `slot_index`, or `None` if dead.
    pub fn cell(&self, slot_index: usize) -> Option<&[u8]> {
        if slot_index >= self.slot_count() {
            return None;
        }
        let (offset, length) = self.read_slot(slot_index);
        if offset == 0 {
            return None;
        }
        let start = offset as usize;
        let end = start + length as usize;
        Some(&self.page.data()[start..end])
    }

    /// Check whether a slot is dead.
    pub fn is_dead(&self, slot_index: usize) -> bool {
        if slot_index >= self.slot_count() {
            return false;
        }
        let (offset, _) = self.read_slot(slot_index);
        offset == 0
    }

    /// Insert a new cell at the end of the slot array. Returns the slot index.
    pub fn insert(&mut self, data: &[u8]) -> Result<usize, StorageError> {
        let needed = SLOT_SIZE + data.len();
        if self.free_space() < needed {
            return Err(StorageError::CorruptedPage(
                "not enough free space in slotted page".to_string(),
            ));
        }

        let slot_index = self.slot_count();
        let cell_offset = self.page.free_space_offset() as usize - data.len();

        // Write cell data
        self.page.data_mut()[cell_offset..cell_offset + data.len()].copy_from_slice(data);

        // Write slot
        self.write_slot(slot_index, cell_offset as u16, data.len() as u16);

        // Update header
        self.page.set_entry_count(slot_index as u32 + 1);
        self.page.set_free_space_offset(cell_offset as u32);
        self.page.mark_dirty();

        Ok(slot_index)
    }

    /// Insert at a specific slot position, shifting subsequent slots forward.
    pub fn insert_at(&mut self, slot_index: usize, data: &[u8]) -> Result<(), StorageError> {
        let count = self.slot_count();
        if slot_index > count {
            return Err(StorageError::CorruptedPage(format!(
                "insert_at index {slot_index} out of bounds (count={count})"
            )));
        }

        let needed = SLOT_SIZE + data.len();
        if self.free_space() < needed {
            return Err(StorageError::CorruptedPage(
                "not enough free space in slotted page".to_string(),
            ));
        }

        let cell_offset = self.page.free_space_offset() as usize - data.len();

        // Write cell data
        self.page.data_mut()[cell_offset..cell_offset + data.len()].copy_from_slice(data);

        // Shift slots from slot_index..count to slot_index+1..count+1
        // Must go backwards to avoid overwriting
        for i in (slot_index..count).rev() {
            let (off, len) = self.read_slot(i);
            self.write_slot(i + 1, off, len);
        }

        // Write the new slot
        self.write_slot(slot_index, cell_offset as u16, data.len() as u16);

        // Update header
        self.page.set_entry_count(count as u32 + 1);
        self.page.set_free_space_offset(cell_offset as u32);
        self.page.mark_dirty();

        Ok(())
    }

    /// Mark a slot as dead by setting its offset to 0.
    pub fn mark_dead(&mut self, slot_index: usize) {
        if slot_index < self.slot_count() {
            let (_, length) = self.read_slot(slot_index);
            self.write_slot(slot_index, 0, length);
            self.page.mark_dirty();
        }
    }

    /// Remove dead slots and compact cell data so all live cells are contiguous
    /// at the end of the page.
    pub fn compact(&mut self) {
        let count = self.slot_count();

        // Collect live cells in order
        let mut live: Vec<(usize, Vec<u8>)> = Vec::new();
        for i in 0..count {
            let (offset, length) = self.read_slot(i);
            if offset != 0 {
                let start = offset as usize;
                let end = start + length as usize;
                live.push((i, self.page.data()[start..end].to_vec()));
            }
        }

        // Reset free_space_offset to end of page
        let mut write_offset = PAGE_SIZE;

        // Rewrite cells from the end and build new slot array
        let new_count = live.len();
        for (new_idx, (_old_idx, cell_data)) in live.iter().enumerate() {
            write_offset -= cell_data.len();
            self.page.data_mut()[write_offset..write_offset + cell_data.len()]
                .copy_from_slice(cell_data);
            self.write_slot(new_idx, write_offset as u16, cell_data.len() as u16);
        }

        // Zero out any leftover slot entries
        for i in new_count..count {
            self.write_slot(i, 0, 0);
        }

        self.page.set_entry_count(new_count as u32);
        self.page.set_free_space_offset(write_offset as u32);
        self.page.mark_dirty();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::PageType;
    use crate::types::PAGE_HEADER_SIZE;

    const TEST_DATA_OFFSET: usize = PAGE_HEADER_SIZE; // 32

    fn make_page() -> Page {
        Page::new(0, PageType::BTreeLeaf)
    }

    #[test]
    fn test_insert_and_read() {
        let mut page = make_page();
        let mut sp = SlottedPage::new(&mut page, TEST_DATA_OFFSET);
        let idx = sp.insert(b"hello").unwrap();
        assert_eq!(idx, 0);
        assert_eq!(sp.cell(0).unwrap(), b"hello");
    }

    #[test]
    fn test_insert_multiple() {
        let mut page = make_page();
        let mut sp = SlottedPage::new(&mut page, TEST_DATA_OFFSET);
        sp.insert(b"aaa").unwrap();
        sp.insert(b"bbb").unwrap();
        sp.insert(b"ccc").unwrap();

        assert_eq!(sp.slot_count(), 3);
        assert_eq!(sp.cell(0).unwrap(), b"aaa");
        assert_eq!(sp.cell(1).unwrap(), b"bbb");
        assert_eq!(sp.cell(2).unwrap(), b"ccc");
    }

    #[test]
    fn test_insert_at_position() {
        let mut page = make_page();
        let mut sp = SlottedPage::new(&mut page, TEST_DATA_OFFSET);
        sp.insert(b"first").unwrap();
        sp.insert(b"third").unwrap();
        sp.insert_at(1, b"second").unwrap();

        assert_eq!(sp.slot_count(), 3);
        assert_eq!(sp.cell(0).unwrap(), b"first");
        assert_eq!(sp.cell(1).unwrap(), b"second");
        assert_eq!(sp.cell(2).unwrap(), b"third");
    }

    #[test]
    fn test_mark_dead() {
        let mut page = make_page();
        let mut sp = SlottedPage::new(&mut page, TEST_DATA_OFFSET);
        sp.insert(b"live").unwrap();
        sp.insert(b"dead").unwrap();
        sp.mark_dead(1);

        assert!(!sp.is_dead(0));
        assert!(sp.is_dead(1));
        assert!(sp.cell(1).is_none());
        assert_eq!(sp.cell(0).unwrap(), b"live");
    }

    #[test]
    fn test_compact() {
        let mut page = make_page();
        let mut sp = SlottedPage::new(&mut page, TEST_DATA_OFFSET);
        sp.insert(b"aaa").unwrap();
        sp.insert(b"bbb").unwrap();
        sp.insert(b"ccc").unwrap();
        sp.insert(b"ddd").unwrap();

        sp.mark_dead(1); // kill "bbb"
        sp.mark_dead(3); // kill "ddd"
        sp.compact();

        assert_eq!(sp.slot_count(), 2);
        assert_eq!(sp.cell(0).unwrap(), b"aaa");
        assert_eq!(sp.cell(1).unwrap(), b"ccc");
    }

    #[test]
    fn test_insert_full() {
        let mut page = make_page();
        let mut sp = SlottedPage::new(&mut page, TEST_DATA_OFFSET);
        // Fill the page with large cells until we run out of space
        let big = vec![0xABu8; 500];
        let mut inserted = 0;
        loop {
            match sp.insert(&big) {
                Ok(_) => inserted += 1,
                Err(_) => break,
            }
        }
        // We should have inserted at least 1 but not unlimited
        assert!(inserted > 0);
        assert!(inserted < 100); // sanity

        // Verify the error is returned
        assert!(sp.insert(&big).is_err());
    }

    #[test]
    fn test_free_space_tracking() {
        let mut page = make_page();
        let initial_free = {
            let sp = SlottedPage::new(&mut page, TEST_DATA_OFFSET);
            sp.free_space()
        };

        let cell_data = b"test_data";
        {
            let mut sp = SlottedPage::new(&mut page, TEST_DATA_OFFSET);
            sp.insert(cell_data).unwrap();
        }

        let after_free = {
            let sp = SlottedPage::new(&mut page, TEST_DATA_OFFSET);
            sp.free_space()
        };

        // Free space should decrease by cell_data.len() + SLOT_SIZE
        assert_eq!(initial_free - after_free, cell_data.len() + SLOT_SIZE);
    }

    #[test]
    fn test_slotted_page_ref() {
        let mut page = make_page();
        {
            let mut sp = SlottedPage::new(&mut page, TEST_DATA_OFFSET);
            sp.insert(b"hello").unwrap();
            sp.insert(b"world").unwrap();
        }

        // Now use the read-only view
        let sp_ref = SlottedPageRef::new(&page, TEST_DATA_OFFSET);
        assert_eq!(sp_ref.slot_count(), 2);
        assert_eq!(sp_ref.cell(0).unwrap(), b"hello");
        assert_eq!(sp_ref.cell(1).unwrap(), b"world");
        assert!(!sp_ref.is_dead(0));
        assert!(sp_ref.free_space() > 0);
    }
}

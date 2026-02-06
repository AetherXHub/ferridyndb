use crate::error::StorageError;
use crate::types::{PAGE_SIZE, PageId};
use xxhash_rust::xxh64::Xxh64;

/// Byte range for the page header checksum computation: bytes 20..4096.
const CHECKSUM_START: usize = 20;

/// Discriminant values for page types stored on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum PageType {
    Header = 0,
    BTreeInternal = 1,
    BTreeLeaf = 2,
    Overflow = 3,
    FreeList = 4,
    PendingFree = 5,
    Tombstone = 6,
}

impl PageType {
    /// Convert a u32 discriminant to a `PageType`.
    pub fn from_u32(value: u32) -> Result<Self, StorageError> {
        match value {
            0 => Ok(PageType::Header),
            1 => Ok(PageType::BTreeInternal),
            2 => Ok(PageType::BTreeLeaf),
            3 => Ok(PageType::Overflow),
            4 => Ok(PageType::FreeList),
            5 => Ok(PageType::PendingFree),
            6 => Ok(PageType::Tombstone),
            other => Err(StorageError::CorruptedPage(format!(
                "invalid page type discriminant: {other}"
            ))),
        }
    }

    /// Return the u32 discriminant for this page type.
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

/// A fixed-size page backed by a `[u8; PAGE_SIZE]` buffer.
///
/// Common page header layout (first 32 bytes):
/// ```text
/// [0..4]   page_type: u32 (little-endian)
/// [4..12]  page_id: u64 (little-endian)
/// [12..20] xxhash64 checksum (of bytes 0..12 + 20..4096)
/// [20..24] entry_count: u32 (little-endian)
/// [24..28] free_space_offset: u32 (little-endian)
/// [28..32] reserved: u32
/// ```
pub struct Page {
    buf: [u8; PAGE_SIZE],
    page_id: PageId,
    dirty: bool,
}

impl Page {
    /// Create a fresh page with the header initialised.
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let mut buf = [0u8; PAGE_SIZE];
        // page_type
        buf[0..4].copy_from_slice(&page_type.as_u32().to_le_bytes());
        // page_id
        buf[4..12].copy_from_slice(&page_id.to_le_bytes());
        // entry_count = 0 (already zeroed)
        // free_space_offset = PAGE_SIZE (no cells allocated yet)
        buf[24..28].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());

        Self {
            buf,
            page_id,
            dirty: true,
        }
    }

    /// Wrap an existing raw page buffer.
    pub fn from_bytes(data: [u8; PAGE_SIZE], page_id: PageId) -> Self {
        Self {
            buf: data,
            page_id,
            dirty: false,
        }
    }

    /// Read and validate the page type from the header.
    pub fn page_type(&self) -> Result<PageType, StorageError> {
        let raw = u32::from_le_bytes(self.buf[0..4].try_into().unwrap());
        PageType::from_u32(raw)
    }

    /// Return the page id.
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Read the entry count from the header.
    pub fn entry_count(&self) -> u32 {
        u32::from_le_bytes(self.buf[20..24].try_into().unwrap())
    }

    /// Set the entry count in the header.
    pub fn set_entry_count(&mut self, count: u32) {
        self.buf[20..24].copy_from_slice(&count.to_le_bytes());
    }

    /// Read the free space offset from the header.
    pub fn free_space_offset(&self) -> u32 {
        u32::from_le_bytes(self.buf[24..28].try_into().unwrap())
    }

    /// Set the free space offset in the header.
    pub fn set_free_space_offset(&mut self, offset: u32) {
        self.buf[24..28].copy_from_slice(&offset.to_le_bytes());
    }

    /// Raw buffer access (read-only).
    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.buf
    }

    /// Raw buffer access (mutable).
    pub fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.buf
    }

    /// Compute the xxhash64 checksum of bytes `[0..12] + [20..PAGE_SIZE]`.
    ///
    /// This covers page_type, page_id, and all data after the checksum field,
    /// skipping only the checksum field itself at `[12..20]`.
    pub fn compute_checksum(&self) -> u64 {
        let mut hasher = Xxh64::new(0);
        hasher.update(&self.buf[0..12]); // page_type + page_id
        hasher.update(&self.buf[CHECKSUM_START..PAGE_SIZE]); // data after checksum field
        hasher.digest()
    }

    /// Compute the checksum and write it into the header at `[12..20]`.
    pub fn write_checksum(&mut self) {
        let cs = self.compute_checksum();
        self.buf[12..20].copy_from_slice(&cs.to_le_bytes());
    }

    /// Verify that the stored checksum matches the computed value.
    pub fn verify_checksum(&self) -> Result<(), StorageError> {
        let stored = u64::from_le_bytes(self.buf[12..20].try_into().unwrap());
        let computed = self.compute_checksum();
        if stored != computed {
            return Err(StorageError::CorruptedPage(format!(
                "checksum mismatch on page {}: stored={stored:#018x}, computed={computed:#018x}",
                self.page_id
            )));
        }
        Ok(())
    }

    /// Mark this page as dirty (modified in memory).
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Check whether this page has been modified.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

/// Compute checksum for a raw page buffer, covering page_type, page_id, and data.
///
/// Hashes bytes `[0..12] + [20..PAGE_SIZE]`, skipping the checksum field at `[12..20]`.
pub fn compute_checksum_buf(buf: &[u8; PAGE_SIZE]) -> u64 {
    let mut hasher = Xxh64::new(0);
    hasher.update(&buf[0..12]);
    hasher.update(&buf[CHECKSUM_START..PAGE_SIZE]);
    hasher.digest()
}

/// Compute and write the checksum into a raw page buffer at `[12..20]`.
pub fn write_checksum_buf(buf: &mut [u8; PAGE_SIZE]) {
    let cs = compute_checksum_buf(buf);
    buf[12..20].copy_from_slice(&cs.to_le_bytes());
}

/// Verify a raw page buffer's checksum and page_id consistency.
///
/// Checks that the stored page_id matches the expected offset and that
/// the stored checksum matches the computed value. Returns `Ok(())` if
/// valid, or a descriptive `StorageError::CorruptedPage` error.
pub fn verify_page_integrity(
    buf: &[u8; PAGE_SIZE],
    expected_page_id: PageId,
) -> Result<(), StorageError> {
    // Check page_id matches expected offset.
    let stored_page_id = u64::from_le_bytes(buf[4..12].try_into().unwrap());
    if stored_page_id != expected_page_id {
        return Err(StorageError::CorruptedPage(format!(
            "page_id mismatch: expected {expected_page_id}, found {stored_page_id}"
        )));
    }
    // Verify checksum.
    let stored = u64::from_le_bytes(buf[12..20].try_into().unwrap());
    let computed = compute_checksum_buf(buf);
    if stored != computed {
        return Err(StorageError::CorruptedPage(format!(
            "checksum mismatch on page {expected_page_id}: stored={stored:#018x}, computed={computed:#018x}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_page_header() {
        let page = Page::new(42, PageType::BTreeLeaf);
        assert_eq!(page.page_type().unwrap(), PageType::BTreeLeaf);
        assert_eq!(page.page_id(), 42);
        assert_eq!(page.entry_count(), 0);
        assert_eq!(page.free_space_offset(), PAGE_SIZE as u32);
    }

    #[test]
    fn test_checksum_roundtrip() {
        let mut page = Page::new(1, PageType::BTreeInternal);
        page.set_entry_count(5);
        page.write_checksum();
        assert!(page.verify_checksum().is_ok());
    }

    #[test]
    fn test_checksum_detects_corruption() {
        let mut page = Page::new(1, PageType::BTreeLeaf);
        page.write_checksum();
        // Flip a byte in the checksummed region
        page.data_mut()[100] ^= 0xFF;
        assert!(page.verify_checksum().is_err());
    }

    #[test]
    fn test_page_type_validation() {
        let types = [
            PageType::Header,
            PageType::BTreeInternal,
            PageType::BTreeLeaf,
            PageType::Overflow,
            PageType::FreeList,
        ];
        for pt in types {
            let page = Page::new(0, pt);
            assert_eq!(page.page_type().unwrap(), pt);
        }
    }

    #[test]
    fn test_page_type_invalid() {
        let page = {
            let mut p = Page::new(0, PageType::Header);
            p.data_mut()[0..4].copy_from_slice(&99u32.to_le_bytes());
            p
        };
        assert!(page.page_type().is_err());
    }

    #[test]
    fn test_from_bytes() {
        let mut original = Page::new(7, PageType::Overflow);
        original.set_entry_count(3);
        original.write_checksum();

        let raw = *original.data();
        let restored = Page::from_bytes(raw, 7);
        assert_eq!(restored.page_type().unwrap(), PageType::Overflow);
        assert_eq!(restored.entry_count(), 3);
        assert!(restored.verify_checksum().is_ok());
        assert!(!restored.is_dirty());
    }

    #[test]
    fn test_dirty_flag() {
        let page = Page::new(0, PageType::Header);
        assert!(page.is_dirty()); // new pages are dirty
        let raw = *page.data();
        let mut page2 = Page::from_bytes(raw, 0);
        assert!(!page2.is_dirty());
        page2.mark_dirty();
        assert!(page2.is_dirty());
    }

    #[test]
    fn test_checksum_covers_page_type() {
        let mut page = Page::new(1, PageType::BTreeLeaf);
        page.write_checksum();
        // Corrupt the page_type field
        page.data_mut()[0..4].copy_from_slice(&99u32.to_le_bytes());
        assert!(page.verify_checksum().is_err());
    }

    #[test]
    fn test_checksum_covers_page_id() {
        let mut page = Page::new(1, PageType::BTreeLeaf);
        page.write_checksum();
        // Corrupt the page_id field
        page.data_mut()[4..12].copy_from_slice(&999u64.to_le_bytes());
        assert!(page.verify_checksum().is_err());
    }

    #[test]
    fn test_verify_page_integrity_valid() {
        let mut page = Page::new(42, PageType::BTreeLeaf);
        page.set_entry_count(5);
        page.write_checksum();
        assert!(verify_page_integrity(page.data(), 42).is_ok());
    }

    #[test]
    fn test_verify_page_integrity_wrong_page_id() {
        let mut page = Page::new(42, PageType::BTreeLeaf);
        page.write_checksum();
        // Verify with wrong expected page_id
        assert!(verify_page_integrity(page.data(), 99).is_err());
    }

    #[test]
    fn test_buf_checksum_helpers() {
        let mut buf = [0u8; PAGE_SIZE];
        // Set page_type
        buf[0..4].copy_from_slice(&(PageType::BTreeLeaf.as_u32()).to_le_bytes());
        // Set page_id
        buf[4..12].copy_from_slice(&7u64.to_le_bytes());
        // Write checksum
        write_checksum_buf(&mut buf);
        // Verify
        assert!(verify_page_integrity(&buf, 7).is_ok());
    }
}

use crate::error::StorageError;
use crate::types::{PAGE_SIZE, PageId};
use xxhash_rust::xxh64::xxh64;

/// Magic bytes identifying a FerridynDB database file.
pub const MAGIC: &[u8; 4] = b"DYNA";

/// Current file format version.
pub const VERSION: u32 = 1;

/// Header checksum covers bytes `[0..44]`.
const CHECKSUM_RANGE_END: usize = 44;

/// The checksum is stored at bytes `[44..52]`.
const CHECKSUM_OFFSET: usize = 44;

/// Double-buffered file header for crash-safe metadata updates.
///
/// Header layout (within a 4096-byte page):
/// ```text
/// [0..4]   magic: "DYNA" (4 bytes)
/// [4..8]   version: u32 (1) little-endian
/// [8..12]  page_size: u32 (4096) little-endian
/// [12..20] txn_counter: u64 little-endian
/// [20..28] catalog_root_page: u64 little-endian
/// [28..36] free_list_head_page: u64 little-endian
/// [36..44] total_page_count: u64 little-endian
/// [44..52] xxhash64 checksum (of bytes 0..44) little-endian
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub txn_counter: u64,
    pub catalog_root_page: PageId,
    pub free_list_head_page: PageId,
    pub total_page_count: u64,
}

impl FileHeader {
    /// Create a fresh header for a new database file.
    /// `total_page_count` starts at 2 (the two header pages themselves).
    pub fn new() -> Self {
        Self {
            txn_counter: 0,
            catalog_root_page: 0,
            free_list_head_page: 0,
            total_page_count: 2,
        }
    }

    /// Parse a header from a raw page buffer, validating magic, version, and checksum.
    pub fn from_page(page_data: &[u8; PAGE_SIZE]) -> Result<Self, StorageError> {
        // Validate magic
        if &page_data[0..4] != MAGIC {
            return Err(StorageError::InvalidMagic);
        }

        // Validate version
        let version = u32::from_le_bytes(page_data[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(StorageError::UnsupportedVersion(version));
        }

        // Validate checksum
        let stored_checksum = u64::from_le_bytes(
            page_data[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 8]
                .try_into()
                .unwrap(),
        );
        let computed_checksum = xxh64(&page_data[..CHECKSUM_RANGE_END], 0);
        if stored_checksum != computed_checksum {
            return Err(StorageError::CorruptedPage(format!(
                "header checksum mismatch: stored={stored_checksum:#018x}, computed={computed_checksum:#018x}"
            )));
        }

        let txn_counter = u64::from_le_bytes(page_data[12..20].try_into().unwrap());
        let catalog_root_page = u64::from_le_bytes(page_data[20..28].try_into().unwrap());
        let free_list_head_page = u64::from_le_bytes(page_data[28..36].try_into().unwrap());
        let total_page_count = u64::from_le_bytes(page_data[36..44].try_into().unwrap());

        Ok(Self {
            txn_counter,
            catalog_root_page,
            free_list_head_page,
            total_page_count,
        })
    }

    /// Serialize this header into a page buffer, including magic, version, page_size,
    /// and the trailing checksum.
    pub fn write_to_page(&self, buf: &mut [u8; PAGE_SIZE]) {
        buf.fill(0);

        // magic
        buf[0..4].copy_from_slice(MAGIC);
        // version
        buf[4..8].copy_from_slice(&VERSION.to_le_bytes());
        // page_size
        buf[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        // txn_counter
        buf[12..20].copy_from_slice(&self.txn_counter.to_le_bytes());
        // catalog_root_page
        buf[20..28].copy_from_slice(&self.catalog_root_page.to_le_bytes());
        // free_list_head_page
        buf[28..36].copy_from_slice(&self.free_list_head_page.to_le_bytes());
        // total_page_count
        buf[36..44].copy_from_slice(&self.total_page_count.to_le_bytes());
        // checksum of [0..44]
        let checksum = xxh64(&buf[..CHECKSUM_RANGE_END], 0);
        buf[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 8].copy_from_slice(&checksum.to_le_bytes());
    }

    /// Given two header page buffers, return the valid one with the higher
    /// `txn_counter` and which slot (0 or 1) it came from.
    ///
    /// If both are valid, the one with the higher txn_counter wins.
    /// If only one is valid, that one wins.
    /// If neither is valid, returns an error.
    pub fn select_current(
        header_a: &[u8; PAGE_SIZE],
        header_b: &[u8; PAGE_SIZE],
    ) -> Result<(FileHeader, u8), StorageError> {
        let a = Self::from_page(header_a);
        let b = Self::from_page(header_b);

        match (a, b) {
            (Ok(ha), Ok(hb)) => {
                if ha.txn_counter >= hb.txn_counter {
                    Ok((ha, 0))
                } else {
                    Ok((hb, 1))
                }
            }
            (Ok(ha), Err(_)) => Ok((ha, 0)),
            (Err(_), Ok(hb)) => Ok((hb, 1)),
            (Err(_), Err(_)) => Err(StorageError::CorruptedPage(
                "both header pages are invalid".to_string(),
            )),
        }
    }

    /// Return the other header slot: 0 becomes 1, 1 becomes 0.
    pub fn alternate_slot(current_slot: u8) -> u8 {
        if current_slot == 0 { 1 } else { 0 }
    }
}

impl Default for FileHeader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let header = FileHeader {
            txn_counter: 42,
            catalog_root_page: 5,
            free_list_head_page: 10,
            total_page_count: 100,
        };

        let mut buf = [0u8; PAGE_SIZE];
        header.write_to_page(&mut buf);

        let parsed = FileHeader::from_page(&buf).unwrap();
        assert_eq!(header, parsed);
    }

    #[test]
    fn test_header_checksum_validation() {
        let header = FileHeader::new();
        let mut buf = [0u8; PAGE_SIZE];
        header.write_to_page(&mut buf);

        // Corrupt a byte in the checksummed region
        buf[20] ^= 0xFF;

        assert!(FileHeader::from_page(&buf).is_err());
    }

    #[test]
    fn test_select_current_picks_higher_txn() {
        let mut h1 = FileHeader::new();
        h1.txn_counter = 5;
        let mut buf_a = [0u8; PAGE_SIZE];
        h1.write_to_page(&mut buf_a);

        let mut h2 = FileHeader::new();
        h2.txn_counter = 10;
        let mut buf_b = [0u8; PAGE_SIZE];
        h2.write_to_page(&mut buf_b);

        let (selected, slot) = FileHeader::select_current(&buf_a, &buf_b).unwrap();
        assert_eq!(selected.txn_counter, 10);
        assert_eq!(slot, 1);
    }

    #[test]
    fn test_select_current_handles_one_invalid() {
        let header = FileHeader::new();
        let mut buf_a = [0u8; PAGE_SIZE];
        header.write_to_page(&mut buf_a);

        // buf_b is all zeros (invalid)
        let buf_b = [0u8; PAGE_SIZE];

        let (selected, slot) = FileHeader::select_current(&buf_a, &buf_b).unwrap();
        assert_eq!(selected, header);
        assert_eq!(slot, 0);
    }

    #[test]
    fn test_select_current_both_invalid() {
        let buf_a = [0u8; PAGE_SIZE];
        let buf_b = [0u8; PAGE_SIZE];
        assert!(FileHeader::select_current(&buf_a, &buf_b).is_err());
    }

    #[test]
    fn test_alternate_slot() {
        assert_eq!(FileHeader::alternate_slot(0), 1);
        assert_eq!(FileHeader::alternate_slot(1), 0);
    }

    #[test]
    fn test_header_magic_validation() {
        let header = FileHeader::new();
        let mut buf = [0u8; PAGE_SIZE];
        header.write_to_page(&mut buf);

        // Corrupt magic
        buf[0] = b'X';
        match FileHeader::from_page(&buf) {
            Err(StorageError::InvalidMagic) => {}
            other => panic!("expected InvalidMagic, got {other:?}"),
        }
    }

    #[test]
    fn test_header_version_validation() {
        let header = FileHeader::new();
        let mut buf = [0u8; PAGE_SIZE];
        header.write_to_page(&mut buf);

        // Set version to 99
        buf[4..8].copy_from_slice(&99u32.to_le_bytes());
        // Re-compute checksum so we actually hit the version check, not the checksum check
        let checksum = xxh64(&buf[..CHECKSUM_RANGE_END], 0);
        buf[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 8].copy_from_slice(&checksum.to_le_bytes());

        match FileHeader::from_page(&buf) {
            Err(StorageError::UnsupportedVersion(99)) => {}
            other => panic!("expected UnsupportedVersion(99), got {other:?}"),
        }
    }

    #[test]
    fn test_default_header() {
        let header = FileHeader::default();
        assert_eq!(header.txn_counter, 0);
        assert_eq!(header.catalog_root_page, 0);
        assert_eq!(header.free_list_head_page, 0);
        assert_eq!(header.total_page_count, 2);
    }
}

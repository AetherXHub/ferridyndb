use crate::error::StorageError;
use crate::types::{PAGE_SIZE, PageId};
use xxhash_rust::xxh3::xxh3_128;
use xxhash_rust::xxh64::xxh64;

/// Magic bytes identifying a FerridynDB database file.
pub const MAGIC: &[u8; 4] = b"DYNA";

/// Current file format version.
pub const VERSION: u32 = 5;

/// Previous file format versions.
const VERSION_1: u32 = 1;
const VERSION_2: u32 = 2;
const VERSION_3: u32 = 3;
const VERSION_4: u32 = 4;

/// V5 header checksum (XXH3_128) covers bytes `[0..76]`.
const CHECKSUM_RANGE_END: usize = 76;

/// V5 checksum (16 bytes) is stored at bytes `[76..92]`.
const CHECKSUM_OFFSET: usize = 76;

/// V4 header checksum (XXH3_128) covers bytes `[0..60]`.
const V4_CHECKSUM_RANGE_END: usize = 60;

/// V4 checksum (16 bytes) is stored at bytes `[60..76]`.
const V4_CHECKSUM_OFFSET: usize = 60;

/// V3 header checksum (xxHash64) covers bytes `[0..60]`.
const V3_CHECKSUM_RANGE_END: usize = 60;

/// V3 checksum (8 bytes) is stored at bytes `[60..68]`.
const V3_CHECKSUM_OFFSET: usize = 60;

/// V2 header checksum covers bytes `[0..52]`.
const V2_CHECKSUM_RANGE_END: usize = 52;

/// V2 checksum is stored at bytes `[52..60]`.
const V2_CHECKSUM_OFFSET: usize = 52;

/// V1 header checksum covers bytes `[0..44]`.
const V1_CHECKSUM_RANGE_END: usize = 44;

/// V1 checksum is stored at bytes `[44..52]`.
const V1_CHECKSUM_OFFSET: usize = 44;

/// Double-buffered file header for crash-safe metadata updates.
///
/// Header layout v5 (within a 4096-byte page):
/// ```text
/// [0..4]   magic: "DYNA" (4 bytes)
/// [4..8]   version: u32 (5) little-endian
/// [8..12]  page_size: u32 (4096) little-endian
/// [12..20] txn_counter: u64 little-endian
/// [20..28] catalog_root_page: u64 little-endian
/// [28..36] free_list_head_page: u64 little-endian
/// [36..44] total_page_count: u64 little-endian
/// [44..52] pending_free_root_page: u64 little-endian
/// [52..60] tombstone_root_page: u64 little-endian
/// [60..76] catalog_root_checksum: u128 little-endian (Merkle root)
/// [76..92] xxh3_128 checksum (of bytes 0..76) little-endian
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub txn_counter: u64,
    pub catalog_root_page: PageId,
    pub free_list_head_page: PageId,
    pub total_page_count: u64,
    pub pending_free_root_page: PageId,
    pub tombstone_root_page: PageId,
    /// Merkle checksum of the catalog B-tree root page.
    /// Enables top-down integrity verification from the header.
    pub catalog_root_checksum: u128,
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
            pending_free_root_page: 0,
            tombstone_root_page: 0,
            catalog_root_checksum: 0,
        }
    }

    /// Parse a header from a raw page buffer, validating magic, version, and checksum.
    ///
    /// Supports v1, v2, v3, v4, and v5 headers with backwards compatibility.
    pub fn from_page(page_data: &[u8; PAGE_SIZE]) -> Result<Self, StorageError> {
        // Validate magic
        if &page_data[0..4] != MAGIC {
            return Err(StorageError::InvalidMagic);
        }

        // Validate version
        let version = u32::from_le_bytes(page_data[4..8].try_into().unwrap());

        let (pending_free_root_page, tombstone_root_page, catalog_root_checksum) = match version {
            VERSION_1 => {
                // V1: xxHash64 checksum at [44..52] covers [0..44].
                let stored = u64::from_le_bytes(
                    page_data[V1_CHECKSUM_OFFSET..V1_CHECKSUM_OFFSET + 8]
                        .try_into()
                        .unwrap(),
                );
                let computed = xxh64(&page_data[..V1_CHECKSUM_RANGE_END], 0);
                if stored != computed {
                    return Err(StorageError::CorruptedPage(format!(
                        "header checksum mismatch: stored={stored:#018x}, computed={computed:#018x}"
                    )));
                }
                (0u64, 0u64, 0u128)
            }
            VERSION_2 => {
                // V2: xxHash64 checksum at [52..60] covers [0..52].
                let stored = u64::from_le_bytes(
                    page_data[V2_CHECKSUM_OFFSET..V2_CHECKSUM_OFFSET + 8]
                        .try_into()
                        .unwrap(),
                );
                let computed = xxh64(&page_data[..V2_CHECKSUM_RANGE_END], 0);
                if stored != computed {
                    return Err(StorageError::CorruptedPage(format!(
                        "header checksum mismatch: stored={stored:#018x}, computed={computed:#018x}"
                    )));
                }
                let pfr = u64::from_le_bytes(page_data[44..52].try_into().unwrap());
                (pfr, 0u64, 0u128)
            }
            VERSION_3 => {
                // V3: xxHash64 checksum at [60..68] covers [0..60].
                let stored = u64::from_le_bytes(
                    page_data[V3_CHECKSUM_OFFSET..V3_CHECKSUM_OFFSET + 8]
                        .try_into()
                        .unwrap(),
                );
                let computed = xxh64(&page_data[..V3_CHECKSUM_RANGE_END], 0);
                if stored != computed {
                    return Err(StorageError::CorruptedPage(format!(
                        "header checksum mismatch: stored={stored:#018x}, computed={computed:#018x}"
                    )));
                }
                let pfr = u64::from_le_bytes(page_data[44..52].try_into().unwrap());
                let trp = u64::from_le_bytes(page_data[52..60].try_into().unwrap());
                (pfr, trp, 0u128)
            }
            VERSION_4 => {
                // V4: XXH3_128 checksum at [60..76] covers [0..60].
                let stored = u128::from_le_bytes(
                    page_data[V4_CHECKSUM_OFFSET..V4_CHECKSUM_OFFSET + 16]
                        .try_into()
                        .unwrap(),
                );
                let computed = xxh3_128(&page_data[..V4_CHECKSUM_RANGE_END]);
                if stored != computed {
                    return Err(StorageError::CorruptedPage(format!(
                        "header checksum mismatch: stored={stored:#034x}, computed={computed:#034x}"
                    )));
                }
                let pfr = u64::from_le_bytes(page_data[44..52].try_into().unwrap());
                let trp = u64::from_le_bytes(page_data[52..60].try_into().unwrap());
                (pfr, trp, 0u128)
            }
            VERSION => {
                // V5: XXH3_128 checksum at [76..92] covers [0..76].
                let stored = u128::from_le_bytes(
                    page_data[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 16]
                        .try_into()
                        .unwrap(),
                );
                let computed = xxh3_128(&page_data[..CHECKSUM_RANGE_END]);
                if stored != computed {
                    return Err(StorageError::CorruptedPage(format!(
                        "header checksum mismatch: stored={stored:#034x}, computed={computed:#034x}"
                    )));
                }
                let pfr = u64::from_le_bytes(page_data[44..52].try_into().unwrap());
                let trp = u64::from_le_bytes(page_data[52..60].try_into().unwrap());
                let crc = u128::from_le_bytes(page_data[60..76].try_into().unwrap());
                (pfr, trp, crc)
            }
            other => return Err(StorageError::UnsupportedVersion(other)),
        };

        let txn_counter = u64::from_le_bytes(page_data[12..20].try_into().unwrap());
        let catalog_root_page = u64::from_le_bytes(page_data[20..28].try_into().unwrap());
        let free_list_head_page = u64::from_le_bytes(page_data[28..36].try_into().unwrap());
        let total_page_count = u64::from_le_bytes(page_data[36..44].try_into().unwrap());

        Ok(Self {
            txn_counter,
            catalog_root_page,
            free_list_head_page,
            total_page_count,
            pending_free_root_page,
            tombstone_root_page,
            catalog_root_checksum,
        })
    }

    /// Serialize this header into a page buffer, including magic, version, page_size,
    /// and the trailing XXH3_128 checksum.
    pub fn write_to_page(&self, buf: &mut [u8; PAGE_SIZE]) {
        buf.fill(0);

        // magic
        buf[0..4].copy_from_slice(MAGIC);
        // version (always writes v5)
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
        // pending_free_root_page
        buf[44..52].copy_from_slice(&self.pending_free_root_page.to_le_bytes());
        // tombstone_root_page
        buf[52..60].copy_from_slice(&self.tombstone_root_page.to_le_bytes());
        // catalog_root_checksum (Merkle root)
        buf[60..76].copy_from_slice(&self.catalog_root_checksum.to_le_bytes());
        // XXH3_128 checksum of [0..76]
        let checksum = xxh3_128(&buf[..CHECKSUM_RANGE_END]);
        buf[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 16].copy_from_slice(&checksum.to_le_bytes());
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
            pending_free_root_page: 7,
            tombstone_root_page: 3,
            catalog_root_checksum: 0xDEAD_BEEF_CAFE_BABE_1234_5678_9ABC_DEF0,
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
        let checksum = xxh3_128(&buf[..CHECKSUM_RANGE_END]);
        buf[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 16].copy_from_slice(&checksum.to_le_bytes());

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
        assert_eq!(header.pending_free_root_page, 0);
        assert_eq!(header.tombstone_root_page, 0);
        assert_eq!(header.catalog_root_checksum, 0);
    }

    #[test]
    fn test_v1_header_compat() {
        // Build a v1 header by hand: xxHash64 checksum at [44..52] covers [0..44].
        let mut buf = [0u8; PAGE_SIZE];
        buf[0..4].copy_from_slice(MAGIC);
        buf[4..8].copy_from_slice(&1u32.to_le_bytes()); // version 1
        buf[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        buf[12..20].copy_from_slice(&7u64.to_le_bytes()); // txn_counter = 7
        buf[20..28].copy_from_slice(&3u64.to_le_bytes()); // catalog_root = 3
        buf[28..36].copy_from_slice(&0u64.to_le_bytes()); // free_list = 0
        buf[36..44].copy_from_slice(&10u64.to_le_bytes()); // total_pages = 10

        let checksum = xxh64(&buf[..V1_CHECKSUM_RANGE_END], 0);
        buf[V1_CHECKSUM_OFFSET..V1_CHECKSUM_OFFSET + 8].copy_from_slice(&checksum.to_le_bytes());

        let header = FileHeader::from_page(&buf).unwrap();
        assert_eq!(header.txn_counter, 7);
        assert_eq!(header.catalog_root_page, 3);
        assert_eq!(header.total_page_count, 10);
        assert_eq!(header.pending_free_root_page, 0);
        assert_eq!(header.tombstone_root_page, 0);
    }

    #[test]
    fn test_v2_header_compat() {
        // Build a v2 header by hand: xxHash64 checksum at [52..60] covers [0..52].
        let mut buf = [0u8; PAGE_SIZE];
        buf[0..4].copy_from_slice(MAGIC);
        buf[4..8].copy_from_slice(&2u32.to_le_bytes()); // version 2
        buf[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        buf[12..20].copy_from_slice(&7u64.to_le_bytes()); // txn_counter = 7
        buf[20..28].copy_from_slice(&3u64.to_le_bytes()); // catalog_root = 3
        buf[28..36].copy_from_slice(&0u64.to_le_bytes()); // free_list = 0
        buf[36..44].copy_from_slice(&10u64.to_le_bytes()); // total_pages = 10
        buf[44..52].copy_from_slice(&5u64.to_le_bytes()); // pending_free = 5

        let checksum = xxh64(&buf[..V2_CHECKSUM_RANGE_END], 0);
        buf[V2_CHECKSUM_OFFSET..V2_CHECKSUM_OFFSET + 8].copy_from_slice(&checksum.to_le_bytes());

        let header = FileHeader::from_page(&buf).unwrap();
        assert_eq!(header.txn_counter, 7);
        assert_eq!(header.catalog_root_page, 3);
        assert_eq!(header.total_page_count, 10);
        assert_eq!(header.pending_free_root_page, 5);
        assert_eq!(header.tombstone_root_page, 0);
    }

    #[test]
    fn test_v3_header_compat() {
        // Build a v3 header by hand: xxHash64 checksum at [60..68] covers [0..60].
        let mut buf = [0u8; PAGE_SIZE];
        buf[0..4].copy_from_slice(MAGIC);
        buf[4..8].copy_from_slice(&3u32.to_le_bytes()); // version 3
        buf[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        buf[12..20].copy_from_slice(&7u64.to_le_bytes()); // txn_counter = 7
        buf[20..28].copy_from_slice(&3u64.to_le_bytes()); // catalog_root = 3
        buf[28..36].copy_from_slice(&0u64.to_le_bytes()); // free_list = 0
        buf[36..44].copy_from_slice(&10u64.to_le_bytes()); // total_pages = 10
        buf[44..52].copy_from_slice(&5u64.to_le_bytes()); // pending_free = 5
        buf[52..60].copy_from_slice(&8u64.to_le_bytes()); // tombstone = 8

        let checksum = xxh64(&buf[..V3_CHECKSUM_RANGE_END], 0);
        buf[V3_CHECKSUM_OFFSET..V3_CHECKSUM_OFFSET + 8].copy_from_slice(&checksum.to_le_bytes());

        let header = FileHeader::from_page(&buf).unwrap();
        assert_eq!(header.txn_counter, 7);
        assert_eq!(header.catalog_root_page, 3);
        assert_eq!(header.total_page_count, 10);
        assert_eq!(header.pending_free_root_page, 5);
        assert_eq!(header.tombstone_root_page, 8);
        assert_eq!(header.catalog_root_checksum, 0);
    }

    #[test]
    fn test_v4_header_compat() {
        // Build a v4 header by hand: XXH3_128 checksum at [60..76] covers [0..60].
        let mut buf = [0u8; PAGE_SIZE];
        buf[0..4].copy_from_slice(MAGIC);
        buf[4..8].copy_from_slice(&4u32.to_le_bytes()); // version 4
        buf[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        buf[12..20].copy_from_slice(&7u64.to_le_bytes()); // txn_counter = 7
        buf[20..28].copy_from_slice(&3u64.to_le_bytes()); // catalog_root = 3
        buf[28..36].copy_from_slice(&0u64.to_le_bytes()); // free_list = 0
        buf[36..44].copy_from_slice(&10u64.to_le_bytes()); // total_pages = 10
        buf[44..52].copy_from_slice(&5u64.to_le_bytes()); // pending_free = 5
        buf[52..60].copy_from_slice(&8u64.to_le_bytes()); // tombstone = 8

        let checksum = xxh3_128(&buf[..V4_CHECKSUM_RANGE_END]);
        buf[V4_CHECKSUM_OFFSET..V4_CHECKSUM_OFFSET + 16].copy_from_slice(&checksum.to_le_bytes());

        let header = FileHeader::from_page(&buf).unwrap();
        assert_eq!(header.txn_counter, 7);
        assert_eq!(header.catalog_root_page, 3);
        assert_eq!(header.total_page_count, 10);
        assert_eq!(header.pending_free_root_page, 5);
        assert_eq!(header.tombstone_root_page, 8);
        assert_eq!(header.catalog_root_checksum, 0);
    }
}

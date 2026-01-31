//! Versioned document wrapping user data with MVCC metadata.
//!
//! Uses a compact binary serialization format to avoid inefficient JSON-in-JSON
//! since user documents are already JSON bytes.
//!
//! ## Binary format
//!
//! ```text
//! [0..8]   created_txn: u64 LE
//! [8..9]   flags: u8 (bit 0 = has_deleted_txn)
//! [9..17]  deleted_txn: u64 LE (0 if not deleted)
//! [17..25] prev_version_page: u64 LE
//! [25..29] prev_version_len: u32 LE
//! [29..33] data_len: u32 LE
//! [33..]   data bytes
//! ```

use crate::error::StorageError;
use crate::types::{PageId, TxnId};

/// Minimum header size for the binary format (before data bytes).
const HEADER_SIZE: usize = 33;

/// A versioned document wrapping user data with MVCC metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct VersionedDocument {
    pub created_txn: TxnId,
    pub deleted_txn: Option<TxnId>,
    pub data: Vec<u8>,
    /// Page ID of the previous version in the version chain (0 = no previous version).
    pub prev_version_page: PageId,
    /// Total byte length of the serialized previous version (0 = no previous version).
    pub prev_version_len: u32,
}

impl VersionedDocument {
    /// Create a fresh versioned document with no deletion marker and no previous version.
    pub fn new(created_txn: TxnId, data: Vec<u8>) -> Self {
        Self {
            created_txn,
            deleted_txn: None,
            data,
            prev_version_page: 0,
            prev_version_len: 0,
        }
    }

    /// Serialize this document to the compact binary format.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + self.data.len());

        // [0..8] created_txn
        buf.extend_from_slice(&self.created_txn.to_le_bytes());

        // [8..9] flags
        let flags: u8 = if self.deleted_txn.is_some() { 1 } else { 0 };
        buf.push(flags);

        // [9..17] deleted_txn
        let deleted = self.deleted_txn.unwrap_or(0);
        buf.extend_from_slice(&deleted.to_le_bytes());

        // [17..25] prev_version_page
        buf.extend_from_slice(&self.prev_version_page.to_le_bytes());

        // [25..29] prev_version_len
        buf.extend_from_slice(&self.prev_version_len.to_le_bytes());

        // [29..33] data_len
        buf.extend_from_slice(&(self.data.len() as u32).to_le_bytes());

        // [33..] data bytes
        buf.extend_from_slice(&self.data);

        buf
    }

    /// Deserialize a document from the compact binary format.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() < HEADER_SIZE {
            return Err(StorageError::CorruptedPage(format!(
                "versioned document too short: {} bytes, need at least {}",
                bytes.len(),
                HEADER_SIZE
            )));
        }

        let created_txn = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let flags = bytes[8];
        let deleted_raw = u64::from_le_bytes(bytes[9..17].try_into().unwrap());
        let deleted_txn = if flags & 1 != 0 {
            Some(deleted_raw)
        } else {
            None
        };
        let prev_version_page = u64::from_le_bytes(bytes[17..25].try_into().unwrap());
        let prev_version_len = u32::from_le_bytes(bytes[25..29].try_into().unwrap());
        let data_len = u32::from_le_bytes(bytes[29..33].try_into().unwrap()) as usize;

        if bytes.len() < HEADER_SIZE + data_len {
            return Err(StorageError::CorruptedPage(format!(
                "versioned document data truncated: have {} bytes after header, need {}",
                bytes.len() - HEADER_SIZE,
                data_len
            )));
        }

        let data = bytes[HEADER_SIZE..HEADER_SIZE + data_len].to_vec();

        Ok(Self {
            created_txn,
            deleted_txn,
            data,
            prev_version_page,
            prev_version_len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let doc = VersionedDocument {
            created_txn: 42,
            deleted_txn: Some(100),
            data: b"hello world".to_vec(),
            prev_version_page: 7,
            prev_version_len: 55,
        };

        let bytes = doc.serialize();
        let recovered = VersionedDocument::deserialize(&bytes).unwrap();
        assert_eq!(doc, recovered);
    }

    #[test]
    fn test_new_document() {
        let doc = VersionedDocument::new(10, b"data".to_vec());
        assert_eq!(doc.created_txn, 10);
        assert_eq!(doc.deleted_txn, None);
        assert_eq!(doc.data, b"data");
        assert_eq!(doc.prev_version_page, 0);
        assert_eq!(doc.prev_version_len, 0);
    }

    #[test]
    fn test_serialize_with_deleted() {
        let doc = VersionedDocument {
            created_txn: 1,
            deleted_txn: Some(5),
            data: b"test".to_vec(),
            prev_version_page: 0,
            prev_version_len: 0,
        };

        let bytes = doc.serialize();
        let recovered = VersionedDocument::deserialize(&bytes).unwrap();
        assert_eq!(recovered.deleted_txn, Some(5));
    }

    #[test]
    fn test_serialize_with_prev_version() {
        let doc = VersionedDocument {
            created_txn: 1,
            deleted_txn: None,
            data: vec![],
            prev_version_page: 99,
            prev_version_len: 200,
        };

        let bytes = doc.serialize();
        let recovered = VersionedDocument::deserialize(&bytes).unwrap();
        assert_eq!(recovered.prev_version_page, 99);
        assert_eq!(recovered.prev_version_len, 200);
    }

    #[test]
    fn test_deserialize_too_short() {
        let bytes = [0u8; 10]; // way too short
        let result = VersionedDocument::deserialize(&bytes);
        assert!(result.is_err());
    }
}

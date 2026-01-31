use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::path::Path;

use crate::error::StorageError;

/// A file-based lock using `flock(2)`.
///
/// The lock is released automatically when this struct is dropped (the
/// underlying file descriptor is closed).
#[derive(Debug)]
pub struct FileLock {
    _file: File,
}

impl FileLock {
    /// Acquire an exclusive (write) lock on the file at `path`.
    ///
    /// Creates the lock file if it does not exist. Blocks until the lock is acquired.
    pub fn exclusive(path: &Path) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        // SAFETY: `flock` is safe to call with a valid file descriptor obtained
        // from `AsRawFd`. The fd remains valid for the lifetime of `file`.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if rc != 0 {
            return Err(StorageError::FileLocked);
        }

        Ok(Self { _file: file })
    }

    /// Acquire a shared (read) lock on the file at `path`.
    ///
    /// Creates the lock file if it does not exist. Blocks until the lock is acquired.
    pub fn shared(path: &Path) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        // SAFETY: `flock` is safe to call with a valid file descriptor obtained
        // from `AsRawFd`. The fd remains valid for the lifetime of `file`.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_SH) };
        if rc != 0 {
            return Err(StorageError::FileLocked);
        }

        Ok(Self { _file: file })
    }

    /// Try to acquire an exclusive lock without blocking.
    /// Returns `Err(StorageError::FileLocked)` if the lock is held.
    pub fn try_exclusive(path: &Path) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        // SAFETY: `flock` is safe to call with a valid file descriptor obtained
        // from `AsRawFd`. The fd remains valid for the lifetime of `file`.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            return Err(StorageError::FileLocked);
        }

        Ok(Self { _file: file })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_exclusive_lock() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("test.lock");

        let lock = FileLock::exclusive(&lock_path).unwrap();
        // Lock is held — verify by trying a non-blocking exclusive lock
        assert!(FileLock::try_exclusive(&lock_path).is_err());
        drop(lock);
    }

    #[test]
    fn test_shared_locks() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("test.lock");

        // Two shared locks should coexist
        let _lock1 = FileLock::shared(&lock_path).unwrap();
        let _lock2 = FileLock::shared(&lock_path).unwrap();
    }

    #[test]
    fn test_exclusive_blocks_exclusive() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("test.lock");

        let _lock = FileLock::exclusive(&lock_path).unwrap();

        // A non-blocking attempt from the same process should fail
        // (flock is per-fd, so a different fd from the same process will block)
        let result = FileLock::try_exclusive(&lock_path);
        assert!(result.is_err());
        match result {
            Err(StorageError::FileLocked) => {}
            other => panic!("expected FileLocked, got {other:?}"),
        }
    }

    #[test]
    fn test_lock_released_on_drop() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("test.lock");

        {
            let _lock = FileLock::exclusive(&lock_path).unwrap();
        }
        // After drop, we should be able to acquire the lock again
        let _lock2 = FileLock::try_exclusive(&lock_path).unwrap();
    }
}

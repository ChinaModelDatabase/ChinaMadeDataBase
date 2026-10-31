use std::fmt;

use mdb_common::{ErrorCode, MdbError};

#[derive(Debug, Clone)]
pub struct StorageError {
    pub code: ErrorCode,
    pub message: String,
}

impl StorageError {
    pub fn new(code: ErrorCode, msg: impl fmt::Display) -> Self {
        Self { code, message: msg.to_string() }
    }

    pub fn io(msg: impl fmt::Display) -> Self {
        Self::new(ErrorCode::HostUnreachable, msg)
    }

    pub fn corrupted(msg: impl fmt::Display) -> Self {
        Self::new(ErrorCode::DataCorruption, msg)
    }

    pub fn invalid(msg: impl fmt::Display) -> Self {
        Self::new(ErrorCode::BadValue, msg)
    }

    pub fn not_found(msg: impl fmt::Display) -> Self {
        Self::new(ErrorCode::NamespaceNotFound, msg)
    }

    pub fn busy(msg: impl fmt::Display) -> Self {
        Self::new(ErrorCode::LockBusy, msg)
    }

    pub fn to_mdb_error(self) -> MdbError {
        MdbError::new(self.code, self.message)
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for StorageError {}

pub fn io_err(msg: impl fmt::Display) -> MdbError {
    StorageError::io(msg).to_mdb_error()
}

pub fn corrupt_err(msg: impl fmt::Display) -> MdbError {
    StorageError::corrupted(msg).to_mdb_error()
}

pub fn invalid_err(msg: impl fmt::Display) -> MdbError {
    StorageError::invalid(msg).to_mdb_error()
}

pub fn not_found_err(msg: impl fmt::Display) -> MdbError {
    StorageError::not_found(msg).to_mdb_error()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_error_display() {
        let e = StorageError::io("disk full");
        assert!(e.message.contains("disk full"));
        assert_eq!(e.code, ErrorCode::HostUnreachable);
    }

    #[test]
    fn corrupt_err_conversion() {
        let e = corrupt_err("bad crc");
        assert_eq!(e.code(), ErrorCode::DataCorruption);
        assert!(e.message().contains("bad crc"));
    }

    #[test]
    fn not_found_err_conversion() {
        let e = not_found_err("no such file");
        assert_eq!(e.code(), ErrorCode::NamespaceNotFound);
    }
}

use std::{
    path::PathBuf,
    sync::{MutexGuard, PoisonError},
};

use crate::ffi::Mdb;

/// Enum listing for errors from mdbsql.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Error when given path is not a file.
    #[error("invalid path: {0}")]
    InvalidPath(PathBuf),

    /// Error when given path is not a valid mdb file.
    #[error("invalid mdb file: {0}")]
    InvalidMdbFile(PathBuf),

    /// Error from libmdbsql
    #[error("{0}")]
    MdbSqlError(String),

    /// Error converting a string to c-string.
    #[error(transparent)]
    NulError(#[from] std::ffi::NulError),

    /// Error converting a string to utf8.
    #[error(transparent)]
    Utf8Error(#[from] std::str::Utf8Error),

    /// Poison Error for `MutexGuard<Mdb>`
    #[error("{0}")]
    MutexPoisonError(String),

    /// Error when access to row value with invalid index
    #[error("invalid index to row results: {0}")]
    InvalidRowIndex(usize),

    /// Error converting SQL value to `T`
    #[error(transparent)]
    FromSqlError(#[from] serde_plain::Error),
}

impl From<PoisonError<MutexGuard<'_, Mdb>>> for Error {
    fn from(error: PoisonError<MutexGuard<Mdb>>) -> Self {
        Self::MutexPoisonError(error.to_string())
    }
}

#[cfg(feature = "rusqlite")]
impl From<Error> for rusqlite::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::InvalidPath(p) => Self::InvalidPath(p),
            Error::NulError(e) => Self::NulError(e),
            Error::Utf8Error(e) => Self::Utf8Error(e),
            err => Self::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::Unknown,
                    extended_code: 0,
                },
                Some(err.to_string()),
            ),
        }
    }
}

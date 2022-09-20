use std::sync::{MutexGuard, PoisonError};

use crate::mdbsql::Mdb;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid path")]
    InvalidPath,
    #[error("invalid mdb file")]
    InvalidMdbFile,
    #[error("mdb sql error: {0}")]
    MdbSqlError(String),
    #[error(transparent)]
    NullError(#[from] std::ffi::NulError),
    #[error(transparent)]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("poison error")]
    PoisonError,
}

impl From<PoisonError<MutexGuard<'_, Mdb>>> for Error {
    fn from(_e: PoisonError<MutexGuard<'_, Mdb>>) -> Self {
        Self::PoisonError
    }
}

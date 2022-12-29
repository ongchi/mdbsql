//! SQL query for Access database on Unix-like systems.
//!
//! This is a simple wrapper for [libmdbsql](https://github.com/mdbtools/mdbtools) in Rust.
//!
//! The implemented SQL subset is limited, please refer to [mdb-sql](https://man.cx/mdb-sql(1)).

//!
//! # Example
//!
//! ```rust
//! use mdbsql::{Connection, Error};
//!
//! # fn main() -> Result<(), Error> {
//! # let path = "./resource/test.mdb";
//! let conn = Connection::open(path)?;
//! let rows = conn.prepare("SELECT ID, A FROM Table1 WHERE ID = 1")?;
//! let col_names: Vec<String> = rows
//!     .columns()
//!     .iter()
//!     .map(|c| c.name())
//!     .collect();
//!
//! assert_eq!(col_names, vec!["ID", "A"]);
//!
//! for row in rows {
//!     let col1: u32 = row.get(0)?;
//!     let col2: String = row.get(1)?;
//!     assert_eq!(col1, 1);
//!     assert_eq!(col2, "Foo");
//! };
//! # Ok(())
//! # }
//! ````
#[deny(missing_docs)]
mod error;
mod ffi;
pub mod mdbsql;
#[cfg(feature = "rusqlite")]
mod rusqlite;

pub use crate::error::Error;
pub use crate::mdbsql::Connection;

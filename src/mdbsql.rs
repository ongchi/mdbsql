use std::ffi::CString;
use std::os::raw::c_char;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use crate::error::Error;
use crate::ffi::{Mdb, SqlColumn, SqlValue};

/// A connection to a mdb database.
#[derive(Debug)]
pub struct Connection {
    db: Mutex<Mdb>,
}

impl Connection {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Ok(Self {
            db: Mutex::new(Mdb::open(path)?),
        })
    }

    pub fn prepare(&self, query: &str) -> Result<Rows<'_>, Error> {
        let guard = self.db.lock()?;

        let query = CString::new(query)?;
        let query = query.as_ptr() as *const c_char;
        guard.sql_run_query(query);

        match guard.error_msg() {
            None => Ok(guard.into()),
            Some(msg) => Err(Error::MdbSqlError(msg)),
        }
    }
}

/// A handle for rows of query result.
pub struct Rows<'mdb> {
    mdb_guard: MutexGuard<'mdb, Mdb>,
    columns: Vec<SqlColumn>,
}

impl<'mdb> Rows<'mdb> {
    pub fn columns(&self) -> &[SqlColumn] {
        &self.columns
    }
}

impl<'mdb> From<MutexGuard<'mdb, Mdb>> for Rows<'mdb> {
    fn from(mdb_guard: MutexGuard<'mdb, Mdb>) -> Self {
        let columns = mdb_guard.sql_columns();
        Self { mdb_guard, columns }
    }
}

impl<'mdb> Iterator for Rows<'mdb> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.mdb_guard.sql_fetch_row() {
            let values = self.mdb_guard.sql_bound_values();
            Some(Row { values })
        } else {
            None
        }
    }
}

impl<'mdb> Drop for Rows<'mdb> {
    fn drop(&mut self) {
        // reset query results when drop
        self.mdb_guard.reset()
    }
}

/// Row of values.
pub struct Row {
    values: Vec<SqlValue>,
}

impl Row {
    /// Get value at index.
    pub fn get<T: FromSql>(&self, idx: usize) -> Result<T, Error> {
        if idx < self.values.len() {
            T::column_result(self.values[idx].get()?)
        } else {
            Err(Error::InvalidRowIndex(idx))
        }
    }
}

pub trait FromSql: Sized {
    /// Converts SQL value into Rust value.
    fn column_result(value: &str) -> Result<Self, Error>;
}

impl<T> FromSql for T
where
    T: serde::de::DeserializeOwned,
{
    fn column_result(value: &str) -> Result<T, Error> {
        Ok(serde_plain::from_str(value)?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[derive(Debug, PartialEq)]
    struct Table1 {
        id: u64,
        a: String,
        b: i64,
        c: f64,
        d: String,
        e: u8,
        f: String,
    }

    #[test]
    fn connection() {
        let conn = Connection::open("resource/test.mdb").unwrap();
        let rows = conn.prepare("select * from Table1 where ID=1").unwrap();
        let col_names: Vec<String> = rows.columns().iter().map(|c| c.name()).collect();

        let tables: Vec<Table1> = rows
            .map(|r| Table1 {
                id: r.get(0).unwrap(),
                a: r.get(1).unwrap(),
                b: r.get(2).unwrap(),
                c: r.get(3).unwrap(),
                d: r.get(4).unwrap(),
                e: r.get(5).unwrap(),
                f: r.get(6).unwrap(),
            })
            .collect();

        assert_eq!(col_names, vec!["ID", "A", "B", "C", "D", "E", "F"]);
        assert_eq!(
            tables[0],
            Table1 {
                id: 1,
                a: "Foo".to_string(),
                b: 1,
                c: 1.0000,
                d: "01/01/00 00:00:00".to_string(),
                e: 1,
                f: "<div><font face=Calibri>FooBar</font></div>".to_string()
            }
        );
    }

    #[test]
    fn open_nonexistent_path() {
        let err = Connection::open("resource/nonexistent.mdb").unwrap_err();
        assert!(matches!(err, Error::InvalidPath(_)));
    }

    #[test]
    fn open_invalid_mdb_file() {
        // A non-mdb file that exists
        let err = Connection::open("Cargo.toml").unwrap_err();
        assert!(matches!(err, Error::InvalidMdbFile(_)));
    }

    #[test]
    fn invalid_sql_query() {
        let conn = Connection::open("resource/test.mdb").unwrap();
        let result = conn.prepare("SELECT * FROM NonExistentTable");
        assert!(matches!(result, Err(Error::MdbSqlError(_))));
    }

    #[test]
    fn invalid_row_index() {
        let conn = Connection::open("resource/test.mdb").unwrap();
        let rows = conn.prepare("select ID from Table1 where ID=1").unwrap();
        let row = rows.into_iter().next().unwrap();
        let err = row.get::<u32>(99).unwrap_err();
        assert!(matches!(err, Error::InvalidRowIndex(99)));
    }

    #[test]
    fn type_conversion_error() {
        let conn = Connection::open("resource/test.mdb").unwrap();
        let rows = conn.prepare("select A from Table1 where ID=1").unwrap();
        for row in rows {
            // Column A is "Foo", which cannot be parsed as u32
            let err = row.get::<u32>(0).unwrap_err();
            assert!(
                matches!(err, Error::FromSqlError(_)),
                "expected FromSqlError, got: {:?}",
                err
            );
        }
    }

    #[test]
    fn sequential_queries() {
        let conn = Connection::open("resource/test.mdb").unwrap();

        // First query
        {
            let rows = conn.prepare("select ID from Table1 where ID=1").unwrap();
            let ids: Vec<u32> = rows.map(|r| r.get(0).unwrap()).collect();
            assert_eq!(ids, vec![1]);
        }
        // Rows dropped, query state reset — second query should work
        {
            let rows = conn.prepare("select ID from Table1 where ID=2").unwrap();
            let ids: Vec<u32> = rows.map(|r| r.get(0).unwrap()).collect();
            assert_eq!(ids, vec![2]);
        }
    }

    #[test]
    fn select_all_rows() {
        let conn = Connection::open("resource/test.mdb").unwrap();
        let rows = conn.prepare("select ID from Table1").unwrap();
        let ids: Vec<u32> = rows.map(|r| r.get(0).unwrap()).collect();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn empty_result_set() {
        let conn = Connection::open("resource/test.mdb").unwrap();
        let rows = conn.prepare("select ID from Table1 where ID=999").unwrap();
        let ids: Vec<u32> = rows.map(|r| r.get(0).unwrap()).collect();
        assert!(ids.is_empty());
    }

    #[test]
    fn column_count() {
        let conn = Connection::open("resource/test.mdb").unwrap();
        let rows = conn.prepare("select ID, A, B from Table1").unwrap();
        assert_eq!(rows.columns().len(), 3);
        assert_eq!(
            rows.columns().iter().map(|c| c.name()).collect::<Vec<_>>(),
            vec!["ID", "A", "B"]
        );
    }

    #[test]
    fn multithreading() {
        let conn = Arc::new(Connection::open("resource/test.mdb").unwrap());

        let cap = 10;
        let mut threads = Vec::with_capacity(cap);
        (0..cap).for_each(|i| {
            let conn_clone = conn.clone();

            threads.push(thread::spawn(move || {
                if i % 2 == 0 {
                    let rows = conn_clone.prepare("select ID from Table1").unwrap();
                    assert_eq!(rows.columns()[0].name(), "ID");
                    assert_eq!(
                        rows.map(|r| r.get(0).unwrap()).collect::<Vec<u32>>(),
                        vec![1, 2]
                    );
                } else {
                    let rows = conn_clone.prepare("select E from Table1").unwrap();
                    assert_eq!(rows.columns()[0].name(), "E");
                    assert_eq!(
                        rows.map(|r| r.get(0).unwrap()).collect::<Vec<u8>>(),
                        vec![1, 0]
                    );
                }
            }));
        });

        threads
            .into_iter()
            .for_each(|thread| thread.join().unwrap());
    }
}

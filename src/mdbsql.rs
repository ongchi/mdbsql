use std::ffi::CString;
use std::os::raw::c_char;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use crate::error::Error;
use crate::ffi::{Column, Mdb, Value};

/// A connection to a mdb database.
pub struct Connection {
    db: Mutex<Mdb>,
}

impl Connection {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Ok(Self {
            db: Mutex::new(Mdb::open(path)?),
        })
    }

    pub fn prepare(&self, query: &str) -> Result<Rows, Error> {
        let guard = self.db.lock()?;

        let query = CString::new(query)?;
        let query = query.as_ptr() as *const c_char;
        guard.run_query(query);

        match guard.error_msg() {
            None => Ok(guard.into()),
            Some(msg) => Err(Error::MdbSqlError(msg)),
        }
    }
}

/// A handle for rows of query result.
pub struct Rows<'mdb> {
    mdb_guard: MutexGuard<'mdb, Mdb>,
    columns: Vec<Column>,
}

impl<'mdb> Rows<'mdb> {
    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }
}

impl<'mdb> From<MutexGuard<'mdb, Mdb>> for Rows<'mdb> {
    fn from(mdb_guard: MutexGuard<'mdb, Mdb>) -> Self {
        let columns = mdb_guard.columns();
        Self { mdb_guard, columns }
    }
}

impl<'mdb> Iterator for Rows<'mdb> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.mdb_guard.fetch_row() {
            let values = self.mdb_guard.bound_values();
            Some(Row { values })
        } else {
            self.mdb_guard.reset();
            None
        }
    }
}

/// Row of values.
pub struct Row {
    values: Vec<Value>,
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

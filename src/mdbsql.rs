use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use crate::error::Error;

#[allow(clippy::all)]
#[allow(non_upper_case_globals)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[allow(dead_code)]
mod ffi {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub(crate) struct Mdb(*mut ffi::MdbSQL);

unsafe impl Send for Mdb {}

impl Drop for Mdb {
    fn drop(&mut self) {
        unsafe { ffi::mdb_sql_exit(self.0) }
    }
}

impl Mdb {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        if !path.as_ref().is_file() {
            return Err(Error::InvalidPath);
        }

        let path = CString::new(path.as_ref().as_os_str().as_bytes())?;
        let path = path.as_ptr() as *const c_char;

        unsafe {
            let mdb_handle = ffi::mdb_open(path, ffi::MdbFileFlags_MDB_NOFLAGS);
            if mdb_handle.is_null() {
                Err(Error::InvalidMdbFile)
            } else {
                let db_ptr = ffi::mdb_sql_init();
                (*db_ptr).mdb = mdb_handle;
                Ok(Mdb(db_ptr))
            }
        }
    }
}

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
        self.db
            .lock()
            .map(|guard| run_query(guard, query))?
            .map(check_error)?
            .map(Into::into)
    }
}

/// A handle for rows of query result.
pub struct Rows<'a> {
    mdb_guard: MutexGuard<'a, Mdb>,
    names: Vec<String>,
}

impl<'a> Rows<'a> {
    pub fn names(&self) -> &Vec<String> {
        &self.names
    }
}

impl<'a> From<MutexGuard<'a, Mdb>> for Rows<'a> {
    fn from(mdb_guard: MutexGuard<'a, Mdb>) -> Self {
        let db_ptr = (*mdb_guard).0;

        let columns = unsafe {
            let n_cols = (*(*db_ptr).columns).len as isize;
            let cols = (*(*db_ptr).columns).pdata as *const *const ffi::MdbSQLColumn;
            (0..n_cols)
                .into_iter()
                .map(|i| *cols.offset(i))
                .map(|col| (*col).name)
                .map(|s| cstr_to_string(s).unwrap())
                .collect()
        };

        Self {
            mdb_guard,
            names: columns,
        }
    }
}

impl<'a> Iterator for Rows<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let db_ptr = (*self.mdb_guard).0;
            if ffi::mdb_sql_fetch_row(db_ptr, (*db_ptr).cur_table) == 1 {
                let len = (*db_ptr).num_columns as isize;
                let vals_ptr = (*(*db_ptr).bound_values).pdata as *const *const c_char;
                Some(Row {
                    values: (0..len).into_iter().map(|i| *vals_ptr.offset(i)).collect(),
                })
            } else {
                ffi::mdb_sql_reset(db_ptr);
                None
            }
        }
    }
}

/// Row of values.
#[derive(Debug)]
pub struct Row {
    values: Vec<*const c_char>,
}

impl Row {
    /// Get value at index.
    pub fn get<T: FromSql>(&self, idx: usize) -> Result<T, Error> {
        if idx <= self.values.len() {
            T::column_result(self.values[idx])
        } else {
            Err(Error::InvalidRowIndex(idx))
        }
    }
}

pub trait FromSql: Sized {
    /// Converts SQL value into Rust value.
    fn column_result(value: *const c_char) -> Result<Self, Error>;
}

impl<T> FromSql for T
where
    T: serde::de::DeserializeOwned,
{
    fn column_result(value: *const c_char) -> Result<T, Error> {
        let value = unsafe { CStr::from_ptr(value) };
        let value = value.to_str()?.trim();
        Ok(serde_plain::from_str(value)?)
    }
}

fn run_query<'a, 'b>(
    mdb_guard: MutexGuard<'a, Mdb>,
    query: &'b str,
) -> Result<MutexGuard<'a, Mdb>, Error> {
    let query = CString::new(query)?;
    let query = query.as_ptr() as *const c_char;
    unsafe { ffi::mdb_sql_run_query((*mdb_guard).0, query) };
    Ok(mdb_guard)
}

fn check_error(mdb_guard: MutexGuard<Mdb>) -> Result<MutexGuard<Mdb>, Error> {
    let db_ptr = (*mdb_guard).0;
    let msg = unsafe { (*db_ptr).error_msg };
    if msg[0] == 0 {
        Ok(mdb_guard)
    } else {
        let e_msg = cstr_to_string(msg.as_ptr())?;
        unsafe { ffi::mdb_sql_reset(db_ptr) };
        Err(Error::MdbSqlError(e_msg))
    }
}

fn cstr_to_string(s: *const c_char) -> Result<String, Error> {
    let cstr = unsafe { CStr::from_ptr(s) };
    Ok(cstr.to_str()?.trim().to_string())
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
        let names = rows.names().clone();
        let tables: Vec<Table1> = rows
            .into_iter()
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

        assert_eq!(names, vec!["ID", "A", "B", "C", "D", "E", "F"]);
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

        let cap = 1000;
        let mut threads = Vec::with_capacity(cap);
        (0..cap).for_each(|i| {
            let conn_clone = conn.clone();

            threads.push(thread::spawn(move || {
                if i % 2 == 0 {
                    let rows = conn_clone.prepare("select ID from Table1").unwrap();
                    assert_eq!(rows.names(), &vec!["ID"]);
                    assert_eq!(
                        rows.into_iter()
                            .map(|r| r.get(0).unwrap())
                            .collect::<Vec<u32>>(),
                        vec![1, 2]
                    );
                } else {
                    let rows = conn_clone.prepare("select E from Table1").unwrap();
                    assert_eq!(rows.names(), &vec!["E"]);
                    assert_eq!(
                        rows.into_iter()
                            .map(|r| r.get(0).unwrap())
                            .collect::<Vec<u8>>(),
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

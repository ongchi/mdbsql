use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

#[allow(clippy::all)]
#[allow(non_upper_case_globals)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[allow(dead_code)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub(crate) use bindings::{
    mdb_open, mdb_sql_exit, mdb_sql_fetch_row, mdb_sql_init, mdb_sql_reset, mdb_sql_run_query,
    MdbFileFlags_MDB_NOFLAGS, MdbSQL, MdbSQLColumn,
};

use crate::error::Error;

pub struct Mdb(*mut MdbSQL);

unsafe impl Send for Mdb {}

impl Drop for Mdb {
    fn drop(&mut self) {
        unsafe { mdb_sql_exit(self.0) }
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
            let mdb_handle = mdb_open(path, MdbFileFlags_MDB_NOFLAGS);
            if mdb_handle.is_null() {
                Err(Error::InvalidMdbFile)
            } else {
                let mdb = mdb_sql_init();
                (*mdb).mdb = mdb_handle;
                Ok(Mdb(mdb))
            }
        }
    }
}

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
            .map(Rows::new)
    }
}

pub struct Rows<'a> {
    mdb_guard: MutexGuard<'a, Mdb>,
    columns: Vec<String>,
}

impl<'a> Rows<'a> {
    fn new(mdb_guard: MutexGuard<'a, Mdb>) -> Self {
        let db = (*mdb_guard).0;

        let columns = unsafe {
            let n_cols = (*(*db).columns).len as isize;
            let cols = (*(*db).columns).pdata as *const *const MdbSQLColumn;
            (0..n_cols)
                .into_iter()
                .map(|i| *cols.offset(i))
                .map(|col| (*col).name)
                .map(|str| cstr_to_string(str).unwrap())
                .collect()
        };

        Self { mdb_guard, columns }
    }

    pub fn columns(&self) -> &Vec<String> {
        &self.columns
    }
}

impl<'a> Iterator for Rows<'a> {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let db = (*self.mdb_guard).0;
            if mdb_sql_fetch_row(db, (*db).cur_table) == 1 {
                let n_vals = (*db).num_columns as isize;
                let vals = (*(*db).bound_values).pdata as *const *const c_char;
                let string_results = (0..n_vals)
                    .into_iter()
                    .map(|i| *vals.offset(i))
                    .map(|cstr| cstr_to_string(cstr).unwrap())
                    .collect();
                Some(string_results)
            } else {
                mdb_sql_reset(db);
                None
            }
        }
    }
}

fn run_query<'a, 'b>(
    mdb_guard: MutexGuard<'a, Mdb>,
    query: &'b str,
) -> Result<MutexGuard<'a, Mdb>, Error> {
    let query = CString::new(query)?;
    let query = query.as_ptr() as *const c_char;
    unsafe { mdb_sql_run_query((*mdb_guard).0, query) };
    check_error(mdb_guard)
}

fn check_error(mdb_guard: MutexGuard<Mdb>) -> Result<MutexGuard<Mdb>, Error> {
    let db = (*mdb_guard).0;
    let msg = unsafe { (*db).error_msg };
    if msg[0] == 0 {
        Ok(mdb_guard)
    } else {
        let e_msg = cstr_to_string(msg.as_ptr())?;
        unsafe { mdb_sql_reset(db) };
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

    #[test]
    fn connection() {
        let conn = Connection::open("resource/test.mdb").unwrap();
        let rows = conn.prepare("select * from Table1").unwrap();

        assert_eq!(rows.columns(), &vec!["ID", "A", "B", "C", "D", "E", "F"]);
        assert_eq!(
            rows.into_iter().collect::<Vec<_>>(),
            vec![
                vec![
                    "1",
                    "Foo",
                    "1",
                    "1.0000",
                    "01/01/00 00:00:00",
                    "1",
                    "<div><font face=Calibri>FooBar</font></div>"
                ],
                vec![
                    "2",
                    "fOO",
                    "100",
                    "99.0000",
                    "01/01/99 00:00:00",
                    "0",
                    "<div><font face=Calibri>FOOBARBAZ</font></div>"
                ],
            ]
        );
    }

    #[test]
    fn mutex() {
        let conn = Arc::new(Connection::open("resource/test.mdb").unwrap());

        let cap = 1000;
        let mut threads = Vec::with_capacity(cap);
        (0..cap).for_each(|i| {
            let conn_clone = conn.clone();

            threads.push(thread::spawn(move || {
                if i % 2 == 0 {
                    let rows = conn_clone.prepare("select ID from Table1").unwrap();
                    assert_eq!(rows.columns(), &vec!["ID"]);
                    assert_eq!(
                        rows.into_iter().collect::<Vec<_>>(),
                        vec![vec!["1"], vec!["2"]]
                    );
                } else {
                    let rows = conn_clone.prepare("select E from Table1").unwrap();
                    assert_eq!(rows.columns(), &vec!["E"]);
                    assert_eq!(
                        rows.into_iter().collect::<Vec<_>>(),
                        vec![vec!["1"], vec!["0"]]
                    );
                }
            }));
        });

        threads
            .into_iter()
            .for_each(|thread| thread.join().unwrap());
    }
}

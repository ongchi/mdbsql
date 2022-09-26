use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use crate::error::Error;

#[allow(clippy::all)]
#[allow(non_upper_case_globals)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[allow(dead_code)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
use bindings::*;

#[derive(Debug)]
pub struct Value(*const c_char);

impl Value {
    pub fn get(&self) -> Result<&str, Error> {
        unsafe { Ok(CStr::from_ptr(self.0).to_str()?) }
    }
}

#[derive(Debug)]
pub struct Column(*const MdbSQLColumn);

impl Column {
    pub fn name(&self) -> String {
        unsafe { CStr::from_ptr((*self.0).name).to_str().unwrap().to_string() }
    }
}

pub struct Mdb(*mut MdbSQL);

unsafe impl Send for Mdb {}

impl Drop for Mdb {
    fn drop(&mut self) {
        unsafe { mdb_sql_exit(self.0) }
    }
}

impl Mdb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
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
                let db_ptr = mdb_sql_init();
                (*db_ptr).mdb = mdb_handle;
                Ok(Mdb(db_ptr))
            }
        }
    }

    pub fn columns(&self) -> Vec<Column> {
        unsafe {
            let columns = *(*self.0).columns;
            let columns_data = columns.pdata as *const *const MdbSQLColumn;
            (0..columns.len as isize)
                .into_iter()
                .map(|i| *columns_data.offset(i))
                .map(Column)
                .collect()
        }
    }

    pub fn bound_values(&self) -> Vec<Value> {
        unsafe {
            let values = *(*self.0).bound_values;
            let values_data = values.pdata as *const *const c_char;
            (0..values.len as isize)
                .into_iter()
                .map(|i| *values_data.offset(i))
                .map(Value)
                .collect()
        }
    }

    pub fn run_query(&self, query: *const c_char) {
        unsafe {
            mdb_sql_run_query(self.0, query);
        }
    }

    pub fn error_msg(&self) -> Option<String> {
        unsafe {
            let error_msg = (*self.0).error_msg;
            match error_msg[0] {
                0 => None,
                _ => {
                    let msg = CStr::from_ptr(error_msg.as_ptr());
                    let msg = msg.to_str().unwrap().to_string();
                    Some(msg)
                }
            }
        }
    }

    pub fn fetch_row(&self) -> bool {
        unsafe { mdb_sql_fetch_row(self.0, (*self.0).cur_table) == 1 }
    }

    pub fn reset(&self) {
        unsafe { mdb_sql_reset(self.0) }
    }
}

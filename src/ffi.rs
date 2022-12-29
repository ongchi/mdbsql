use glib_sys::{g_free, GPtrArray};
use libc::{c_char, c_int, c_void, size_t};
use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr;
use std::string::ToString;

use libmdb_sys::{
    mdb_bind_column, mdb_fetch_row, mdb_is_system_table, mdb_normalise_and_replace,
    mdb_ole_read_full, mdb_open, mdb_print_col, mdb_print_schema, mdb_read_catalog,
    mdb_read_columns, mdb_read_table_by_name, mdb_rewind_table, mdb_set_bind_size,
    mdb_set_default_backend, mdb_sql_exit, mdb_sql_fetch_row, mdb_sql_init, mdb_sql_reset,
    mdb_sql_run_query, MdbCatalogEntry, MdbColumn, MdbFileFlags_MDB_NOFLAGS, MdbSQL, MdbSQLColumn,
    MdbTableDef, MDB_OLE, MDB_SHEXP_BULK_INSERT, MDB_SHEXP_INDEXES, MDB_SHEXP_RELATIONS, MDB_TABLE,
};

use crate::error::Error;

const EXPORT_BIND_SIZE: size_t = 200000;

struct PtrArray<T> {
    arr: GPtrArray,
    idx: u32,
    _marker: PhantomData<T>,
}

impl<T> From<GPtrArray> for PtrArray<T> {
    fn from(value: GPtrArray) -> Self {
        Self {
            arr: value,
            idx: 0,
            _marker: Default::default(),
        }
    }
}

impl<T> Iterator for PtrArray<T> {
    type Item = *const T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.arr.len {
            let arr = self.arr.pdata as *const *const T;
            let data = unsafe { *arr.offset(self.idx as isize) };
            self.idx += 1;
            Some(data)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct SqlValue(*const c_char);

impl SqlValue {
    pub fn get(&self) -> Result<&str, Error> {
        unsafe { Ok(CStr::from_ptr(self.0).to_str()?) }
    }
}

#[derive(Debug)]
pub struct SqlColumn(*const MdbSQLColumn);

impl SqlColumn {
    pub fn name(&self) -> String {
        unsafe { CStr::from_ptr((*self.0).name).to_str().unwrap().to_string() }
    }

    pub fn bind_type(&self) -> c_int {
        unsafe { (*self.0).bind_type }
    }
}

#[derive(Debug)]
pub struct Mdb(*mut MdbSQL);

unsafe impl Send for Mdb {}

impl Drop for Mdb {
    fn drop(&mut self) {
        unsafe { mdb_sql_exit(self.0) }
    }
}

impl Mdb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref();
        if !path.is_file() {
            return Err(Error::InvalidPath(path.to_path_buf()));
        }

        let c_path = CString::new(path.as_os_str().as_bytes())?;
        let c_path = c_path.as_ptr();

        unsafe {
            let mdb_handle = mdb_open(c_path, MdbFileFlags_MDB_NOFLAGS);
            if mdb_handle.is_null() {
                Err(Error::InvalidMdbFile(path.to_path_buf()))
            } else {
                let db_ptr = mdb_sql_init();
                (*db_ptr).mdb = mdb_handle;
                Ok(Mdb(db_ptr))
            }
        }
    }

    pub fn table_names(&self) -> Vec<String> {
        unsafe {
            let mdb = (*self.0).mdb;
            mdb_read_catalog(mdb, 1);

            let catalogs: PtrArray<MdbCatalogEntry> = (*(*mdb).catalog).into();
            catalogs
                .into_iter()
                .filter(|e| mdb_is_system_table(*e as _) == 0)
                .map(|e| (*e).object_name)
                .map(|n| CStr::from_ptr(n.as_ptr()).to_str().unwrap().to_string())
                .collect()
        }
    }

    /// Columns for current table
    pub fn sql_columns(&self) -> Vec<SqlColumn> {
        unsafe {
            let columns: PtrArray<MdbSQLColumn> = (*(*self.0).columns).into();
            columns.into_iter().map(SqlColumn).collect()
        }
    }

    pub fn sql_bound_values(&self) -> Vec<SqlValue> {
        unsafe {
            let values: PtrArray<c_char> = (*(*self.0).bound_values).into();
            values.into_iter().map(SqlValue).collect()
        }
    }

    pub fn sql_run_query(&self, query: *const c_char) {
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

    pub fn sql_fetch_row(&self) -> bool {
        unsafe { mdb_sql_fetch_row(self.0, (*self.0).cur_table) == 1 }
    }

    pub fn reset(&self) {
        unsafe { mdb_sql_reset(self.0) }
    }

    pub fn set_default_backend(&self, backend_name: &str) -> Result<(), Error> {
        let backend = CString::new(backend_name)?;
        match unsafe { mdb_set_default_backend((*self.0).mdb, backend.as_ptr()) } {
            0 => Err(Error::MdbSqlError(format!(
                "Invalid backend type: {}",
                backend_name
            ))),
            _ => Ok(()),
        }
    }

    pub fn set_bind_size(&self, bind_size: u64) {
        unsafe {
            mdb_set_bind_size((*self.0).mdb, bind_size);
        }
    }

    pub fn schema(&self, table_name: &str) -> Result<String, Error> {
        unsafe {
            let mdb = (*self.0).mdb;

            let mut buf: *mut c_char = ptr::null_mut();
            let mut buf_sizeloc: size_t = 0;
            let mem_fd = libc::open_memstream(&mut buf, &mut buf_sizeloc);

            let table = table_name.as_ptr() as *mut c_char;
            let namespace: *mut c_char = ptr::null_mut();
            let export_opt = MDB_SHEXP_INDEXES | MDB_SHEXP_RELATIONS | MDB_SHEXP_BULK_INSERT;
            mdb_print_schema(mdb, mem_fd, table, namespace, export_opt);

            libc::fclose(mem_fd);
            let schema = CStr::from_ptr(buf as _).to_str().unwrap().to_string();

            g_free(namespace as _);

            Ok(schema)
        }
    }

    pub fn read_table(&self, table_name: &str) -> Result<*mut MdbTableDef, Error> {
        unsafe {
            let table = mdb_read_table_by_name((*self.0).mdb, table_name.as_ptr() as _, MDB_TABLE);
            if table.is_null() {
                Err(Error::MdbSqlError(format!(
                    "Table {} does not exist in this database.",
                    table_name
                )))
            } else {
                mdb_read_columns(table);
                mdb_rewind_table(table);
                Ok(table)
            }
        }
    }

    pub fn export(&self, table_name: &str) -> Result<String, Error> {
        let quote_text = 1;
        let export_flags = 0;

        self.set_bind_size(EXPORT_BIND_SIZE as u64);

        let table = self.read_table(table_name)?;

        unsafe {
            let mdb = (*self.0).mdb;

            let mut bound_values = vec![];
            for i in 1..=(*table).num_cols {
                let bind_ptr = glib_sys::g_malloc0(EXPORT_BIND_SIZE) as *mut c_void;
                let len_ptr =
                    glib_sys::g_malloc(std::mem::size_of::<c_int>() as size_t) as *mut c_int;

                mdb_bind_column(table, i as c_int, bind_ptr, len_ptr);

                bound_values.push((bind_ptr, len_ptr));
            }

            let mut buf: *mut c_char = ptr::null_mut();
            let mut buf_sizeloc: size_t = 0;
            let mem_fd = libc::open_memstream(&mut buf, &mut buf_sizeloc);

            let backend = (*mdb).default_backend;

            while mdb_fetch_row(table) == 1 {
                let mut quoted_name =
                    (*backend).quote_schema_name.unwrap()(ptr::null(), table_name.as_ptr() as _);
                quoted_name = mdb_normalise_and_replace(mdb, &mut quoted_name);
                libc::fputs("INSERT INTO \0".as_ptr() as _, mem_fd);
                libc::fputs(quoted_name, mem_fd);
                libc::fputs(" (\0".as_ptr() as _, mem_fd);
                g_free(quoted_name as _);

                let cols: PtrArray<MdbColumn> = (*(*(table)).columns).into();
                for (i, c) in cols.enumerate() {
                    if i > 0 {
                        libc::fputs(", \0".as_ptr() as _, mem_fd);
                    }
                    let mut quoted_name = (*c).name.as_ptr() as _;
                    quoted_name = mdb_normalise_and_replace(mdb, &mut quoted_name as _);
                    libc::fputs(quoted_name, mem_fd);
                }

                libc::fputs(") VALUES (\0".as_ptr() as _, mem_fd);

                let cols: PtrArray<MdbColumn> = (*(*(table)).columns).into();
                for (i, c) in cols.enumerate() {
                    let col_type = (*c).col_type;

                    let (value, length) = if col_type == MDB_OLE as i32 {
                        let len_ptr = ptr::null_mut();
                        let bind_ptr = mdb_ole_read_full(mdb, c as _, len_ptr);
                        (bind_ptr, len_ptr as *mut i32)
                    } else {
                        bound_values[i]
                    };

                    if i > 0 {
                        libc::fputs(",\0".as_ptr() as _, mem_fd);
                    }

                    mdb_print_col(
                        mem_fd,
                        value as _,
                        quote_text,
                        col_type,
                        *length,
                        "\"\0".as_ptr() as _,
                        ptr::null_mut(),
                        export_flags,
                    );
                }
                libc::fputs(");\0".as_ptr() as _, mem_fd);
            }

            libc::fclose(mem_fd);

            let stmt = CStr::from_ptr(buf as _).to_str()?.to_string();

            g_free(buf as _);
            for (v_ptr, i_ptr) in bound_values {
                g_free(v_ptr);
                g_free(i_ptr as _);
            }

            Ok(stmt)
        }
    }
}

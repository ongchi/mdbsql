#![allow(clippy::all)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
use libc::FILE;
use std::os::raw::{c_uint, c_void};

// Same layout in GLib and in mdbtools' fakeglib
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct GPtrArray {
    pub pdata: *mut *mut c_void,
    pub len: c_uint,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct GList {
    pub data: *mut c_void,
    pub next: *mut GList,
    pub prev: *mut GList,
}

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

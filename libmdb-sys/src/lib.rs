#![allow(clippy::all)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
use glib_sys::{GList, GPtrArray};
use libc::FILE;
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

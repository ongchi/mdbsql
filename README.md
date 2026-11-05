# mdbsql

**SQL query for Access database on Unix-like systems and Windows.**

This is a simple wrapper for [libmdbsql](https://github.com/mdbtools/mdbtools) in Rust.

The implemented SQL subset is limited, please refer to
[mdb-sql](https://man.cx/mdb-sql(1)).

## Requirements

By default, this crate links to the system `libmdbsql` (from
[mdbtools](https://github.com/mdbtools/mdbtools)) found via `pkg-config`, and
generates bindings with `bindgen`, which needs libclang.

## Features

- `bundled`: compile the vendored mdbtools source (v1.0.1) and link it
  statically. Only a C compiler for the target is needed: no mdbtools, GLib,
  `pkg-config` or libclang. This makes cross compiling easy.
- `buildtime_bindgen`: with `bundled`, run `bindgen` at build time instead of
  using the pre-generated bindings (needs libclang). Use it for targets without
  pre-generated bindings.
- `rusqlite`: load an entire Access database into an in-memory SQLite database.

```toml
[dependencies]
mdbsql = { version = "0.1", features = ["bundled"] }
```

| Features                       | mdbtools                | Bindings      |
|--------------------------------|-------------------------|---------------|
| (none)                         | system, via pkg-config  | bindgen       |
| `bundled`                      | vendored                | pre-generated |
| `bundled`, `buildtime_bindgen` | vendored                | bindgen       |

Pre-generated bindings exist for 64-bit Unix targets (macOS, Linux, BSD) and
`x86_64-pc-windows-gnu`. Windows (MinGW) is supported with `bundled` only.

### Notes on `bundled`

The bundled build uses mdbtools' own small GLib replacement instead of GLib.
Its hash tables use a linear search, and case-insensitive matching of non-ASCII
names depends on the C locale.

mdbtools is licensed under LGPL-2.0-or-later. With `bundled`, it is linked
statically into your binary. If you distribute such a binary, you must comply
with the LGPL, which includes letting users relink it against a modified
mdbtools (for example by providing your source code or object files).

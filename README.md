# mdbsql

**SQL query for Access database on Unix-like systems.**

This is a simple wrapper for [libmdbsql](https://github.com/mdbtools/mdbtools) in Rust.

The implemented SQL subset is limited, please refer to
[mdb-sql](https://man.cx/mdb-sql(1)).

## Requirements

By default, this crate links to the system `libmdbsql` (from
[mdbtools](https://github.com/mdbtools/mdbtools)) found via `pkg-config`.

## Features

- `bundled`: compile the vendored mdbtools source (v1.0.1) and link it
  statically, so mdbtools does not need to be installed. GLib (>= 2.68) and
  `pkg-config` are still required.
- `rusqlite`: load an entire Access database into an in-memory SQLite database.

```toml
[dependencies]
mdbsql = { version = "0.1", features = ["bundled"] }
```

### License note for `bundled`

mdbtools is licensed under LGPL-2.0-or-later. With `bundled`, it is linked
statically into your binary. If you distribute such a binary, you must comply
with the LGPL, which includes letting users relink it against a modified
mdbtools (for example by providing your source code or object files).

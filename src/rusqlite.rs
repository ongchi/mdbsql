use std::path::Path;

use rusqlite::{Connection, Result};

use crate::ffi::Mdb;

pub trait OpenMdb {
    /// Load a mdb file into an in-memory SQLite database.
    fn open_mdb<P: AsRef<Path>>(path: P) -> Result<Connection> {
        let conn = Connection::open_in_memory()?;

        let mdb = Mdb::open(path)?;
        let tables = mdb.table_names();

        mdb.set_default_backend("sqlite")?;
        for table in &tables {
            let schema = dbg!(mdb.schema(table)?);
            conn.execute(&schema, ())?;

            let stmt = dbg!(mdb.export(table)?);
            conn.execute(&stmt, ())?;
        }

        Ok(conn)
    }
}

impl OpenMdb for Connection {}

#[cfg(test)]
mod test {
    use super::*;

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
    fn sqlite() {
        let conn = Connection::open_mdb("resource/test.mdb").unwrap();
        let mut stmt = conn.prepare("SELECT * FROM Table1").unwrap();
        let table = stmt
            .query_map([], |row| {
                Ok(Table1 {
                    id: row.get(0).unwrap(),
                    a: row.get(1).unwrap(),
                    b: row.get(2).unwrap(),
                    c: row.get(3).unwrap(),
                    d: row.get(4).unwrap(),
                    e: row.get(5).unwrap(),
                    f: row.get(6).unwrap(),
                })
            })
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(
            table,
            Table1 {
                id: 1,
                a: "Foo".to_string(),
                b: 1,
                c: 1.0000,
                d: "2000-01-01 00:00:00".to_string(),
                e: 1,
                f: "<div><font face=Calibri>FooBar</font></div>".to_string()
            }
        )
    }
}

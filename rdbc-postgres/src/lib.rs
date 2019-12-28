//! Postgres RDBC Driver
//!
//! This crate implements an RDBC Driver for the `postgres` crate.
//!
//! The RDBC (Rust DataBase Connectivity) API is loosely based on the ODBC and JDBC standards.
//!
//! ```rust,ignore
//! use rdbc::Value;
//! use rdbc_postgres::PostgresDriver;
//! let driver = PostgresDriver::new();
//! let conn = driver.connect("postgres://postgres:password@localhost:5433").unwrap();
//! let mut conn = conn.borrow_mut();
//! let stmt = conn.prepare("SELECT a FROM b WHERE c = ?").unwrap();
//! let mut stmt = stmt.borrow_mut();
//! let rs = stmt.execute_query(&vec![Value::Int32(123)]).unwrap();
//! let mut rs = rs.borrow_mut();
//! while rs.next() {
//!   println!("{:?}", rs.get_string(1));
//! }
//! ```

use postgres::rows::Rows;
use postgres::{Connection, TlsMode};

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer, Word};

use postgres::types::Type;
use rdbc::Column;

pub struct PostgresDriver {}

impl PostgresDriver {
    pub fn new() -> Self {
        PostgresDriver {}
    }
}

impl rdbc::Driver for PostgresDriver {
    fn connect(&self, url: &str) -> rdbc::Result<Box<dyn rdbc::Connection>> {
        let c = postgres::Connection::connect(url, TlsMode::None).map_err(to_rdbc_err)?;
        Ok(Box::new(PConnection::new(c)))
    }
}

struct PConnection {
    conn: Connection,
}

impl PConnection {
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }
}

impl rdbc::Connection for PConnection {
    fn create(&mut self, sql: &str) -> rdbc::Result<Box<dyn rdbc::Statement + '_>> {
        self.prepare(sql)
    }

    fn prepare(&mut self, sql: &str) -> rdbc::Result<Box<dyn rdbc::Statement + '_>> {
        // translate SQL, mapping ? into $1 style bound param placeholder
        let dialect = PostgreSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut i = 0;
        let tokens: Vec<Token> = tokens
            .iter()
            .map(|t| match t {
                Token::Char(c) if *c == '?' => {
                    i += 1;
                    Token::Word(Word {
                        value: format!("${}", i),
                        quote_style: None,
                        keyword: "".to_owned(),
                    })
                }
                _ => t.clone(),
            })
            .collect();
        let sql = tokens
            .iter()
            .map(|t| format!("{}", t))
            .collect::<Vec<String>>()
            .join("");

        Ok(Box::new(PStatement {
            conn: &self.conn,
            sql,
        }))
    }
}

struct PStatement<'a> {
    conn: &'a Connection,
    sql: String,
}

impl<'a> rdbc::Statement for PStatement<'a> {
    fn execute_query(
        &mut self,
        params: &[rdbc::Value],
    ) -> rdbc::Result<Box<dyn rdbc::ResultSet + '_>> {
        let params = to_postgres_value(params);
        let params: Vec<&dyn postgres::types::ToSql> = params.iter().map(|v| v.as_ref()).collect();
        let rows = self
            .conn
            .query(&self.sql, params.as_slice())
            .map_err(to_rdbc_err)?;
        let meta = rows
            .columns()
            .iter()
            .map(|c| rdbc::Column::new(c.name(), to_rdbc_type(c.type_())))
            .collect();

        Ok(Box::new(PResultSet { meta, i: 0, rows }))
    }

    fn execute_update(&mut self, params: &[rdbc::Value]) -> rdbc::Result<u64> {
        let params = to_postgres_value(params);
        let params: Vec<&dyn postgres::types::ToSql> = params.iter().map(|v| v.as_ref()).collect();
        self.conn
            .execute(&self.sql, params.as_slice())
            .map_err(to_rdbc_err)
    }
}

struct PResultSet {
    meta: Vec<Column>,
    i: usize,
    rows: Rows,
}

impl rdbc::ResultSet for PResultSet {
    fn meta_data(&self) -> rdbc::Result<Box<dyn rdbc::ResultSetMetaData>> {
        Ok(Box::new(self.meta.clone()))
    }

    fn next(&mut self) -> bool {
        if self.i < self.rows.len() {
            self.i = self.i + 1;
            true
        } else {
            false
        }
    }

    fn get_i8(&self, i: u64) -> rdbc::Result<Option<i8>> {
        Ok(self.rows.get(self.i - 1).get(i as usize))
    }

    fn get_i16(&self, i: u64) -> rdbc::Result<Option<i16>> {
        Ok(self.rows.get(self.i - 1).get(i as usize))
    }

    fn get_i32(&self, i: u64) -> rdbc::Result<Option<i32>> {
        Ok(self.rows.get(self.i - 1).get(i as usize))
    }

    fn get_i64(&self, i: u64) -> rdbc::Result<Option<i64>> {
        Ok(self.rows.get(self.i - 1).get(i as usize))
    }

    fn get_f32(&self, i: u64) -> rdbc::Result<Option<f32>> {
        Ok(self.rows.get(self.i - 1).get(i as usize))
    }

    fn get_f64(&self, i: u64) -> rdbc::Result<Option<f64>> {
        Ok(self.rows.get(self.i - 1).get(i as usize))
    }

    fn get_string(&self, i: u64) -> rdbc::Result<Option<String>> {
        Ok(self.rows.get(self.i - 1).get(i as usize))
    }

    fn get_bytes(&self, i: u64) -> rdbc::Result<Option<Vec<u8>>> {
        Ok(self.rows.get(self.i - 1).get(i as usize))
    }
}

/// Convert a Postgres error into an RDBC error
fn to_rdbc_err(e: postgres::error::Error) -> rdbc::Error {
    rdbc::Error::General(format!("{:?}", e))
}

fn to_rdbc_type(ty: &Type) -> rdbc::DataType {
    match ty.name() {
        "" => rdbc::DataType::Bool,
        //TODO all types
        _ => rdbc::DataType::Utf8,
    }
}

fn to_postgres_value(values: &[rdbc::Value]) -> Vec<Box<dyn postgres::types::ToSql>> {
    values
        .iter()
        .map(|v| match v {
            rdbc::Value::String(s) => Box::new(s.clone()) as Box<dyn postgres::types::ToSql>,
            rdbc::Value::Int32(n) => Box::new(*n) as Box<dyn postgres::types::ToSql>,
            rdbc::Value::UInt32(n) => Box::new(*n) as Box<dyn postgres::types::ToSql>,
            //TODO all types
        })
        .collect()
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::Arc;

    #[test]
    fn execute_query() -> rdbc::Result<()> {
        execute("DROP TABLE IF EXISTS test", &vec![])?;
        execute("CREATE TABLE test (a INT NOT NULL)", &vec![])?;
        execute(
            "INSERT INTO test (a) VALUES (?)",
            &vec![rdbc::Value::Int32(123)],
        )?;

        let driver: Arc<dyn rdbc::Driver> = Arc::new(PostgresDriver::new());
        let mut conn = driver.connect("postgres://rdbc:secret@127.0.0.1:5433")?;
        let mut stmt = conn.prepare("SELECT a FROM test")?;
        let mut rs = stmt.execute_query(&vec![])?;

        assert!(rs.next());
        assert_eq!(Some(123), rs.get_i32(0)?);
        assert!(!rs.next());

        Ok(())
    }

    fn execute(sql: &str, values: &Vec<rdbc::Value>) -> rdbc::Result<u64> {
        println!("Executing '{}' with {} params", sql, values.len());
        let driver: Arc<dyn rdbc::Driver> = Arc::new(PostgresDriver::new());
        let mut conn = driver.connect("postgres://rdbc:secret@127.0.0.1:5433")?;
        let mut stmt = conn.prepare(sql)?;
        stmt.execute_update(values)
    }
}

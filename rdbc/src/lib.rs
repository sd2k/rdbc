//! The RDBC (Rust DataBase Connectivity) API is loosely based on the ODBC and JDBC standards
//! and provides a database agnostic programming interface for executing queries and fetching
//! results.
//!
//! Reference implementation RDBC Drivers exist for Postgres, MySQL and SQLite.
//!
//! The following example demonstrates how RDBC can be used to run a trivial query against Postgres.
//!
//! ```rust,ignore
//! use rdbc::*;
//! use rdbc_postgres::PostgresDriver;
//!
//! let driver = PostgresDriver::new();
//! let mut conn = driver.connect("postgres://postgres:password@localhost:5433").unwrap();
//! let mut stmt = conn.prepare("SELECT a FROM b WHERE c = ?").unwrap();
//! let mut rs = stmt.execute_query(&[Value::Int32(123)]).unwrap();
//! while rs.next() {
//!   println!("{:?}", rs.get_string(1));
//! }
//! ```

use std::pin::Pin;

use async_trait::async_trait;
use futures::stream::StreamExt;
use tokio::stream::{iter, Stream};

/// RDBC Error
#[derive(Debug)]
pub enum Error {
    General(String),
}

#[derive(Debug, Clone)]
pub enum Value {
    Int32(i32),
    UInt32(u32),
    String(String),
    //TODO add other types
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::Int32(n) => format!("{}", n),
            Value::UInt32(n) => format!("{}", n),
            Value::String(s) => format!("'{}'", s),
        }
    }
}

/// RDBC Result type
pub type Result<T> = std::result::Result<T, Error>;

/// Represents database driver that can be shared between threads, and can therefore implement
/// a connection pool
pub trait Driver: Sync + Send {
    /// The type of connection created by this driver.
    type Connection: Connection;

    /// Create a connection to the database. Note that connections are intended to be used
    /// in a single thread since most database connections are not thread-safe
    fn connect(url: &str) -> Result<Self::Connection>;
}

/// Represents a connection to a database
pub trait Connection {
    /// The type of statement produced by this connection.
    type Statement: Statement;

    /// Create a statement for execution
    fn create(&mut self, sql: &str) -> Result<Self::Statement>;

    /// Create a prepared statement for execution
    fn prepare(&mut self, sql: &str) -> Result<Self::Statement>;
}

/// Represents an executable statement
pub trait Statement {
    /// The type of ResultSet returned by this statement.
    type ResultSet: ResultSet;

    /// Execute a query that is expected to return a result set, such as a `SELECT` statement
    fn execute_query(&mut self, params: &[Value]) -> Result<Self::ResultSet>;

    /// Execute a query that is expected to update some rows.
    fn execute_update(&mut self, params: &[Value]) -> Result<u64>;
}

/// Result set from executing a query against a statement
#[async_trait]
pub trait ResultSet {
    /// The type of metadata associated with this result set.
    type MetaData: MetaData;
    /// The type of row included in this result set.
    type Row: Row;

    /// get meta data about this result set
    fn meta_data(&self) -> Result<Self::MetaData>;

    /// Get a stream where each item is a batch of rows.
    async fn batches(&mut self) -> Result<Pin<Box<dyn Stream<Item = Vec<Self::Row>>>>>;

    /// Get a stream of rows.
    ///
    /// Note that the rows are actually returned from the database in batches;
    /// this just flattens the batches to provide a (possibly) simpler API.
    async fn rows<'a>(&'a mut self) -> Result<Box<dyn Stream<Item = Self::Row> + 'a>> {
        Ok(Box::new(self.batches().await?.map(iter).flatten()))
    }
}

pub trait Row {
    fn get_i8(&self, i: u64) -> Result<Option<i8>>;
    fn get_i16(&self, i: u64) -> Result<Option<i16>>;
    fn get_i32(&self, i: u64) -> Result<Option<i32>>;
    fn get_i64(&self, i: u64) -> Result<Option<i64>>;
    fn get_f32(&self, i: u64) -> Result<Option<f32>>;
    fn get_f64(&self, i: u64) -> Result<Option<f64>>;
    fn get_string(&self, i: u64) -> Result<Option<String>>;
    fn get_bytes(&self, i: u64) -> Result<Option<Vec<u8>>>;
}

/// Meta data for result set
pub trait MetaData {
    fn num_columns(&self) -> u64;
    fn column_name(&self, i: u64) -> String;
    fn column_type(&self, i: u64) -> DataType;
}

/// RDBC Data Types
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DataType {
    Bool,
    Byte,
    Char,
    Short,
    Integer,
    Float,
    Double,
    Decimal,
    Date,
    Time,
    Datetime,
    Utf8,
    Binary,
}

#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    data_type: DataType,
}

impl Column {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Column {
            name: name.to_owned(),
            data_type,
        }
    }
}

impl MetaData for Vec<Column> {
    fn num_columns(&self) -> u64 {
        self.len() as u64
    }

    fn column_name(&self, i: u64) -> String {
        self[i as usize].name.clone()
    }

    fn column_type(&self, i: u64) -> DataType {
        self[i as usize].data_type
    }
}

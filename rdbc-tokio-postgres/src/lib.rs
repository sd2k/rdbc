use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use bytes::BytesMut;
use sqlparser::{
    dialect::PostgreSqlDialect,
    tokenizer::{Token, Tokenizer, Word},
};
use tokio::stream::Stream;
use tokio_postgres::{
    types::{to_sql_checked, IsNull, ToSql, Type},
    Client, NoTls, Row, Statement,
};

#[derive(Debug)]
pub enum Error {
    TokioPostgres(tokio_postgres::Error),
}

impl From<tokio_postgres::Error> for Error {
    fn from(other: tokio_postgres::Error) -> Self {
        Self::TokioPostgres(other)
    }
}

pub struct TokioPostgresDriver {
    // TODO store a connection pool here?
    url: String,
}

#[async_trait]
impl rdbc::Driver for TokioPostgresDriver {
    type Connection = TokioPostgresConnection;
    type Error = Error;

    async fn new(url: String) -> Result<Self, Self::Error> {
        Ok(Self { url })
    }

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let (client, conn) = tokio_postgres::connect(&self.url, NoTls).await?;
        tokio::spawn(conn);
        Ok(TokioPostgresConnection {
            inner: Arc::new(client),
        })
    }
}

pub struct TokioPostgresConnection {
    inner: Arc<Client>,
}

#[async_trait]
impl rdbc::Connection for TokioPostgresConnection {
    type Statement = TokioPostgresStatement;
    type Error = Error;

    async fn create(&mut self, sql: &str) -> Result<Self::Statement, Self::Error> {
        let sql = {
            let dialect = PostgreSqlDialect {};
            let mut tokenizer = Tokenizer::new(&dialect, sql);
            let tokens = tokenizer.tokenize().unwrap();
            let mut i = 0_usize;
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
            tokens
                .iter()
                .map(|t| format!("{}", t))
                .collect::<Vec<String>>()
                .join("")
        };
        let statement = self.inner.prepare(&sql).await?;
        Ok(TokioPostgresStatement {
            client: Arc::clone(&self.inner),
            statement,
        })
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement, Self::Error> {
        self.create(sql).await
    }
}

pub struct TokioPostgresStatement {
    client: Arc<Client>,
    statement: Statement,
}

fn to_rdbc_type(ty: &Type) -> Option<rdbc::DataType> {
    match ty {
        &Type::BOOL => Some(rdbc::DataType::Bool),
        &Type::CHAR => Some(rdbc::DataType::Char),
        //TODO all types
        _ => Some(rdbc::DataType::Utf8),
    }
}

#[derive(Debug)]
pub struct PostgresType<'a>(&'a rdbc::Value);

impl ToSql for PostgresType<'_> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + 'static + Sync + Send>> {
        match self.0 {
            rdbc::Value::Int32(i) => i.to_sql(ty, out),
            rdbc::Value::UInt32(i) => i.to_sql(ty, out),
            rdbc::Value::String(i) => i.to_sql(ty, out),
            // TODO add other types and convert to macro
        }
    }
    to_sql_checked!();
    fn accepts(ty: &Type) -> bool {
        to_rdbc_type(ty).is_some()
    }
}

#[async_trait]
impl rdbc::Statement for TokioPostgresStatement {
    type ResultSet = TokioPostgresResultSet;
    type Error = Error;

    async fn execute_query(
        &mut self,
        params: &[rdbc::Value],
    ) -> Result<Self::ResultSet, Self::Error> {
        let meta = self
            .statement
            .columns()
            .iter()
            .map(|c| rdbc::Column::new(c.name(), to_rdbc_type(c.type_()).unwrap()))
            .collect();
        let pg_params: Vec<_> = params.iter().map(PostgresType).collect();
        let params: Vec<&(dyn ToSql + Sync)> =
            pg_params.iter().map(|x| x as &(dyn ToSql + Sync)).collect();
        let rows = self
            .client
            .query(&self.statement, params.as_slice())
            .await?
            .into_iter()
            .map(|row| TokioPostgresRow { inner: row })
            .collect();
        Ok(TokioPostgresResultSet { rows, meta })
    }

    async fn execute_update(&mut self, params: &[rdbc::Value]) -> Result<u64, Self::Error> {
        let pg_params: Vec<_> = params.iter().map(PostgresType).collect();
        let params: Vec<&(dyn ToSql + Sync)> =
            pg_params.iter().map(|x| x as &(dyn ToSql + Sync)).collect();
        Ok(self
            .client
            .execute(&self.statement, params.as_slice())
            .await?)
    }
}

pub struct TokioPostgresResultSet {
    meta: Vec<rdbc::Column>,
    rows: Vec<TokioPostgresRow>,
}

#[async_trait]
impl rdbc::ResultSet for TokioPostgresResultSet {
    type MetaData = Vec<rdbc::Column>;
    type Row = TokioPostgresRow;
    type Error = Error;

    fn meta_data(&self) -> Result<&Self::MetaData, Self::Error> {
        Ok(&self.meta)
    }

    async fn batches(
        &mut self,
    ) -> Result<Pin<Box<dyn Stream<Item = Vec<Self::Row>>>>, Self::Error> {
        let rows = std::mem::take(&mut self.rows);
        Ok(Box::pin(tokio::stream::once(rows)))
    }
}

pub struct TokioPostgresRow {
    inner: Row,
}

macro_rules! impl_resultset_fns {
    ($($fn: ident -> $ty: ty),*) => {
        $(
            fn $fn(&self, i: u64) -> Result<Option<$ty>, Self::Error> {
                Some(self.inner.try_get((i - 1) as usize)).transpose().map_err(Into::into)
            }
        )*
    }
}

impl rdbc::Row for TokioPostgresRow {
    type Error = Error;
    impl_resultset_fns! {
        get_i8 -> i8,
        get_i16 -> i16,
        get_i32 -> i32,
        get_i64 -> i64,
        get_f32 -> f32,
        get_f64 -> f64,
        get_string -> String,
        get_bytes -> Vec<u8>
    }
}

#[cfg(test)]
mod tests {

    use rdbc::{Connection, Driver, ResultSet, Row, Statement};
    use tokio::stream::StreamExt;

    use super::*;

    #[tokio::test]
    async fn execute_queries() -> Result<(), Error> {
        let driver =
            TokioPostgresDriver::new("postgres://rdbc:secret@127.0.0.1:5433".to_string()).await?;
        let mut conn = driver.connect().await?;
        conn.prepare("DROP TABLE IF EXISTS test")
            .await?
            .execute_update(&[])
            .await?;
        conn.prepare("CREATE TABLE test (a INT NOT NULL)")
            .await?
            .execute_update(&[])
            .await?;
        conn.prepare("INSERT INTO test (a) VALUES (?)")
            .await?
            .execute_update(&[rdbc::Value::Int32(123)])
            .await?;
        assert_eq!(
            conn.prepare("SELECT a FROM test")
                .await?
                .execute_query(&[])
                .await?
                .batches()
                .await?
                .next()
                .await
                .unwrap()
                .get(0)
                .unwrap()
                .get_i32(1)?
                .unwrap(),
            123,
        );
        assert_eq!(
            conn.prepare("SELECT a FROM test")
                .await?
                .execute_query(&[])
                .await?
                .rows()
                .await?
                .next()
                .await
                .unwrap()
                .get_i32(1)?
                .unwrap(),
            123
        );
        Ok(())
    }
}

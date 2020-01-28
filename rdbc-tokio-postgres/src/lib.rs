use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use sqlparser::{
    dialect::PostgreSqlDialect,
    tokenizer::{Token, Tokenizer, Word},
};
use tokio::stream::Stream;
use tokio_postgres::{types::Type, Client, NoTls, Row, Statement};

#[derive(Debug)]
pub enum Error {
    TokioPostgres(tokio_postgres::Error),
}

impl From<tokio_postgres::Error> for Error {
    fn from(other: tokio_postgres::Error) -> Self {
        Self::TokioPostgres(other)
    }
}

pub struct TokioPostgresDriver;

#[async_trait]
impl rdbc::Driver for TokioPostgresDriver {
    type Connection = TokioPostgresConnection;
    type Error = Error;

    async fn connect(url: &str) -> Result<Self::Connection, Self::Error> {
        let (client, conn) = tokio_postgres::connect(url, NoTls).await?;
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

fn to_rdbc_type(ty: &Type) -> rdbc::DataType {
    match ty {
        &Type::BOOL => rdbc::DataType::Bool,
        &Type::CHAR => rdbc::DataType::Char,
        //TODO all types
        _ => rdbc::DataType::Utf8,
    }
}

fn to_postgres_params(values: &[rdbc::Value]) -> Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> {
    values
        .iter()
        .map(|v| match v {
            rdbc::Value::String(s) => {
                Box::new(s.clone()) as Box<dyn tokio_postgres::types::ToSql + Sync>
            }
            rdbc::Value::Int32(n) => Box::new(*n) as Box<dyn tokio_postgres::types::ToSql + Sync>,
            rdbc::Value::UInt32(n) => Box::new(*n) as Box<dyn tokio_postgres::types::ToSql + Sync>, //TODO all types
        })
        .collect()
}

#[async_trait]
impl rdbc::Statement for TokioPostgresStatement {
    type ResultSet = TokioPostgresResultSet;
    type Error = Error;
    async fn execute_query(
        &mut self,
        params: &[rdbc::Value],
    ) -> Result<Self::ResultSet, Self::Error> {
        let params = to_postgres_params(params);
        let params: Vec<_> = params.into_iter().map(|p| p.as_ref()).collect();
        let rows = self
            .client
            .query(&self.statement, params.as_slice())
            .await?
            .into_iter()
            .map(|row| TokioPostgresRow { inner: row })
            .collect();
        let meta = self
            .statement
            .columns()
            .iter()
            .map(|c| rdbc::Column::new(c.name(), to_rdbc_type(c.type_())))
            .collect();
        Ok(TokioPostgresResultSet { rows, meta })
    }
    async fn execute_update(&mut self, params: &[rdbc::Value]) -> Result<u64, Self::Error> {
        todo!()
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

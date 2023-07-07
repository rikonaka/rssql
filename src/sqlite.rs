use anyhow;
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::{Column, Row, TypeInfo};
use sqlx::sqlite::SqliteRow;
use std::collections::HashMap;
use std::fmt;

use crate::SQLDataTypes;
use crate::SQLRets;
use crate::BINARY;
use crate::UNKNOWN_DATA_TYPE;

#[derive(Debug, Clone)]
pub enum SQLiteDataTypes {
    /// From https://docs.rs/sqlx-sqlite/0.7.0/sqlx_sqlite/types/index.html
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    String(String),
    Binary(Vec<u8>),
    NaiveDateTime(NaiveDateTime),
    DateTime(DateTime<chrono::Utc>),
    NaiveDate(NaiveDate),
    NaiveTime(NaiveTime),
}

impl fmt::Display for SQLiteDataTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SQLiteDataTypes::Bool(v) => write!(f, "{}", v),
            SQLiteDataTypes::I32(v) => write!(f, "{}", v),
            SQLiteDataTypes::I64(v) => write!(f, "{}", v),
            SQLiteDataTypes::F64(v) => write!(f, "{}", v),
            SQLiteDataTypes::String(v) => write!(f, "{}", v),
            SQLiteDataTypes::Binary(_) => write!(f, "{}", BINARY),
            SQLiteDataTypes::NaiveDateTime(v) => write!(f, "{}", v),
            SQLiteDataTypes::DateTime(v) => write!(f, "{}", v),
            SQLiteDataTypes::NaiveDate(v) => write!(f, "{}", v),
            SQLiteDataTypes::NaiveTime(v) => write!(f, "{}", v),
        }
    }
}

pub async fn rows_process(rows: Vec<SqliteRow>) -> anyhow::Result<SQLRets> {
    let mut sql_rets = SQLRets::new();

    if rows.len() > 0 {
        // push all column
        let mysql_row = &rows[0];
        let mysql_row_len = mysql_row.len();
        for i in 0..mysql_row_len {
            let col = mysql_row.column(i);
            let col_name = col.name().to_string();
            sql_rets.push_column_name(&col_name);
        }
    }

    for mysql_row in &rows {
        let mut sql_row: HashMap<String, SQLDataTypes> = HashMap::new();
        let sqlite_row_len = mysql_row.len();

        for i in 0..sqlite_row_len {
            let col = mysql_row.column(i);
            let col_name = col.name().to_string();
            let type_info = col.type_info();
            let sqlite_value = match type_info.name() {
                "BOOLEAN" => {
                    let value: bool = mysql_row.get(i);
                    SQLiteDataTypes::Bool(value)
                }
                "INTEGER" => {
                    let value: i32 = mysql_row.get(i);
                    SQLiteDataTypes::I32(value)
                }
                "BIGINT" | "INT8" => {
                    let value: i64 = mysql_row.get(i);
                    SQLiteDataTypes::I64(value)
                }
                "REAL" => {
                    let value: f64 = mysql_row.get(i);
                    SQLiteDataTypes::F64(value)
                }
                "TEXT" => {
                    let value: String = mysql_row.get(i);
                    SQLiteDataTypes::String(value)
                }
                "BLOB" => {
                    let value: Vec<u8> = mysql_row.get(i);
                    SQLiteDataTypes::Binary(value)
                }
                "DATETIME" => {
                    let value: NaiveDateTime = mysql_row.get(i);
                    SQLiteDataTypes::NaiveDateTime(value)
                }
                "DATE" => {
                    let value: NaiveDate = mysql_row.get(i);
                    SQLiteDataTypes::NaiveDate(value)
                }
                "TIME" => {
                    let value: NaiveTime = mysql_row.get(i);
                    SQLiteDataTypes::NaiveTime(value)
                }
                _ => SQLiteDataTypes::String(UNKNOWN_DATA_TYPE.into()),
            };
            let sql_value = SQLDataTypes::SQLiteDataTypes(sqlite_value);
            sql_row.insert(col_name, sql_value);
        }
        sql_rets.push_rets(sql_row);
    }
    Ok(sql_rets)
}

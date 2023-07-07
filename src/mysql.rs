use anyhow;
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::types::{BigDecimal, JsonValue, Uuid};
use sqlx::{Column, Row, TypeInfo};
use sqlx::mysql::MySqlRow;
use std::collections::HashMap;
use std::fmt;

use crate::SQLDataTypes;
use crate::SQLRets;
use crate::BINARY;
use crate::UNKNOWN_DATA_TYPE;

#[derive(Debug, Clone)]
pub enum MySQLDataTypes {
    /// From https://docs.rs/sqlx-mysql/0.7.0/sqlx_mysql/types/index.html
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Binary(Vec<u8>),
    DateTime(DateTime<chrono::Utc>),
    NaiveDateTime(NaiveDateTime),
    NaiveDate(NaiveDate),
    NaiveTime(NaiveTime),
    BigDecimal(BigDecimal),
    Uuid(Uuid),
    JsonValue(JsonValue),
}

impl fmt::Display for MySQLDataTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MySQLDataTypes::Bool(v) => write!(f, "{}", v),
            MySQLDataTypes::I8(v) => write!(f, "{}", v),
            MySQLDataTypes::I16(v) => write!(f, "{}", v),
            MySQLDataTypes::I32(v) => write!(f, "{}", v),
            MySQLDataTypes::I64(v) => write!(f, "{}", v),
            MySQLDataTypes::U8(v) => write!(f, "{}", v),
            MySQLDataTypes::U16(v) => write!(f, "{}", v),
            MySQLDataTypes::U32(v) => write!(f, "{}", v),
            MySQLDataTypes::U64(v) => write!(f, "{}", v),
            MySQLDataTypes::F32(v) => write!(f, "{}", v),
            MySQLDataTypes::F64(v) => write!(f, "{}", v),
            MySQLDataTypes::String(v) => write!(f, "{}", v),
            MySQLDataTypes::Binary(_) => write!(f, "{}", BINARY),
            MySQLDataTypes::DateTime(v) => write!(f, "{}", v),
            MySQLDataTypes::NaiveDateTime(v) => write!(f, "{}", v),
            MySQLDataTypes::NaiveDate(v) => write!(f, "{}", v),
            MySQLDataTypes::NaiveTime(v) => write!(f, "{}", v),
            MySQLDataTypes::BigDecimal(v) => write!(f, "{}", v),
            MySQLDataTypes::Uuid(v) => write!(f, "{}", v),
            MySQLDataTypes::JsonValue(v) => write!(f, "{}", v),
        }
    }
}

pub async fn rows_process(rows: Vec<MySqlRow>) -> anyhow::Result<SQLRets> {
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
        let mysql_row_len = mysql_row.len();

        for i in 0..mysql_row_len {
            let col = mysql_row.column(i);
            let col_name = col.name().to_string();
            let type_info = col.type_info();
            let mysql_value = match type_info.name() {
                "BOOLEAN" | "TINYINT(1)" => {
                    let value: bool = mysql_row.get(i);
                    MySQLDataTypes::Bool(value)
                }
                "TINYINT" => {
                    let value: i8 = mysql_row.get(i);
                    MySQLDataTypes::I8(value)
                }
                "SMALLINT" => {
                    let value: i16 = mysql_row.get(i);
                    MySQLDataTypes::I16(value)
                }
                "INT" => {
                    let value: i32 = mysql_row.get(i);
                    MySQLDataTypes::I32(value)
                }
                "BIGINT" => {
                    let value: i64 = mysql_row.get(i);
                    MySQLDataTypes::I64(value)
                }
                "TINYINT UNSIGNED" => {
                    let value: u8 = mysql_row.get(i);
                    MySQLDataTypes::U8(value)
                }
                "SMALLINT UNSIGNED" => {
                    let value: u16 = mysql_row.get(i);
                    MySQLDataTypes::U16(value)
                }
                "INT UNSIGNED" => {
                    let value: u32 = mysql_row.get(i);
                    MySQLDataTypes::U32(value)
                }
                "BIGINT UNSIGNED" => {
                    let value: u64 = mysql_row.get(i);
                    MySQLDataTypes::U64(value)
                }
                "FLOAT" => {
                    let value: f32 = mysql_row.get(i);
                    MySQLDataTypes::F32(value)
                }
                "DOUBLE" => {
                    let value: f64 = mysql_row.get(i);
                    MySQLDataTypes::F64(value)
                }
                "VARCHAR" | "CHAR" | "TEXT" => {
                    let value: String = mysql_row.get(i);
                    MySQLDataTypes::String(value)
                }
                "VARBINARY" | "BINARY" | "BLOB" => {
                    let value: Vec<u8> = mysql_row.get(i);
                    MySQLDataTypes::Binary(value)
                }
                "TIMESTAMP" => {
                    let value: DateTime<chrono::Utc> = mysql_row.get(i);
                    MySQLDataTypes::DateTime(value)
                }
                "DATETIME" => {
                    let value: NaiveDateTime = mysql_row.get(i);
                    MySQLDataTypes::NaiveDateTime(value)
                }
                "DATE" => {
                    let value: NaiveDate = mysql_row.get(i);
                    MySQLDataTypes::NaiveDate(value)
                }
                "TIME" => {
                    let value: NaiveTime = mysql_row.get(i);
                    MySQLDataTypes::NaiveTime(value)
                }
                "DECIMAL" => {
                    let value: BigDecimal = mysql_row.get(i);
                    MySQLDataTypes::BigDecimal(value)
                }
                "BYTE(16)" => {
                    let value: Uuid = mysql_row.get(i);
                    MySQLDataTypes::Uuid(value)
                }
                "JSON" => {
                    let value: JsonValue = mysql_row.get(i);
                    MySQLDataTypes::JsonValue(value)
                }
                _ => MySQLDataTypes::String(UNKNOWN_DATA_TYPE.into()),
            };
            let sql_value = SQLDataTypes::MySQLDataTypes(mysql_value);
            sql_row.insert(col_name, sql_value);
        }
        sql_rets.push_rets(sql_row);
    }
    Ok(sql_rets)
}

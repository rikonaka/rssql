use anyhow;
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::types::{BigDecimal, JsonValue, Uuid};
use sqlx::{Column, MySqlConnection, Row, TypeInfo};
use std::collections::HashMap;
use std::fmt;

use crate::SqlDataType;
use crate::SqlRets;
use crate::BINARY_DATA_TYPE;
use crate::JSON_DATA_MAX_SHOW;
use crate::UNKNOWN_DATA_TYPE;

#[derive(Debug)]
pub enum MySQLDataType {
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

impl fmt::Display for MySQLDataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MySQLDataType::Bool(v) => write!(f, "{}", v),
            MySQLDataType::I8(v) => write!(f, "{}", v),
            MySQLDataType::I16(v) => write!(f, "{}", v),
            MySQLDataType::I32(v) => write!(f, "{}", v),
            MySQLDataType::I64(v) => write!(f, "{}", v),
            MySQLDataType::U8(v) => write!(f, "{}", v),
            MySQLDataType::U16(v) => write!(f, "{}", v),
            MySQLDataType::U32(v) => write!(f, "{}", v),
            MySQLDataType::U64(v) => write!(f, "{}", v),
            MySQLDataType::F32(v) => write!(f, "{}", v),
            MySQLDataType::F64(v) => write!(f, "{}", v),
            MySQLDataType::String(v) => write!(f, "{}", v),
            MySQLDataType::Binary(_) => write!(f, "{}", BINARY_DATA_TYPE),
            MySQLDataType::DateTime(v) => write!(f, "{}", v),
            MySQLDataType::NaiveDateTime(v) => write!(f, "{}", v),
            MySQLDataType::NaiveDate(v) => write!(f, "{}", v),
            MySQLDataType::NaiveTime(v) => write!(f, "{}", v),
            MySQLDataType::BigDecimal(v) => write!(f, "{}", v),
            MySQLDataType::Uuid(v) => write!(f, "{}", v),
            MySQLDataType::JsonValue(v) => {
                let json_str = format!("{}", v);
                // let json_str = &json_str[0..JSON_DATA_MAX_SHOW];
                write!(f, "{}", &json_str[0..JSON_DATA_MAX_SHOW])
            }
        }
    }
}

pub async fn raw_mysql_query(conn: &mut MySqlConnection, sql: &str) -> anyhow::Result<SqlRets> {
    let rows = sqlx::query(sql).fetch_all(conn).await?;
    let mut sql_rets = SqlRets::new();

    for mysql_row in &rows {
        let mut sql_row: HashMap<String, SqlDataType> = HashMap::new();
        let mysql_row_len = mysql_row.len();
        for i in 0..mysql_row_len {
            let col = mysql_row.column(i);
            let col_name = col.name().to_string();
            sql_rets.push_column_name(&col_name);
            let type_info = col.type_info();
            let mysql_value = match type_info.name() {
                "BOOLEAN" | "TINYINT(1)" => {
                    let value: bool = mysql_row.get(i);
                    MySQLDataType::Bool(value)
                }
                "TINYINT" => {
                    let value: i8 = mysql_row.get(i);
                    MySQLDataType::I8(value)
                }
                "SMALLINT" => {
                    let value: i16 = mysql_row.get(i);
                    MySQLDataType::I16(value)
                }
                "INT" => {
                    let value: i32 = mysql_row.get(i);
                    MySQLDataType::I32(value)
                }
                "BIGINT" => {
                    let value: i64 = mysql_row.get(i);
                    MySQLDataType::I64(value)
                }
                "TINYINT UNSIGNED" => {
                    let value: u8 = mysql_row.get(i);
                    MySQLDataType::U8(value)
                }
                "SMALLINT UNSIGNED" => {
                    let value: u16 = mysql_row.get(i);
                    MySQLDataType::U16(value)
                }
                "INT UNSIGNED" => {
                    let value: u32 = mysql_row.get(i);
                    MySQLDataType::U32(value)
                }
                "BIGINT UNSIGNED" => {
                    let value: u64 = mysql_row.get(i);
                    MySQLDataType::U64(value)
                }
                "FLOAT" => {
                    let value: f32 = mysql_row.get(i);
                    MySQLDataType::F32(value)
                }
                "DOUBLE" => {
                    let value: f64 = mysql_row.get(i);
                    MySQLDataType::F64(value)
                }
                "VARCHAR" | "CHAR" | "TEXT" => {
                    let value: String = mysql_row.get(i);
                    MySQLDataType::String(value)
                }
                "VARBINARY" | "BINARY" | "BLOB" => {
                    let value: Vec<u8> = mysql_row.get(i);
                    MySQLDataType::Binary(value)
                }
                "TIMESTAMP" => {
                    let value: DateTime<chrono::Utc> = mysql_row.get(i);
                    MySQLDataType::DateTime(value)
                }
                "DATETIME" => {
                    let value: NaiveDateTime = mysql_row.get(i);
                    MySQLDataType::NaiveDateTime(value)
                }
                "DATE" => {
                    let value: NaiveDate = mysql_row.get(i);
                    MySQLDataType::NaiveDate(value)
                }
                "TIME" => {
                    let value: NaiveTime = mysql_row.get(i);
                    MySQLDataType::NaiveTime(value)
                }
                "DECIMAL" => {
                    let value: BigDecimal = mysql_row.get(i);
                    MySQLDataType::BigDecimal(value)
                }
                "BYTE(16)" => {
                    let value: Uuid = mysql_row.get(i);
                    MySQLDataType::Uuid(value)
                }
                "JSON" => {
                    let value: JsonValue = mysql_row.get(i);
                    MySQLDataType::JsonValue(value)
                }
                _ => MySQLDataType::String(UNKNOWN_DATA_TYPE.into()),
            };
            let sql_value = SqlDataType::MySQLDataType(mysql_value);
            sql_row.insert(col_name, sql_value);
        }
        sql_rets.push_rets(sql_row);
    }
    Ok(sql_rets)
}

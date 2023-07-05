use anyhow;
use sqlx::postgres::types::{PgInterval, PgMoney, PgRange, PgTimeTz, PgLTree, PgLQuery};
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::types::ipnetwork::IpNetwork;
use sqlx::types::mac_address::MacAddress;
use sqlx::types::{BigDecimal, BitVec, JsonValue, Uuid};
use sqlx::{Column, PgConnection, Row, TypeInfo};
use std::collections::HashMap;
use std::fmt;

use crate::{SqlDataType, JSON_DATA_MAX_SHOW};
use crate::SqlRets;
use crate::UNKNOWN_DATA_TYPE;
use crate::BINARY_DATA_TYPE;


static PGINTERVAL: &str = "[PGINTERVAL]";
static PGMONEY: &str = "[PGMONEY]";
static PGTIMETZ: &str = "[PGTIMETZ]";

#[derive(Debug, Clone)]
pub enum PostgreSQLDataType {
    /// From https://docs.rs/sqlx-postgres/0.7.0/sqlx_postgres/types/index.html
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Binary(Vec<u8>),
    Void(()),
    // not impl Display
    PgInterval(PgInterval),
    PgRangeBigDecimal(PgRange<BigDecimal>),
    PgRangeDateTime(PgRange<DateTime<chrono::Utc>>),
    PgRangeNaiveDate(PgRange<NaiveDate>),
    PgRangeNaiveDateTime(PgRange<NaiveDateTime>),
    PgRangeI32(PgRange<i32>),
    PgRangeI64(PgRange<i64>),
    PgMoney(PgMoney),
    PgLTree(PgLTree),
    PgLQuery(PgLQuery),
    BigDecimal(BigDecimal),
    DateTime(DateTime<chrono::Utc>),
    NaiveDateTime(NaiveDateTime),
    NaiveDate(NaiveDate),
    NaiveTime(NaiveTime),
    PgTimeTz(PgTimeTz),
    Uuid(Uuid),
    IpNetwork(IpNetwork),
    MacAddress(MacAddress),
    BitVec(BitVec),
    JsonValue(JsonValue),
}

impl fmt::Display for PostgreSQLDataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PostgreSQLDataType::Bool(v) => write!(f, "{}", v),
            PostgreSQLDataType::I8(v) => write!(f, "{}", v),
            PostgreSQLDataType::I16(v) => write!(f, "{}", v),
            PostgreSQLDataType::I32(v) => write!(f, "{}", v),
            PostgreSQLDataType::I64(v) => write!(f, "{}", v),
            PostgreSQLDataType::U8(v) => write!(f, "{}", v),
            PostgreSQLDataType::U16(v) => write!(f, "{}", v),
            PostgreSQLDataType::U64(v) => write!(f, "{}", v),
            PostgreSQLDataType::F32(v) => write!(f, "{}", v),
            PostgreSQLDataType::F64(v) => write!(f, "{}", v),
            PostgreSQLDataType::String(v) => write!(f, "{}", v),
            PostgreSQLDataType::Binary(_) => write!(f, "{}", BINARY_DATA_TYPE),
            PostgreSQLDataType::Void(_) => write!(f, "()"),
            PostgreSQLDataType::PgInterval(_) => write!(f, "{}", PGINTERVAL),
            PostgreSQLDataType::PgRangeBigDecimal(v) => write!(f, "{}", v),
            PostgreSQLDataType::PgRangeDateTime(v) => write!(f, "{}", v),
            PostgreSQLDataType::PgRangeNaiveDate(v) => write!(f, "{}", v),
            PostgreSQLDataType::PgRangeNaiveDateTime(v) => write!(f, "{}", v),
            PostgreSQLDataType::PgRangeI32(v) => write!(f, "{}", v),
            PostgreSQLDataType::PgRangeI64(v) => write!(f, "{}", v),
            PostgreSQLDataType::PgMoney(_) => write!(f, "{}", PGMONEY),
            PostgreSQLDataType::PgLTree(v) => write!(f, "{}", v),
            PostgreSQLDataType::PgLQuery(v) => write!(f, "{}", v),
            PostgreSQLDataType::BigDecimal(v) => write!(f, "{}", v),
            PostgreSQLDataType::DateTime(v) => write!(f, "{}", v),
            PostgreSQLDataType::NaiveDateTime(v) => write!(f, "{}", v),
            PostgreSQLDataType::NaiveDate(v) => write!(f, "{}", v),
            PostgreSQLDataType::NaiveTime(v) => write!(f, "{}", v),
            PostgreSQLDataType::PgTimeTz(_) => write!(f, "{}", PGTIMETZ),
            PostgreSQLDataType::Uuid(v) => write!(f, "{}", v),
            PostgreSQLDataType::IpNetwork(v) => write!(f, "{}", v),
            PostgreSQLDataType::MacAddress(v) => write!(f, "{}", v),
            PostgreSQLDataType::BitVec(_) => write!(f, "{}", BINARY_DATA_TYPE),
            PostgreSQLDataType::JsonValue(v) => {
                let json_value = format!("{}", v);
                write!(f, "{}", &json_value[0..JSON_DATA_MAX_SHOW])
            },
        }
    }
}

pub async fn raw_psql_query(conn: &mut PgConnection, sql: &str) -> anyhow::Result<SqlRets> {
    let rows = sqlx::query(sql).fetch_all(conn).await?;
    let mut sql_rets = SqlRets::new();

    if rows.len() > 0 {
        // push all column
        let pg_row = &rows[0];
        let mysql_row_len = pg_row.len();
        for i in 0..mysql_row_len {
            let col = pg_row.column(i);
            let col_name = col.name().to_string();
            sql_rets.push_column_name(&col_name);
        }
    }

    for pg_row in &rows {
        let mut sql_row: HashMap<String, SqlDataType> = HashMap::new();
        let pg_row_len = pg_row.len();
        for i in 0..pg_row_len {
            let col = pg_row.column(i);
            let col_name = col.name().to_string();
            let type_info = col.type_info();
            let postgresql_value = match type_info.name() {
                "BOOL" => {
                    let value: bool = pg_row.get(i);
                    PostgreSQLDataType::Bool(value)
                }
                "CHAR" => {
                    let value: i8 = pg_row.get(i);
                    PostgreSQLDataType::I8(value)
                }
                "SMALLINT" | "SMALLSERIAL" | "INT2" => {
                    let value: i16 = pg_row.get(i);
                    PostgreSQLDataType::I16(value)
                }
                "INT" | "SERIAL" | "INT4" => {
                    let value: i32 = pg_row.get(i);
                    PostgreSQLDataType::I32(value)
                }
                "BIGINT" | "BIGSERIAL" | "INT8" => {
                    let value: i64 = pg_row.get(i);
                    PostgreSQLDataType::I64(value)
                }
                "REAL" | "FLOAT4" => {
                    let value: f32 = pg_row.get(i);
                    PostgreSQLDataType::F32(value)
                }
                "DOUBLE PRECISION" | "FLOAT8" => {
                    let value: f64 = pg_row.get(i);
                    PostgreSQLDataType::F64(value)
                }
                "VARCHAR" | "CHAR(N)" | "TEXT" | "NAME" => {
                    let value: String = pg_row.get(i);
                    PostgreSQLDataType::String(value)
                }
                "BYTEA" => {
                    let value: Vec<u8> = pg_row.get(i);
                    PostgreSQLDataType::Binary(value)
                }
                "VOID" => {
                    let value = ();
                    PostgreSQLDataType::Void(value)
                }
                "INTERVAL" => {
                    let value: PgInterval = pg_row.get(i);
                    PostgreSQLDataType::PgInterval(value)
                }
                "NUMRANGE" => {
                    let value: PgRange<BigDecimal> = pg_row.get(i);
                    PostgreSQLDataType::PgRangeBigDecimal(value)
                }
                "DATERANGE" => {
                    let value: PgRange<NaiveDate> = pg_row.get(i);
                    PostgreSQLDataType::PgRangeNaiveDate(value)
                }
                "TSTZRANGE" => {
                    let value: PgRange<DateTime<chrono::Utc>> = pg_row.get(i);
                    PostgreSQLDataType::PgRangeDateTime(value)
                }
                "TSRANGE" => {
                    let value: PgRange<NaiveDateTime> = pg_row.get(i);
                    PostgreSQLDataType::PgRangeNaiveDateTime(value)
                }
                "INT4RANGE" => {
                    let value: PgRange<i32> = pg_row.get(i);
                    PostgreSQLDataType::PgRangeI32(value)
                }
                "INT8RANGE" => {
                    let value: PgRange<i64> = pg_row.get(i);
                    PostgreSQLDataType::PgRangeI64(value)
                }
                // "INT8RANGE" | "INT4RANGE" | "TSRANGE" | "TSTZRANGE" | "DATERANGE" | "NUMRANGE" => {
                //     let value: PgRange<i64> = pg_row.get(i);
                //     PostgreSQLDataType::PgRange(value)
                // }
                "MONEY" => {
                    let value: PgMoney = pg_row.get(i);
                    PostgreSQLDataType::PgMoney(value)
                }
                "LTREE" => {
                    let value: PgLTree = pg_row.get(i);
                    PostgreSQLDataType::PgLTree(value)
                }
                "LQUERY" => {
                    let value: PgLQuery = pg_row.get(i);
                    PostgreSQLDataType::PgLQuery(value)
                }
                "NUMERIC" => {
                    let value: BigDecimal = pg_row.get(i);
                    PostgreSQLDataType::BigDecimal(value)
                }
                "TIMESTAMPTZ" => {
                    let value: DateTime<chrono::Utc> = pg_row.get(i);
                    PostgreSQLDataType::DateTime(value)
                }
                "TIMESTAMP" => {
                    let value: NaiveDateTime = pg_row.get(i);
                    PostgreSQLDataType::NaiveDateTime(value)
                }
                "DATE" => {
                    let value: NaiveDate = pg_row.get(i);
                    PostgreSQLDataType::NaiveDate(value)
                }
                "TIME" => {
                    let value: NaiveTime = pg_row.get(i);
                    PostgreSQLDataType::NaiveTime(value)
                }
                "TIMETZ" => {
                    let value: PgTimeTz = pg_row.get(i);
                    PostgreSQLDataType::PgTimeTz(value)
                }
                "UUID" => {
                    let value: Uuid = pg_row.get(i);
                    PostgreSQLDataType::Uuid(value)
                }
                "INET" | "CIDR" => {
                    let value: IpNetwork = pg_row.get(i);
                    PostgreSQLDataType::IpNetwork(value)
                }
                "MACADDR" => {
                    let value: MacAddress = pg_row.get(i);
                    PostgreSQLDataType::MacAddress(value)
                }
                "BIT" | "VARBIT" => {
                    let value: BitVec = pg_row.get(i);
                    PostgreSQLDataType::BitVec(value)
                }
                "JSON" | "JSONB" => {
                    let value: JsonValue = pg_row.get(i);
                    PostgreSQLDataType::JsonValue(value)
                }
                _ => {
                    PostgreSQLDataType::String(UNKNOWN_DATA_TYPE.into())
                }
            };
            let sql_value = SqlDataType::PostgreSQLDataType(postgresql_value);
            sql_row.insert(col_name, sql_value);
        }
        sql_rets.push_rets(sql_row);
    }
    Ok(sql_rets)
}



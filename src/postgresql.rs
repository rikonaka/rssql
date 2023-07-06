use anyhow;
use sqlx::postgres::types::{PgInterval, PgMoney, PgRange, PgTimeTz, PgLTree, PgLQuery};
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::types::ipnetwork::IpNetwork;
use sqlx::types::mac_address::MacAddress;
use sqlx::types::{BigDecimal, BitVec, JsonValue, Uuid};
use sqlx::{Column, Row, TypeInfo};
use sqlx::postgres::PgRow;
use std::collections::HashMap;
use std::fmt;

use crate::SQLDataTypes;
use crate::SQLRets;
use crate::UNKNOWN_DATA_TYPE;
use crate::BINARY;


static PGINTERVAL: &str = "[PGINTERVAL]";
static PGMONEY: &str = "[PGMONEY]";
static PGTIMETZ: &str = "[PGTIMETZ]";

#[derive(Debug, Clone)]
pub enum PostgreSQLDataTypes {
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

impl fmt::Display for PostgreSQLDataTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PostgreSQLDataTypes::Bool(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::I8(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::I16(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::I32(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::I64(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::U8(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::U16(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::U64(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::F32(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::F64(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::String(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::Binary(_) => write!(f, "{}", BINARY),
            PostgreSQLDataTypes::Void(_) => write!(f, "()"),
            PostgreSQLDataTypes::PgInterval(_) => write!(f, "{}", PGINTERVAL),
            PostgreSQLDataTypes::PgRangeBigDecimal(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::PgRangeDateTime(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::PgRangeNaiveDate(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::PgRangeNaiveDateTime(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::PgRangeI32(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::PgRangeI64(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::PgMoney(_) => write!(f, "{}", PGMONEY),
            PostgreSQLDataTypes::PgLTree(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::PgLQuery(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::BigDecimal(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::DateTime(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::NaiveDateTime(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::NaiveDate(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::NaiveTime(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::PgTimeTz(_) => write!(f, "{}", PGTIMETZ),
            PostgreSQLDataTypes::Uuid(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::IpNetwork(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::MacAddress(v) => write!(f, "{}", v),
            PostgreSQLDataTypes::BitVec(_) => write!(f, "{}", BINARY),
            PostgreSQLDataTypes::JsonValue(v) => write!(f, "{}", v),
        }
    }
}

pub async fn row_process(rows: Vec<PgRow>) -> anyhow::Result<SQLRets> {
    let mut sql_rets = SQLRets::new();

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
        let mut sql_row: HashMap<String, SQLDataTypes> = HashMap::new();
        let pg_row_len = pg_row.len();
        for i in 0..pg_row_len {
            let col = pg_row.column(i);
            let col_name = col.name().to_string();
            let type_info = col.type_info();
            let postgresql_value = match type_info.name() {
                "BOOL" => {
                    let value: bool = pg_row.get(i);
                    PostgreSQLDataTypes::Bool(value)
                }
                "CHAR" => {
                    let value: i8 = pg_row.get(i);
                    PostgreSQLDataTypes::I8(value)
                }
                "SMALLINT" | "SMALLSERIAL" | "INT2" => {
                    let value: i16 = pg_row.get(i);
                    PostgreSQLDataTypes::I16(value)
                }
                "INT" | "SERIAL" | "INT4" => {
                    let value: i32 = pg_row.get(i);
                    PostgreSQLDataTypes::I32(value)
                }
                "BIGINT" | "BIGSERIAL" | "INT8" => {
                    let value: i64 = pg_row.get(i);
                    PostgreSQLDataTypes::I64(value)
                }
                "REAL" | "FLOAT4" => {
                    let value: f32 = pg_row.get(i);
                    PostgreSQLDataTypes::F32(value)
                }
                "DOUBLE PRECISION" | "FLOAT8" => {
                    let value: f64 = pg_row.get(i);
                    PostgreSQLDataTypes::F64(value)
                }
                "VARCHAR" | "CHAR(N)" | "TEXT" | "NAME" => {
                    let value: String = pg_row.get(i);
                    PostgreSQLDataTypes::String(value)
                }
                "BYTEA" => {
                    let value: Vec<u8> = pg_row.get(i);
                    PostgreSQLDataTypes::Binary(value)
                }
                "VOID" => {
                    let value = ();
                    PostgreSQLDataTypes::Void(value)
                }
                "INTERVAL" => {
                    let value: PgInterval = pg_row.get(i);
                    PostgreSQLDataTypes::PgInterval(value)
                }
                "NUMRANGE" => {
                    let value: PgRange<BigDecimal> = pg_row.get(i);
                    PostgreSQLDataTypes::PgRangeBigDecimal(value)
                }
                "DATERANGE" => {
                    let value: PgRange<NaiveDate> = pg_row.get(i);
                    PostgreSQLDataTypes::PgRangeNaiveDate(value)
                }
                "TSTZRANGE" => {
                    let value: PgRange<DateTime<chrono::Utc>> = pg_row.get(i);
                    PostgreSQLDataTypes::PgRangeDateTime(value)
                }
                "TSRANGE" => {
                    let value: PgRange<NaiveDateTime> = pg_row.get(i);
                    PostgreSQLDataTypes::PgRangeNaiveDateTime(value)
                }
                "INT4RANGE" => {
                    let value: PgRange<i32> = pg_row.get(i);
                    PostgreSQLDataTypes::PgRangeI32(value)
                }
                "INT8RANGE" => {
                    let value: PgRange<i64> = pg_row.get(i);
                    PostgreSQLDataTypes::PgRangeI64(value)
                }
                // "INT8RANGE" | "INT4RANGE" | "TSRANGE" | "TSTZRANGE" | "DATERANGE" | "NUMRANGE" => {
                //     let value: PgRange<i64> = pg_row.get(i);
                //     PostgreSQLDataType::PgRange(value)
                // }
                "MONEY" => {
                    let value: PgMoney = pg_row.get(i);
                    PostgreSQLDataTypes::PgMoney(value)
                }
                "LTREE" => {
                    let value: PgLTree = pg_row.get(i);
                    PostgreSQLDataTypes::PgLTree(value)
                }
                "LQUERY" => {
                    let value: PgLQuery = pg_row.get(i);
                    PostgreSQLDataTypes::PgLQuery(value)
                }
                "NUMERIC" => {
                    let value: BigDecimal = pg_row.get(i);
                    PostgreSQLDataTypes::BigDecimal(value)
                }
                "TIMESTAMPTZ" => {
                    let value: DateTime<chrono::Utc> = pg_row.get(i);
                    PostgreSQLDataTypes::DateTime(value)
                }
                "TIMESTAMP" => {
                    let value: NaiveDateTime = pg_row.get(i);
                    PostgreSQLDataTypes::NaiveDateTime(value)
                }
                "DATE" => {
                    let value: NaiveDate = pg_row.get(i);
                    PostgreSQLDataTypes::NaiveDate(value)
                }
                "TIME" => {
                    let value: NaiveTime = pg_row.get(i);
                    PostgreSQLDataTypes::NaiveTime(value)
                }
                "TIMETZ" => {
                    let value: PgTimeTz = pg_row.get(i);
                    PostgreSQLDataTypes::PgTimeTz(value)
                }
                "UUID" => {
                    let value: Uuid = pg_row.get(i);
                    PostgreSQLDataTypes::Uuid(value)
                }
                "INET" | "CIDR" => {
                    let value: IpNetwork = pg_row.get(i);
                    PostgreSQLDataTypes::IpNetwork(value)
                }
                "MACADDR" => {
                    let value: MacAddress = pg_row.get(i);
                    PostgreSQLDataTypes::MacAddress(value)
                }
                "BIT" | "VARBIT" => {
                    let value: BitVec = pg_row.get(i);
                    PostgreSQLDataTypes::BitVec(value)
                }
                "JSON" | "JSONB" => {
                    let value: JsonValue = pg_row.get(i);
                    PostgreSQLDataTypes::JsonValue(value)
                }
                _ => {
                    PostgreSQLDataTypes::String(UNKNOWN_DATA_TYPE.into())
                }
            };
            let sql_value = SQLDataTypes::PostgreSQLDataTypes(postgresql_value);
            sql_row.insert(col_name, sql_value);
        }
        sql_rets.push_rets(sql_row);
    }
    Ok(sql_rets)
}



#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("lib.md")]
use sqlx::{Connection, MySqlConnection, PgConnection, SqliteConnection};
use std::collections::HashMap;
use std::fmt;

mod mysql;
mod postgresql;
mod sqlite;

use mysql::MySQLDataTypes;
use postgresql::PostgreSQLDataTypes;
use sqlite::SQLiteDataTypes;

pub static UNKNOWN: &str = "[unkonwn]";
pub static BINARY: &str = "[binary]";
pub static CONNECTION_CLOSED_ERROR: &str = "the connection is closed";

#[derive(Debug, Clone)]
pub enum SQLDataTypes {
    MySQLDataTypes(MySQLDataTypes),
    PostgreSQLDataTypes(PostgreSQLDataTypes),
    SQLiteDataTypes(SQLiteDataTypes),
}

impl fmt::Display for SQLDataTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SQLDataTypes::MySQLDataTypes(m) => write!(f, "{}", m),
            SQLDataTypes::PostgreSQLDataTypes(p) => write!(f, "{}", p),
            SQLDataTypes::SQLiteDataTypes(s) => write!(f, "{}", s),
        }
    }
}

impl SQLDataTypes {}

#[derive(Debug)]
pub struct SQLRets {
    /// Column name vec sort by default.
    pub column: Vec<String>,
    /// Returns.
    rets: Vec<HashMap<String, SQLDataTypes>>,
}

impl SQLRets {
    pub fn new() -> SQLRets {
        let rets = Vec::new();
        let column = Vec::new();
        SQLRets { column, rets }
    }
    pub fn push_rets(&mut self, row: HashMap<String, SQLDataTypes>) {
        self.rets.push(row);
    }
    pub fn push_column_name(&mut self, column_name: &str) {
        let column_name = column_name.to_string();
        if !self.column.contains(&column_name) {
            self.column.push(column_name)
        }
    }
    /// Get first data by column name.
    ///
    /// ```
    /// use rssql::PostgreSQL;
    /// async fn get_data() {
    ///     let mut postgresql = PostgreSQL::connect("postgre://user:password@127.0.0.1:5432/test")
    ///         .await
    ///         .unwrap();
    ///     let check = postgresql.check_connection().await;
    ///     assert_eq!(check, true);
    ///     let sql = "CREATE TABLE IF NOT EXISTS info (id INT PRIMARY KEY NOT NULL, name VARCHAR(16), date DATE)";
    ///     let rows_affecteds = postgresql.execute(sql).await.unwrap();
    ///     for i in 0..10 {
    ///         let sql = format!(
    ///             "INSERT INTO info (id, name, date) VALUES ({}, 'test{}', '2023-07-07')",
    ///             i, i
    ///         );
    ///         let rows_affecteds = postgresql.execute(&sql).await.unwrap();
    ///     }
    ///     let rets = postgresql.execute_fetch_all("SELECT * FROM info").await.unwrap();
    ///     println!("{}", rets);
    ///     for column in &rets.column {
    ///         println!("{}", rets.get_first_one(&column).unwrap());
    ///     }
    ///     for r in rets.get_all("name").unwrap() {
    ///         println!("{}", r);
    ///     }
    ///     postgresql.close().await;
    /// }
    /// ```
    pub fn get_first_one(&self, column_name: &str) -> Option<SQLDataTypes> {
        if self.column.contains(&column_name.to_string()) {
            if self.rets.len() > 0 {
                Some(self.rets[0].get(column_name).unwrap().clone())
            } else {
                None
            }
        } else {
            None
        }
    }
    /// Get all data by column name.
    ///
    /// ```
    /// use rssql::PostgreSQL;
    /// async fn get_data() {
    ///     let mut postgresql = PostgreSQL::connect("postgre://user:password@127.0.0.1:5432/test")
    ///         .await
    ///         .unwrap();
    ///     let rets = postgresql.execute_fetch_all("SELECT * FROM info").await.unwrap();
    ///     println!("{}", rets);
    ///     println!("{}", rets.rows_affected().unwrap());
    ///     postgresql.close().await;
    /// }
    /// ```
    pub fn get_all(&self, column_name: &str) -> Option<Vec<SQLDataTypes>> {
        if self.column.contains(&column_name.to_string()) {
            if self.rets.len() > 0 {
                let mut result = Vec::new();
                for ret in &self.rets {
                    result.push(ret.get(column_name).unwrap().clone())
                }
                Some(result)
            } else {
                None
            }
        } else {
            None
        }
    }
    /// Return rows affected.
    pub fn rows_affected(&self) -> anyhow::Result<u64> {
        match self.rets.len().try_into() {
            Ok(r) => Ok(r),
            Err(e) => Err(e.into()),
        }
    }
}

impl fmt::Display for SQLRets {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.rets.len() > 0 {
            let mut column_max_len = HashMap::new();
            for name in &self.column {
                column_max_len.insert(name.to_string(), name.len() + 2);
            }
            for ret in &self.rets {
                for name in &self.column {
                    let value = ret.get(name).unwrap();
                    let value_str = format!("{}", value);
                    if value_str.len() + 2 > *column_max_len.get(name).unwrap() {
                        column_max_len.insert(name.to_string(), value_str.len() + 2);
                    }
                }
            }

            let mut col_string = String::from("|");
            let mut hline_string = String::from("+");
            for name in &self.column {
                let need_pad_len = (column_max_len.get(name).unwrap() - name.len()) as i32;
                let mut col_name = name.to_string();
                for i in 0..need_pad_len {
                    if i % 2 == 0 {
                        col_name = format!("{} ", col_name);
                    } else {
                        col_name = format!(" {}", col_name);
                    }
                }
                col_string = format!("{}{}|", col_string, col_name);
                let mut hline = String::new();
                for _ in 0..col_name.len() {
                    hline = format!("{}-", hline);
                }
                hline_string = format!("{}{}+", hline_string, hline);
            }
            let mut print_str = format!("{}\n{}\n{}", hline_string, col_string, hline_string);
            for ret in &self.rets {
                let mut col_string = String::from("|");
                for name in &self.column {
                    let value = ret.get(name).unwrap();
                    let mut value_str = format!("{}", value);
                    let need_pad_len = (column_max_len.get(name).unwrap() - value_str.len()) as i32;
                    for i in 0..need_pad_len {
                        if i % 2 == 0 {
                            value_str = format!("{} ", value_str);
                        } else {
                            value_str = format!(" {}", value_str);
                        }
                    }
                    col_string = format!("{}{}|", col_string, value_str);
                }
                print_str = format!("{}\n{}", print_str, col_string);
            }
            write!(f, "{}\n{}", print_str, hline_string)
        } else {
            write!(f, "null")
        }
    }
}

pub struct SQLite {
    alive: bool,
    connection: SqliteConnection,
}

impl SQLite {
    /// Connect to sqlite database.
    ///
    /// # Example
    /// ```
    /// use rssql::SQLite;
    /// async fn test_sqlite() {
    ///     let mut sqlite = SQLite::connect("sqlite:sqlite_test.db?mode=rwc").await.unwrap();
    ///     let check = sqlite.check_connection().await;
    ///     assert_eq!(check, true);
    ///     let rows_affecteds = sqlite.execute("CREATE TABLE IF NOT EXISTS info (name TEXT, md5 TEXT, sha1 TEXT)").await.unwrap();
    ///     let rows_affecteds = sqlite.execute("INSERT INTO info (name, md5, sha1) VALUES ('test1', 'test1', 'test1')").await.unwrap();
    ///     let rets = sqlite.execute_fetch_all("SELECT * FROM info").await.unwrap();
    ///     println!("{}", rets);
    /// }
    /// ```
    /// # Output
    /// ```bash
    /// +-------+-------+-------+
    /// | name  |  md5  | sha1  |
    /// +-------+-------+-------+
    /// | test1 | test1 | test1 |
    /// | test1 | test1 | test1 |
    /// +-------+-------+-------+
    /// ```
    pub async fn connect(url: &str) -> anyhow::Result<SQLite> {
        let connection = SqliteConnection::connect(url).await?;
        let alive = true;
        Ok(SQLite { connection, alive })
    }
    /// Execute the sql but do not get data from database, returns the rows affected.
    pub async fn execute(&mut self, sql: &str) -> anyhow::Result<u64> {
        match self.alive {
            true => {
                let rows = sqlx::query(sql).execute(&mut self.connection).await?;
                Ok(rows.rows_affected())
            }
            false => panic!("{}", CONNECTION_CLOSED_ERROR),
        }
    }
    /// Execute and fetch all.
    pub async fn execute_fetch_all(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        match self.alive {
            true => {
                let rows = sqlx::query(sql).fetch_all(&mut self.connection).await?;
                sqlite::rows_process(rows).await
            }
            false => panic!("{}", CONNECTION_CLOSED_ERROR),
        }
    }
    /// Execute and fetch one.
    pub async fn execute_fetch_one(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        match self.alive {
            true => {
                let row = sqlx::query(sql).fetch_one(&mut self.connection).await?;
                let rows = vec![row];
                sqlite::rows_process(rows).await
            }
            false => panic!("{}", CONNECTION_CLOSED_ERROR),
        }
    }
    /// Close the sqlite connnection.
    pub async fn close(mut self) {
        self.alive = false;
        let _ = self.connection.close().await;
    }
    /// Check if the connection is valid.
    pub async fn check_connection(&mut self) -> bool {
        match self.alive {
            true => match self.connection.ping().await {
                Ok(_) => {
                    self.alive = true;
                    true
                }
                Err(_) => {
                    self.alive = false;
                    false
                }
            },
            false => panic!("{}", CONNECTION_CLOSED_ERROR),
        }
    }
}

pub struct MySQL {
    alive: bool,
    connection: MySqlConnection,
}

impl MySQL {
    /// Connect to mysql (mariadb) database.
    ///
    /// # Example
    /// ```
    /// use rssql::MySQL;
    /// async fn test_mysql() {
    ///     let mut mysql = MySQL::connect("mysql://user:password@127.0.0.1:3306/test").await.unwrap();
    ///     let check = mysql.check_connection().await;
    ///     assert_eq!(check, true);
    ///     let rets = mysql.execute_fetch_all("SELECT * FROM info").await.unwrap();
    ///     println!("{}", rets);
    ///     let rows_affecteds = mysql.execute("INSERT INTO info (name, datetime, date) VALUES ('test3', '2011-01-01', '2011-02-02')").await.unwrap();
    ///     let rets = mysql.execute_fetch_all("SELECT * FROM info").await.unwrap();
    ///     println!("{}", rets);
    ///     mysql.close().await;
    /// }
    /// ```
    /// # Output
    /// ```bash
    /// +----+-------+---------------------+------------+
    /// | id | name  |      datetime       |    date    |
    /// +----+-------+---------------------+------------+
    /// | 1  | test1 | 2023-03-20 00:00:00 | 2001-10-22 |
    /// | 2  | test2 | 2023-03-20 00:00:00 | 2001-10-22 |
    /// +----+-------+---------------------+------------+
    /// +----+-------+---------------------+------------+
    /// | id | name  |      datetime       |    date    |
    /// +----+-------+---------------------+------------+
    /// | 1  | test1 | 2023-03-20 00:00:00 | 2001-10-22 |
    /// | 2  | test2 | 2023-03-20 00:00:00 | 2001-10-22 |
    /// | 3  | test3 | 2011-01-01 00:00:00 | 2011-02-02 |
    /// +----+-------+---------------------+------------+
    /// ```
    pub async fn connect(url: &str) -> anyhow::Result<MySQL> {
        let connection = MySqlConnection::connect(url).await?;
        let alive = true;
        Ok(MySQL { connection, alive })
    }
    /// Execute the sql but do not get data from database, returns the rows affected.
    pub async fn execute(&mut self, sql: &str) -> anyhow::Result<u64> {
        match self.alive {
            true => {
                let rows = sqlx::query(sql).execute(&mut self.connection).await?;
                Ok(rows.rows_affected())
            }
            false => panic!("{}", CONNECTION_CLOSED_ERROR),
        }
    }
    /// Execute the sql and fetch all.
    pub async fn execute_fetch_all(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        match self.alive {
            true => {
                let rows = sqlx::query(sql).fetch_all(&mut self.connection).await?;
                mysql::rows_process(rows).await
            }
            false => panic!("{}", CONNECTION_CLOSED_ERROR),
        }
    }
    /// Execute and fetch one.
    pub async fn execute_fetch_one(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        match self.alive {
            true => {
                let row = sqlx::query(sql).fetch_one(&mut self.connection).await?;
                let rows = vec![row];
                mysql::rows_process(rows).await
            }
            false => panic!("{}", CONNECTION_CLOSED_ERROR),
        }
    }
    /// Close the mysql (mariadb) connnection.
    pub async fn close(mut self) {
        self.alive = false;
        let _ = self.connection.close().await;
    }
    /// Check if the connection is valid.
    pub async fn check_connection(&mut self) -> bool {
        match self.alive {
            true => match self.connection.ping().await {
                Ok(_) => {
                    self.alive = true;
                    true
                }
                Err(_) => {
                    self.alive = false;
                    false
                }
            },
            false => panic!("{}", CONNECTION_CLOSED_ERROR),
        }
    }
}

pub struct PostgreSQL {
    alive: bool,
    connection: PgConnection,
}

impl PostgreSQL {
    /// Connect to postgresql database.
    ///
    /// ```
    /// use rssql::PostgreSQL;
    /// async fn test_postgresql() {
    ///     let mut postgresql = PostgreSQL::connect("postgre://user:password@127.0.0.1:5432/test").await.unwrap();
    ///     let check = postgresql.check_connection().await;
    ///     assert_eq!(check, true);
    ///     let rets = postgresql.execute_fetch_all("SELECT * FROM info").await.unwrap();
    ///     println!("{}", rets);
    ///     postgresql.close().await;
    /// }
    /// ```
    /// # Output
    /// ```bash
    /// +----+-------+------------+
    /// | id | name  |    date    |
    /// +----+-------+------------+
    /// | 1  | test2 | 2023-06-11 |
    /// | 2  | test1 | 2023-06-11 |
    /// +----+-------+------------+
    /// ```
    pub async fn connect(url: &str) -> anyhow::Result<PostgreSQL> {
        let connection = PgConnection::connect(url).await?;
        let alive = true;
        Ok(PostgreSQL { connection, alive })
    }
    /// Execute the sql but do not get data from database, returns the rows affected.
    pub async fn execute(&mut self, sql: &str) -> anyhow::Result<u64> {
        match self.alive {
            true => {
                let rows = sqlx::query(sql).execute(&mut self.connection).await?;
                Ok(rows.rows_affected())
            }
            false => panic!("{}", CONNECTION_CLOSED_ERROR),
        }
    }
    /// Execute the sql and fetch all.
    pub async fn execute_fetch_all(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        match self.alive {
            true => {
                let rows = sqlx::query(sql).fetch_all(&mut self.connection).await?;
                postgresql::rows_process(rows).await
            }
            false => panic!("{}", CONNECTION_CLOSED_ERROR),
        }
    }
    /// Execute and fetch one.
    pub async fn execute_fetch_one(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        match self.alive {
            true => {
                let row = sqlx::query(sql).fetch_one(&mut self.connection).await?;
                let rows = vec![row];
                postgresql::rows_process(rows).await
            }
            false => panic!("{}", CONNECTION_CLOSED_ERROR),
        }
    }
    /// Close the postgresql connnection.
    pub async fn close(mut self) {
        self.alive = false;
        let _ = self.connection.close().await;
    }
    /// Check if the connection is valid.
    pub async fn check_connection(&mut self) -> bool {
        match self.alive {
            true => match self.connection.ping().await {
                Ok(_) => {
                    self.alive = true;
                    true
                }
                Err(_) => {
                    self.alive = false;
                    false
                }
            },
            false => panic!("{}", CONNECTION_CLOSED_ERROR),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_sqlite() {
        let mut sqlite: SQLite = SQLite::connect("sqlite:test.db?mode=rwc").await.unwrap();
        let check: bool = sqlite.check_connection().await;
        assert_eq!(check, true);
        let _ = sqlite
            .execute("CREATE TABLE IF NOT EXISTS info (name TEXT, md5 TEXT, sha1 TEXT)")
            .await
            .unwrap();
        for i in 0..10 {
            let sql = format!(
                "INSERT INTO info (name, md5, sha1) VALUES ('test{}', 'md5{}', 'sha1{}')",
                i, i, i
            );
            let rows_affecteds = sqlite.execute(&sql).await.unwrap();
            assert_eq!(rows_affecteds, 1);
        }
        let rets: SQLRets = sqlite
            .execute_fetch_all("SELECT * FROM info")
            .await
            .unwrap();
        println!("{}", rets);
        println!("{}", rets.rows_affected().unwrap());
    }
    #[tokio::test]
    async fn test_mysql() {
        // let mut mysql = MySQL::connect("mysql://user:password@docker:13306/test")
        let mut mysql: MySQL = MySQL::connect("mysql://user:password@127.0.0.1:3306/test")
            .await
            .unwrap();
        let check: bool = mysql.check_connection().await;
        assert_eq!(check, true);
        let sql = "CREATE TABLE IF NOT EXISTS info (id INT PRIMARY KEY NOT NULL AUTO_INCREMENT, name VARCHAR(16), date DATE)";
        let _ = mysql.execute(sql).await.unwrap();
        for i in 0..10 {
            let sql = format!(
                "INSERT INTO info (name, date) VALUES ('test{}', '2023-07-07')",
                i
            );
            let rows_affecteds = mysql.execute(&sql).await.unwrap();
            assert_eq!(rows_affecteds, 1);
        }
        let rets: SQLRets = mysql.execute_fetch_all("SELECT * FROM info").await.unwrap();
        println!("{}", rets);
        println!("{}", rets.rows_affected().unwrap());
        for column in &rets.column {
            let value: SQLDataTypes = rets.get_first_one(&column).unwrap();
            println!("{}", value);
        }
        let values: Vec<SQLDataTypes> = rets.get_all("name").unwrap();
        for value in values {
            println!("{}", value);
        }
        mysql.close().await;
    }
    #[tokio::test]
    async fn test_postgresql() {
        // let mut postgresql = PostgreSQL::connect("postgre://user:password@docker:15432/test")
        let mut postgresql: PostgreSQL =
            PostgreSQL::connect("postgre://user:password@127.0.0.1:5432/test")
                .await
                .unwrap();
        let check: bool = postgresql.check_connection().await;
        assert_eq!(check, true);
        let sql = "CREATE TABLE IF NOT EXISTS info (id SERIAL PRIMARY KEY NOT NULL, name VARCHAR(16), date DATE)";
        let _ = postgresql.execute(sql).await.unwrap();
        for i in 0..10 {
            let sql = format!(
                "INSERT INTO info (name, date) VALUES ('test{}', '2023-07-07')",
                i
            );
            let rows_affecteds = postgresql.execute(&sql).await.unwrap();
            assert_eq!(rows_affecteds, 1);
        }
        let rets: SQLRets = postgresql
            .execute_fetch_all("SELECT * FROM info")
            .await
            .unwrap();
        println!("{}", rets);
        println!("{}", rets.rows_affected().unwrap());
        for column in &rets.column {
            let value: SQLDataTypes = rets.get_first_one(&column).unwrap();
            println!("{}", value);
        }
        let _: Vec<SQLDataTypes> = rets.get_all("name").unwrap();
        // for value in values {
        //     match value {
        //         SQLDataTypes::MySQLDataTypes(m) => (),
        //         SQLDataTypes::PostgreSQLDataTypes(p) => (),
        //         SQLDataTypes::SQLiteDataTypes(s) => match s {
        //             SQLiteDataTypes::Binary(b) => (),
        //             SQLiteDataTypes::Bool(b) => (),
        //             SQLiteDataTypes::DateTime(d) => (),
        //             SQLiteDataTypes::F64(f) => {
        //                 let new_f = f + 3.14;
        //                 println!("new float value: {}", new_f);
        //             }
        //             SQLiteDataTypes::I32(i) => (),
        //             SQLiteDataTypes::I64(i) => (),
        //             SQLiteDataTypes::NaiveDate(n) => (),
        //             SQLiteDataTypes::NaiveDateTime(n) => (),
        //             SQLiteDataTypes::NaiveTime(n) => (),
        //             SQLiteDataTypes::String(s) => (),
        //         },
        //     }
        // }
        postgresql.close().await;
    }
    #[tokio::test]
    async fn test_all() {
        use sqlx::postgres::PgPoolOptions;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://user:password@127.0.0.1:5432/test")
            .await
            .unwrap();

        // Make a simple query to return the given parameter (use a question mark `?` instead of `$1` for MySQL)
        let row: (i32,) = sqlx::query_as("SELECT id FROM info WHERE id=1")
            .fetch_one(&pool)
            .await
            .unwrap();

        // println!(">>>> {}", row.0);
        assert_eq!(row.0, 1);
    }
}

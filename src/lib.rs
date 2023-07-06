#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("lib.md")]
use std::collections::HashMap;
use std::fmt;

use anyhow;
use sqlx::{Connection, MySqlConnection, PgConnection, SqliteConnection};

mod mysql;
mod postgresql;
mod sqlite;

use mysql::MySQLDataTypes;
use postgresql::PostgreSQLDataTypes;
use sqlite::SQLiteDataTypes;

pub static UNKNOWN_DATA_TYPE: &str = "[UNKNOWN]";
pub static BINARY: &str = "[BINARY]";
pub static CLOSED_CONNECTION_ERROR: &str = "the connection is closed";

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

#[derive(Debug)]
pub struct SQLRets {
    /// Column name vec sort by default.
    pub column: Vec<String>,
    /// Returns.
    pub rets: Vec<HashMap<String, SQLDataTypes>>,
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
    /// use rssql::MySQL;
    /// async fn test_mysql_one() {
    ///     let url = "mysql://user:password@127.0.0.1:13306/test";
    ///     let mut mysql = MySQL::connect(url).await.unwrap();
    ///     let check = mysql.check_connection().await;
    ///     println!("{}", check);
    ///     let rets = mysql.execute("SELECT * FROM info").await.unwrap();
    ///     for c in &rets.column {
    ///         println!("{}", rets.get_first_one(&c).unwrap());
    ///     }
    ///     for r in rets.get_all("id").unwrap() {
    ///         println!("{}", r);
    ///     }
    ///     mysql.close().await;
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
    ///     let url = "sqlite:sqlite_test.db?mode=rwc";
    ///     let mut sqlite = SQLite::connect(url).await.unwrap();
    ///     let sql = "SELECT * FROM info";
    ///     let rets = sqlite.execute(sql).await.unwrap();
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
    /// Execute the sql and fetch all.
    pub async fn execute(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        Self::execute_fetch_all(self, sql).await
    }
    /// Same as execute.
    pub async fn execute_fetch_all(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        match self.alive {
            true => {
                let rows = sqlx::query(sql).fetch_all(&mut self.connection).await?;
                sqlite::raw_process(rows).await
            }
            false => panic!("{}", CLOSED_CONNECTION_ERROR),
        }
    }
    /// Execute and fetch one.
    pub async fn execute_fetch_one(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        match self.alive {
            true => {
                let row = sqlx::query(sql).fetch_one(&mut self.connection).await?;
                let rows = vec![row];
                sqlite::raw_process(rows).await
            }
            false => panic!("{}", CLOSED_CONNECTION_ERROR),
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
            false => panic!("{}", CLOSED_CONNECTION_ERROR),
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
    ///     let url = "mysql://user:password@127.0.0.1:13306/test";
    ///     let mut mysql = MySQL::connect(url).await.unwrap();
    ///     let check = mysql.check_connection().await;
    ///     println!("{}", check);
    ///     let rets = mysql.execute("SELECT * FROM info").await.unwrap();
    ///     println!("{}", rets);
    ///     let rets = mysql.execute("INSERT INTO info (name, datetime, date) VALUES ('test3', '2011-01-01', '2011-02-02')").await.unwrap();
    ///     let rets = mysql.execute("SELECT * FROM info").await.unwrap();
    ///     println!("{}", rets);
    ///     mysql.close().await;
    /// }
    /// ```
    /// # Output
    /// ```bash
    /// true
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
    /// Execute the sql and fetch all.
    pub async fn execute(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        Self::execute_fetch_all(self, sql).await
    }
    /// Same as execute.
    pub async fn execute_fetch_all(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        match self.alive {
            true => {
                let rows = sqlx::query(sql).fetch_all(&mut self.connection).await?;
                mysql::raw_process(rows).await
            }
            false => panic!("{}", CLOSED_CONNECTION_ERROR),
        }
    }
    /// Execute and fetch one.
    pub async fn execute_fetch_one(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        match self.alive {
            true => {
                let row = sqlx::query(sql).fetch_one(&mut self.connection).await?;
                let rows = vec![row];
                mysql::raw_process(rows).await
            }
            false => panic!("{}", CLOSED_CONNECTION_ERROR),
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
            false => panic!("{}", CLOSED_CONNECTION_ERROR),
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
    ///     let url = "postgre://user:password@127.0.0.1:15432/test";
    ///     let mut postgresql = PostgreSQL::connect(url).await.unwrap();
    ///     let check = postgresql.check_connection().await;
    ///     println!("{}", check);
    ///     let rets = postgresql.execute("SELECT * FROM info").await.unwrap();
    ///     println!("{}", rets);
    ///     postgresql.close().await;
    /// }
    /// ```
    /// # Output
    /// ```bash
    /// true
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
    /// Execute the sql and fetch all.
    pub async fn execute(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        Self::execute_fetch_all(self, sql).await
    }
    /// Same as execute.
    pub async fn execute_fetch_all(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        match self.alive {
            true => {
                let rows = sqlx::query(sql).fetch_all(&mut self.connection).await?;
                postgresql::raw_process(rows).await
            }
            false => panic!("{}", CLOSED_CONNECTION_ERROR),
        }
    }
    /// Execute and fetch one.
    pub async fn execute_fetch_one(&mut self, sql: &str) -> anyhow::Result<SQLRets> {
        match self.alive {
            true => {
                let row = sqlx::query(sql).fetch_one(&mut self.connection).await?;
                let rows = vec![row];
                postgresql::raw_process(rows).await
            }
            false => panic!("{}", CLOSED_CONNECTION_ERROR),
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
            false => panic!("{}", CLOSED_CONNECTION_ERROR),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_sqlite() {
        let url = "sqlite:test.db?mode=rwc";
        let mut sqlite = SQLite::connect(url).await.unwrap();
        let sql = "CREATE TABLE IF NOT EXISTS info (name TEXT, md5 TEXT, sha1 TEXT)";
        let _ = sqlite.execute(sql).await.unwrap();
        let sql = "INSERT INTO info (name, md5, sha1) VALUES ('test1', 'test1', 'test1')";
        let _ = sqlite.execute(sql).await.unwrap();
        let sql = "SELECT * FROM info";
        let rets = sqlite.execute(sql).await.unwrap();
        println!("{}", rets);
    }
    #[tokio::test]
    async fn test_mysql() {
        let url = "mysql://root:password@127.0.0.1:3306/test";
        let mut mysql = MySQL::connect(url).await.unwrap();
        let check = mysql.check_connection().await;
        println!("{}", check);
        let sql = "CREATE TABLE IF NOT EXISTS info (id INT PRIMARY KEY NOT NULL AUTO_INCREMENT, name VARCHAR(30) NOT NULL, date DATE NOT NULL)";
        let _ = mysql.execute(sql).await.unwrap();
        let sql = "INSERT INTO info (id, name, date) VALUES (1, 'test3', '2011-02-02')";
        let _ = mysql.execute(sql).await.unwrap();
        let rets = mysql.execute("SELECT * FROM info").await.unwrap();
        println!("{}", rets);
        for column in &rets.column {
            println!("{}", rets.get_first_one(&column).unwrap());
        }
        for r in rets.get_all("name").unwrap() {
            println!("{}", r);
        }
        mysql.close().await;
    }
    #[tokio::test]
    async fn test_postgresql() {
        let url = "postgre://user:password@127.0.0.1:5432/test";
        let mut postgresql = PostgreSQL::connect(url).await.unwrap();
        let check = postgresql.check_connection().await;
        println!("{}", check);
        let sql = "CREATE TABLE IF NOT EXISTS info (id INT PRIMARY KEY NOT NULL, name VARCHAR(30) NOT NULL, date DATE NOT NULL)";
        let _ = postgresql.execute(sql).await.unwrap();
        let sql = "INSERT INTO info (id, name, date) VALUES (1, 'test3', '2011-02-02')";
        let _ = postgresql.execute(sql).await.unwrap();
        let rets = postgresql.execute("SELECT * FROM info").await.unwrap();
        println!("{}", rets);
        for r in rets.get_all("name").unwrap() {
            println!("{}", r);
        }
        postgresql.close().await;
    }
}

# rssql

Struct-free Rust SQL tool.

## Intro

[`sqlx`](https://github.com/launchbadge/sqlx) based sql tools, support `mysql (mariadb)`, `postgresql` and `sqlite`.

## Example

### Start with the easy ones

```rust
use rssql::PostgreSQL;

async fn test_postgresql() {
    /// Connect to database.
    let mut postgresql = PostgreSQL::connect("postgre://user:password@127.0.0.1:5432/test")
        .await
        .unwrap();
    /// Check connection.
    let check = postgresql.check_connection().await;
    assert_eq!(check, true);
    let sql = "CREATE TABLE IF NOT EXISTS info (id INT PRIMARY KEY NOT NULL, name VARCHAR(16), date DATE)";
    let _ = postgresql.execute(sql).await.unwrap();
    /// Insert 10 rows data into table `info`.
    for i in 0..10 {
        let sql = format!(
            "INSERT INTO info (id, name, date) VALUES ({}, 'test{}', '2023-07-07')",
            i, i
        );
        let _ = postgresql.execute(&sql).await.unwrap();
    }
    /// Select all from table `info`.
    let rets = postgresql.execute("SELECT * FROM info").await.unwrap();
    // let rets = postgresql.execute_fetch_all("SELECT * FROM info").await.unwrap();
    // let rets = postgresql.execute_fetch_one("SELECT * FROM info").await.unwrap();
    println!("{}", rets);
    /// Get first one from returns by column name.
    for column in &rets.column {
        println!("{}", rets.get_first_one(&column).unwrap());
    }
    /// Get all by column name.
    for r in rets.get_all("name").unwrap() {
        println!("{}", r);
    }
    /// Close the connection.
    postgresql.close().await;
}
```

### Show the result

```rust
use rssql::PostgreSQL;

async fn postgresql_select() {
    let mut postgresql = PostgreSQL::connect("postgre://user:password@127.0.0.1:5432/test")
        .await
        .unwrap();
    let check = postgresql.check_connection().await;
    assert_eq!(check, true);
    let rets = postgresql.execute("SELECT * FROM info").await.unwrap();
    println!("{}", rets);
    postgresql.close().await;
}
```

**Output**

```bash
+----+-------+------------+
| id | name  |    date    |
+----+-------+------------+
| 1  | test1 | 2023-06-11 |
| 2  | test2 | 2023-06-11 |
+----+-------+------------+
```
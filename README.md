# rssql

Struct-free Rust SQL tool.

## Intro

[`sqlx`](https://github.com/launchbadge/sqlx) based sql tools, support `mysql (mariadb)`, `postgresql` and `sqlite`.

## Example

### Start with the easy ones

```rust
use rssql::PostgreSQL;

async fn test_postgresql() {
    // Connect to database
    let url = "postgre://user:password@docker:15432/test";
    let mut postgresql = PostgreSQL::connect(url).await.unwrap();
    // Check connection
    let check = postgresql.check_connection().await;
    println!("{}", check);
    // Select all from table `info`
    let rets = postgresql.execute("SELECT * FROM info").await.unwrap();
    // let rets = postgresql.execute_fetch_all("SELECT * FROM info").await.unwrap();
    // let rets = postgresql.execute_fetch_one("SELECT * FROM info").await.unwrap();
    println!("{}", rets);
    // Insert one row data into table `info`
    let rets = postgresql.execute("INSERT INTO info (name, date) VALUES ('test3', '2022-01-01')").await.unwrap();
    let rets = postgresql.execute("SELECT * FROM info").await.unwrap();
    println!("{}", rets);
    // Close the connection
    postgresql.close().await;
}
```

**Output**

```bash
true
+----+-------+------------+
| id | name  |    date    |
+----+-------+------------+
| 1  | test1 | 2023-06-11 |
| 2  | test2 | 2023-06-11 |
+----+-------+------------+
+----+-------+------------+
| id | name  |    date    |
+----+-------+------------+
| 1  | test1 | 2023-06-11 |
| 2  | test2 | 2023-06-11 |
| 3  | test3 | 2022-01-01 |
+----+-------+------------+
```

### Get data by column name

```rust
use rssql::PostgreSQL;

async fn test_postgresql_one() {
    // Connect to database
    let url = "postgre://user:password@docker:15432/test";
    let mut postgresql = PostgreSQL::connect(url).await.unwrap();
    // Check connection
    let check = postgresql.check_connection().await;
    println!("{}", check);
    let rets = postgresql.execute("SELECT * FROM info").await.unwrap();
    // Get first data by column name 
    for c in &rets.column {
        println!("{}", rets.get_first_one(&c).unwrap());
    }
    // Get all data by column name 
    for r in rets.get_all("id").unwrap() {
        println!("{}", r);
    }
    // Close the connection
    postgresql.close().await;
}
```
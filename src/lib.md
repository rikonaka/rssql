The async struct free SQL toolkit for Rust.

## Simple example

```
use rssql::PostgreSQL;

async fn test_postgresql() {
    let url = "postgre://user:password@docker:15432/test";
    let mut postgresql = PostgreSQL::connect(url).await.unwrap();
    let check = postgresql.check_connection().await;
    println!("{}", check);
    let rets = postgresql.execute("SELECT * FROM info").await.unwrap();
    println!("{}", rets);
    postgresql.close().await;
}
```

**Output**
```
true
+----+-------+------------+
| id | name  |    date    |
+----+-------+------------+
| 1  | test2 | 2023-06-11 |
| 2  | test1 | 2023-06-11 |
+----+-------+------------+
```
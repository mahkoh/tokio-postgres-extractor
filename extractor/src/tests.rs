use {
    crate::{Extract, RowExtractExt},
    tokio_postgres::{Client, NoTls, Row},
    tokio_postgres_extractor_macros::Columns,
};

pub async fn connect() -> Client {
    let (client, conn) = tokio_postgres::connect(
        "postgres://postgres:postgres@localhost:5433/postgres",
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    client
}

pub async fn row(sql: &str) -> Row {
    connect().await.query_one(sql, &[]).await.unwrap()
}

#[tokio::test]
async fn test() {
    #[derive(Columns, Extract)]
    struct X {
        x: i32,
    }

    let row: X = row("select 1 x").await.extract_once();
    assert_eq!(row.x, 1);
}

#[tokio::test]
async fn borrow() {
    #[derive(Columns, Extract)]
    struct X<'a> {
        x: String,
        #[column(name = "x")]
        y: &'a str,
    }

    let row = row("select 'hello' x").await;
    let row = row.extract_once::<X>();
    assert_eq!(row.x, "hello");
    assert_eq!(row.y, "hello");
}

#[tokio::test]
async fn generic() {
    #[derive(Columns, Extract)]
    struct X<'a, T: ?Sized> {
        x: String,
        #[column(name = "x")]
        y: &'a T,
    }

    let row = row("select 'hello' x").await;
    let row = row.extract_once::<X<str>>();
    assert_eq!(row.x, "hello");
    assert_eq!(row.y, "hello");
}

#[tokio::test]
async fn extract() {
    #[derive(Columns, Extract)]
    struct X {
        x: i32,
        y: i32,
    }

    let mut columns = None;

    let row = row("select 2 y, 1 x").await;

    let x: X = row.extract(&mut columns);
    assert_eq!(x.x, 1);
    assert_eq!(x.y, 2);

    assert_eq!(columns, Some([1, 0]));

    let x: X = row.extract(&mut columns);
    assert_eq!(x.x, 1);
    assert_eq!(x.y, 2);
}

#[tokio::test]
async fn mixed_use() {
    #[derive(Columns, Extract)]
    struct X {
        x: i32,
        y: i32,
    }

    let mut columns = None;

    let x: X = row("select 1 x, 2 y").await.extract(&mut columns);
    assert_eq!(x.x, 1);
    assert_eq!(x.y, 2);

    assert_eq!(columns, Some([0, 1]));

    let x: X = row("select 2 y, 1 x").await.extract(&mut columns);
    assert_eq!(x.x, 2);
    assert_eq!(x.y, 1);

    columns = None;

    let x: X = row("select 2 y, 1 x").await.extract(&mut columns);
    assert_eq!(x.x, 1);
    assert_eq!(x.y, 2);

    assert_eq!(columns, Some([1, 0]));
}

#[tokio::test]
async fn extract_known() {
    #[derive(Columns, Extract)]
    struct X {
        x: i32,
        y: i32,
    }

    let x: X = row("select 1 x, 2 y").await.extract_with_columns(&[1, 0]);
    assert_eq!(x.x, 2);
    assert_eq!(x.y, 1);
}

#[tokio::test]
async fn pre_defined() {
    #[derive(Columns, Extract)]
    struct X {
        x: i32,
        #[column(idx = 2)]
        y: i32,
    }

    let x: X = row("select 1 x, 2, 3").await.extract_once();
    assert_eq!(x.x, 1);
    assert_eq!(x.y, 3);
}

#[tokio::test]
async fn non_trivial() {
    #[derive(Columns, Extract)]
    struct X {
        axx: i32,
        axy: i32,
        ayx: i32,
        ayy: i32,
    }

    let x: X = row("select 1 axx, 2 axy, 3 ayx, 4 ayy")
        .await
        .extract_once();
    assert_eq!(x.axx, 1);
    assert_eq!(x.axy, 2);
    assert_eq!(x.ayx, 3);
    assert_eq!(x.ayy, 4);
}

#[tokio::test]
async fn tuple_struct() {
    #[derive(Columns, Extract)]
    struct X(i32, i32, #[column(name = "x")] i32, i32);

    let x: X = row("select 1, 2, 3, 4, 5 x").await.extract_once();
    assert_eq!(x.0, 1);
    assert_eq!(x.1, 2);
    assert_eq!(x.2, 5);
    assert_eq!(x.3, 4);
}

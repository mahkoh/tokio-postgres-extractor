use {
    crate::{stream::RowStreamExtractExt, tests::connect},
    futures_util::TryStreamExt,
    std::pin::pin,
    tokio_postgres_extractor_macros::{Columns, Extract},
};

#[tokio::test]
async fn test() {
    #[derive(Columns, Extract)]
    struct X {
        x: i32,
        y: i32,
    }

    let res: Vec<X> = connect()
        .await
        .query_raw("select * from (values (1, 2), (3, 4)) t(x, y)", None::<i32>)
        .await
        .unwrap()
        .extract()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(res.len(), 2);
    assert_eq!(res[0].x, 1);
    assert_eq!(res[0].y, 2);
    assert_eq!(res[1].x, 3);
    assert_eq!(res[1].y, 4);
}

#[tokio::test]
async fn test_mut() {
    #[derive(Columns, Extract)]
    struct X {
        x: i32,
        y: i32,
    }

    let stream = connect()
        .await
        .query_raw("select * from (values (1, 2), (3, 4)) t(x, y)", None::<i32>)
        .await
        .unwrap();
    let mut stream = pin!(stream);

    let res: Vec<X> = stream.as_mut().extract_mut().try_collect().await.unwrap();

    assert_eq!(res.len(), 2);
    assert_eq!(res[0].x, 1);
    assert_eq!(res[0].y, 2);
    assert_eq!(res[1].x, 3);
    assert_eq!(res[1].y, 4);
    assert_eq!(stream.rows_affected(), Some(2));
}

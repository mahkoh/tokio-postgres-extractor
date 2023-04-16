use {
    crate::{
        iter::{IterExtractExt, IterExtractRefExt},
        tests::row,
    },
    tokio_postgres_extractor_macros::{Columns, Extract},
};

#[tokio::test]
async fn extract_ref() {
    #[derive(Columns, Extract)]
    struct X<'a> {
        s: &'a str,
    }

    let rows = [row("select 'a' s").await, row("select 'b' s").await];

    let res: Vec<_> = rows.iter().extract_ref::<X>().map(|s| s.s).collect();

    assert_eq!(res, ["a", "b"]);
}

#[tokio::test]
async fn extract() {
    #[derive(Columns, Extract)]
    struct X {
        s: String,
    }

    let rows = [row("select 'a' s").await, row("select 'b' s").await];

    let res: Vec<_> = rows.into_iter().extract::<X>().map(|s| s.s).collect();

    assert_eq!(res, ["a", "b"]);
}

#[tokio::test]
async fn extract_ref_mixed() {
    #[derive(Columns, Extract)]
    struct X<'a> {
        s: &'a str,
    }

    let rows = [
        row("select 'a' s, 'x' t").await,
        row("select 'x' t, 'b' s").await,
    ];

    let res: Vec<_> = rows.iter().extract_ref::<X>().map(|s| s.s).collect();

    assert_eq!(res, ["a", "x"]);
}

#[tokio::test]
async fn extract_mixed() {
    #[derive(Columns, Extract)]
    struct X {
        s: String,
    }

    let rows = [
        row("select 'a' s, 'x' t").await,
        row("select 'x' t, 'b' s").await,
    ];

    let res: Vec<_> = rows.into_iter().extract::<X>().map(|s| s.s).collect();

    assert_eq!(res, ["a", "x"]);
}
